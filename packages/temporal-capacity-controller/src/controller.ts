import { randomUUID } from "node:crypto";

import {
  TEMPORAL_CAPACITY_MANIFEST_VERSION,
  TEMPORAL_RETIREMENT_MAX_BUILDS_PER_CYCLE,
  TEMPORAL_RETIREMENT_VERIFY_TIMEOUT_SECONDS,
  TEMPORAL_SCALE_IN_COOLDOWN_SECONDS,
  TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS,
  TEMPORAL_SCALE_IN_INTENT_TIMEOUT_SECONDS,
  TEMPORAL_SCALE_IN_MAX_WAVE_FRACTION,
  TEMPORAL_SCALE_IN_SLOT_UTILIZATION,
  TEMPORAL_SCALE_IN_VERIFY_TIMEOUT_SECONDS,
  TEMPORAL_STABLE_POOL_IDS,
  TEMPORAL_STABLE_POOLS,
  isTemporalPoolOptional,
  type TemporalRetirementRecord,
  type TemporalStablePoolId,
} from "@capy/shared/temporal/capacity";

import { allocateGlobalCapacity } from "./allocation.js";
import { AwsCapacityReader } from "./aws-capacity.js";
import { ControllerChain } from "./chain.js";
import { calculatePoolDemand } from "./demand.js";
import { stableHash } from "./hash.js";
import { updateServiceTimes } from "./service-time.js";
import {
  CapacityStateStore,
  isConditionalCheckContention,
  isRetriableDynamoContention,
} from "./state.js";
import { TemporalCapacityReader } from "./temporal-capacity.js";
import type {
  CapacityEnvironment,
  CapacityLedger,
  CapacitySnapshot,
  ControllerConfig,
  ManagedTemporalService,
  ManagedTemporalTask,
  PoolDemand,
  PoolGrant,
  QueueCapacityObservation,
  ReconcileInput,
  ReconcileOutput,
  ReconcilerState,
  MaintenanceRedeploy,
  TemporalDrainRecord,
  WorkerProcessObservation,
} from "./types.js";

export type ControllerDependencies = {
  state: Pick<
    CapacityStateStore,
    | "initialize"
    | "readControlSnapshot"
    | "claimCycle"
    | "claimCapacityPlan"
    | "verifyWriteFence"
    | "completeCycle"
    | "expireReservations"
    | "admitReservation"
    | "readReservation"
    | "consumeReservation"
    | "releaseReservation"
    | "isConditionalFailure"
    | "listActiveDrains"
    | "putDrainIntent"
    | "putRetirementIntent"
    | "listActiveRetirements"
    | "claimRetirementRelease"
    | "completeRetirement"
    | "cancelDrain"
    | "refreshReadyDrainFence"
    | "claimProtectedDecrease"
    | "markDrainVerifying"
    | "completeDrain"
    | "rollbackProtectedDecrease"
    | "recoverApplyingDecrease"
    | "putMaintenanceRedeploy"
    | "readMaintenanceRedeploy"
    | "requestMaintenanceRerun"
    | "markMaintenanceLaunchAttempt"
    | "prepareMaintenanceRerun"
    | "listActiveMaintenanceRedeploys"
    | "updateMaintenanceRedeploy"
    | "claimMaintenanceDrain"
    | "writeReservationSnapshot"
    | "readReservationSnapshot"
  >;
  aws: Pick<
    AwsCapacityReader,
    | "read"
    | "readForReservation"
    | "updateDesiredCount"
    | "decreaseDesiredCount"
    | "zeroDesiredCount"
    | "updateTaskProtection"
    | "readTaskTerminalState"
    | "updateManagedService"
    | "emitCycleMetrics"
    | "emitCycleResultMetric"
    | "emitLoadGateMetrics"
    | "emitQueueBacklogMetrics"
  >;
  temporal: Pick<TemporalCapacityReader, "read">;
  chain: Pick<ControllerChain, "heartbeat">;
};

const observationsForService = (
  service: Parameters<typeof calculatePoolDemand>[0],
  observations: Awaited<
    ReturnType<TemporalCapacityReader["read"]>
  >["observations"],
) =>
  observations.filter(
    (observation) =>
      observation.environment === service.environment &&
      observation.deploymentName === service.deploymentName &&
      observation.buildId === service.buildId &&
      observation.poolId === service.poolId,
  );

const allocationsForGrants = (grants: PoolGrant[]) =>
  Object.fromEntries(
    grants.map((grant) => [
      grant.service.serviceArn,
      grant.granted * (grant.service.cpuUnits / 1024),
    ]),
  );

const updatesInPriorityOrder = (grants: PoolGrant[]) =>
  grants
    .filter((grant) => grant.granted > grant.service.desiredCount)
    .sort(
      (left, right) =>
        Number(right.service.environment === "prod") -
          Number(left.service.environment === "prod") ||
        left.service.serviceArn.localeCompare(right.service.serviceArn),
    );

const scaleInEligible = (
  demand: PoolDemand,
  observations: QueueCapacityObservation[],
) => {
  // DRAINED builds are owned by the batch retirement lane
  // (reconcileDrainedRetirements) and never enter the live protected
  // scale-in pipeline.
  if (demand.service.buildState === "DRAINED") return false;
  if (
    demand.staleInput ||
    demand.service.desiredCount <= demand.boundedWanted ||
    demand.service.pendingCount > 0 ||
    demand.service.runningCount !== demand.service.desiredCount ||
    demand.service.deploymentInProgress
  ) {
    return false;
  }
  if (observations.length === 0) return false;
  return observations.every((observation) => {
    const reportedSlots = observation.activeSlots + observation.availableSlots;
    // Some workers (e.g. jam-run with custom slot suppliers) report
    // currentAvailableSlots as 0 even when idle, which made any nonzero
    // activity read as 100% utilization. Floor the denominator at the
    // pool's configured capacity for the running task count.
    const queueConfig = (
      TEMPORAL_STABLE_POOLS[demand.service.poolId].queues as Partial<
        Record<string, { activitySlots: number; workflowSlots: number }>
      >
    )[observation.taskQueue];
    const configuredSlots =
      (observation.taskType === "activity"
        ? (queueConfig?.activitySlots ?? 0)
        : (queueConfig?.workflowSlots ?? 0)) * demand.service.runningCount;
    const totalSlots = Math.max(reportedSlots, configuredSlots);
    return (
      observation.fresh &&
      observation.workerTelemetryFresh &&
      observation.backlogCount === 0 &&
      observation.backlogAgeSeconds === 0 &&
      // Arrival rate is intentionally not gated here: boundedWanted already
      // prices arrivals via the demand formula, so desired > wanted plus low
      // utilization proves surplus even under steady trickle traffic. An
      // absolute add-rate gate kept busy-but-overprovisioned pools (the
      // common daytime case) from ever draining.
      (totalSlots === 0 ||
        observation.activeSlots / totalSlots <
          TEMPORAL_SCALE_IN_SLOT_UTILIZATION)
    );
  });
};

const workerForTask = (
  task: ManagedTemporalTask,
  workers: WorkerProcessObservation[],
  requireIdle = true,
) =>
  workers.find((worker) => {
    const expectedQueues = Object.keys(
      TEMPORAL_STABLE_POOLS[task.poolId].queues,
    );
    const expectedQueueTypes = expectedQueues.flatMap((queue) => [
      `${queue}#activity`,
      `${queue}#workflow`,
    ]);
    const observedQueueTypes = new Set(worker.taskQueues);
    return (
      worker.taskArn === task.taskArn &&
      worker.buildId === task.buildId &&
      worker.poolId === task.poolId &&
      worker.fresh &&
      (!requireIdle ||
        (worker.activeActivitySlots === 0 &&
          worker.activeWorkflowSlots === 0)) &&
      expectedQueueTypes.every((queueType) =>
        [...observedQueueTypes].some((observed) =>
          observed.endsWith(queueType),
        ),
      )
    );
  });

const siblingsProtected = (
  service: PoolDemand["service"],
  tasks: ManagedTemporalTask[],
  protectionFreshUntil = 0,
) => {
  const siblings = tasks.filter(
    (task) => task.serviceArn === service.serviceArn,
  );
  return (
    siblings.length === service.runningCount &&
    service.runningCount >= service.desiredCount &&
    siblings.every(
      (task) =>
        task.protectionEnabled &&
        (task.protectionExpirationDate ?? 0) >= protectionFreshUntil &&
        task.lastStatus === "RUNNING" &&
        task.desiredStatus === "RUNNING" &&
        task.healthStatus !== "UNHEALTHY",
    )
  );
};

export class CapacityController {
  constructor(
    private readonly config: ControllerConfig,
    private readonly dependencies: ControllerDependencies,
  ) {}

  private async launchMaintenanceRedeploy(
    maintenance: MaintenanceRedeploy,
    now: number,
  ) {
    await this.dependencies.state.markMaintenanceLaunchAttempt({
      maintenance,
      now,
    });
    const result = await this.updateManagedService({
      clusterArn: maintenance.clusterArn,
      serviceArn: maintenance.serviceArn,
      desiredCount: maintenance.desiredCount,
      taskDefinitionArn: maintenance.taskDefinitionArn,
      forceNewDeployment: true,
      reservationId: maintenance.reservationId,
      reservationOwnerToken: maintenance.reservationOwnerToken,
    });
    await this.dependencies.state.updateMaintenanceRedeploy({
      maintenance,
      state: "DEPLOYING",
      now: Date.now(),
    });
    return result;
  }

  async redeployManagedService(params: {
    clusterArn: string;
    serviceArn: string;
    desiredCount: number;
    taskDefinitionArn?: string;
    reservationId: string;
    reservationOwnerToken: string;
  }) {
    await this.dependencies.state.initialize();
    const control = await this.dependencies.state.readControlSnapshot();
    if (control.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA") {
      throw new Error(
        "Protected maintenance redeploy requires controller writer authority",
      );
    }
    const existingMaintenance =
      await this.dependencies.state.readMaintenanceRedeploy(params.serviceArn);
    if (
      existingMaintenance &&
      (existingMaintenance.state === "REQUESTED" ||
        existingMaintenance.state === "DEPLOYING" ||
        existingMaintenance.state === "DRAINING")
    ) {
      if (
        existingMaintenance.reservationId === params.reservationId &&
        existingMaintenance.reservationOwnerToken ===
          params.reservationOwnerToken
      ) {
        return {
          maintenanceId: existingMaintenance.maintenanceId,
          requestId: undefined,
          adopted: true,
        };
      }
      const rerunReservation = await this.dependencies.state.readReservation(
        params.reservationId,
      );
      if (
        !rerunReservation ||
        rerunReservation.ownerToken !== params.reservationOwnerToken ||
        rerunReservation.buildId !== existingMaintenance.buildId ||
        !rerunReservation.pools.includes(existingMaintenance.poolId) ||
        rerunReservation.state !== "ACTIVE" ||
        rerunReservation.expiresAt <= Date.now()
      ) {
        throw new Error(
          "Maintenance rerun reservation is absent, stale, or does not cover the active service",
        );
      }
      await this.dependencies.state.requestMaintenanceRerun({
        maintenance: existingMaintenance,
        now: Date.now(),
      });
      await this.releaseDeployment({
        reservationId: rerunReservation.reservationId,
        ownerToken: rerunReservation.ownerToken,
      });
      return {
        maintenanceId: existingMaintenance.maintenanceId,
        requestId: undefined,
        adopted: true,
      };
    }
    const awsRead = await this.dependencies.aws.read(
      control.ledger.activeReservationVcpu,
    );
    const service = awsRead.services.find(
      (candidate) => candidate.serviceArn === params.serviceArn,
    );
    if (!service || service.clusterArn !== params.clusterArn) {
      throw new Error(`Managed service ${params.serviceArn} was not found`);
    }
    if (service.desiredCount !== params.desiredCount) {
      throw new Error(
        `Maintenance desired count changed for ${service.serviceName}: expected ${params.desiredCount}, observed ${service.desiredCount}`,
      );
    }
    const oldTaskArns = awsRead.tasks
      .filter((task) => task.serviceArn === service.serviceArn)
      .map((task) => task.taskArn);
    if (
      oldTaskArns.length !== service.desiredCount ||
      !siblingsProtected(service, awsRead.tasks, Date.now() + 2 * 60_000)
    ) {
      throw new Error(
        `Maintenance redeploy requires ${service.desiredCount} healthy protected tasks for ${service.serviceName}`,
      );
    }
    const now = Date.now();
    const maintenance: MaintenanceRedeploy = {
      maintenanceId: randomUUID(),
      clusterArn: service.clusterArn,
      serviceArn: service.serviceArn,
      buildId: service.buildId,
      poolId: service.poolId,
      oldTaskArns,
      desiredCount: service.desiredCount,
      state: "REQUESTED",
      createdAt: now,
      deadline: now + 24 * 60 * 60_000,
      reservationId: params.reservationId,
      reservationOwnerToken: params.reservationOwnerToken,
      taskDefinitionArn: params.taskDefinitionArn,
    };
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const activeDrains = await this.dependencies.state.listActiveDrains();
      if (activeDrains.length > 0) {
        throw new Error(
          `Protected maintenance redeploy refused while drain ${activeDrains[0]?.intentId} is active`,
        );
      }
      try {
        await this.dependencies.state.putMaintenanceRedeploy({
          maintenance,
          now: Date.now(),
        });
        break;
      } catch (error) {
        if (
          !this.dependencies.state.isConditionalFailure(error) ||
          attempt === 5
        ) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, attempt * 500));
      }
    }
    const result = await this.launchMaintenanceRedeploy(maintenance, now);
    return {
      maintenanceId: maintenance.maintenanceId,
      requestId: result.requestId,
    };
  }

  private async reconcileMaintenance(params: {
    cycleId: string;
    authorityGeneration: number;
    ledgerGeneration: number;
    maintenance: MaintenanceRedeploy;
    services: PoolDemand[];
    workers: WorkerProcessObservation[];
    tasks: ManagedTemporalTask[];
    activeDrain?: TemporalDrainRecord;
    now: number;
  }) {
    const writes: Array<Record<string, unknown>> = [];
    const service = params.services.find(
      (demand) => demand.service.serviceArn === params.maintenance.serviceArn,
    )?.service;
    if (!service) {
      await this.dependencies.state.updateMaintenanceRedeploy({
        maintenance: params.maintenance,
        state: "FAILED",
        reason: "SERVICE_MISSING",
        now: params.now,
      });
      return { writes, partial: true };
    }
    if (params.maintenance.deadline < params.now) {
      if (
        params.maintenance.state === "DEPLOYING" ||
        params.maintenance.state === "DRAINING"
      ) {
        const deadlineExtensionCount =
          params.maintenance.deadlineExtensionCount ??
          (params.maintenance.terminalReason === "MAINTENANCE_DEADLINE_EXTENDED"
            ? 1
            : 0);
        if (deadlineExtensionCount < 1) {
          await this.dependencies.state.updateMaintenanceRedeploy({
            maintenance: params.maintenance,
            state: params.maintenance.state,
            reason: "MAINTENANCE_DEADLINE_EXTENDED",
            deadline: params.now + 24 * 60 * 60_000,
            deadlineExtensionCount: 1,
            now: params.now,
          });
          return { writes, partial: true };
        }
        await this.releaseDeployment({
          reservationId: params.maintenance.reservationId,
          ownerToken: params.maintenance.reservationOwnerToken,
        });
      }
      await this.dependencies.state.updateMaintenanceRedeploy({
        maintenance: params.maintenance,
        state: "FAILED",
        reason: "MAINTENANCE_DEADLINE_EXPIRED",
        now: params.now,
      });
      return { writes, partial: true };
    }
    if (params.maintenance.state === "REQUESTED") {
      const replacementVisible =
        service.deploymentInProgress ||
        params.tasks.some(
          (task) =>
            task.serviceArn === service.serviceArn &&
            !params.maintenance.oldTaskArns.includes(task.taskArn),
        );
      if (replacementVisible) {
        await this.dependencies.state.updateMaintenanceRedeploy({
          maintenance: params.maintenance,
          state: "DEPLOYING",
          now: params.now,
        });
      } else if (
        params.maintenance.launchAttemptedAt &&
        params.now - params.maintenance.launchAttemptedAt < 60_000
      ) {
        return { writes, partial: true };
      } else {
        try {
          const result = await this.launchMaintenanceRedeploy(
            params.maintenance,
            params.now,
          );
          writes.push({
            maintenanceId: params.maintenance.maintenanceId,
            requestId: result.requestId,
            result: "MAINTENANCE_LAUNCH_RETRIED",
          });
          return { writes, applied: true };
        } catch (error) {
          console.error(
            "Protected maintenance launch failed; REQUESTED state will retry from fresh ECS state",
            {
              maintenanceId: params.maintenance.maintenanceId,
              error,
            },
          );
          return { writes, partial: true };
        }
      }
    }
    const oldTasks = params.tasks.filter((task) =>
      params.maintenance.oldTaskArns.includes(task.taskArn),
    );
    if (oldTasks.length === 0) {
      if (service.deploymentInProgress || service.pendingCount > 0) {
        return { writes };
      }
      if (
        params.maintenance.taskDefinitionArn &&
        service.taskDefinitionArn !== params.maintenance.taskDefinitionArn
      ) {
        await this.releaseDeployment({
          reservationId: params.maintenance.reservationId,
          ownerToken: params.maintenance.reservationOwnerToken,
        });
        await this.dependencies.state.updateMaintenanceRedeploy({
          maintenance: params.maintenance,
          state: "FAILED",
          reason: "MAINTENANCE_DEPLOYMENT_ROLLED_BACK",
          now: params.now,
        });
        return { writes, partial: true };
      }
      if (params.maintenance.rerunRequested) {
        const currentTasks = params.tasks.filter(
          (task) => task.serviceArn === service.serviceArn,
        );
        if (
          currentTasks.length !== service.desiredCount ||
          !siblingsProtected(service, params.tasks, params.now + 2 * 60_000)
        ) {
          return { writes };
        }
        await this.releaseDeployment({
          reservationId: params.maintenance.reservationId,
          ownerToken: params.maintenance.reservationOwnerToken,
        });
        const reservationId = `maintenance-rerun-${params.maintenance.maintenanceId}-${params.now}`;
        const reservationOwnerToken = randomUUID();
        await this.reserveDeployment({
          reservationId,
          ownerToken: reservationOwnerToken,
          environment: service.environment,
          buildId: service.buildId,
          pools: [service.poolId],
          requestedVcpu: service.desiredCount * (service.cpuUnits / 1024),
          expiresAt: params.now + 55 * 60_000,
        });
        try {
          await this.dependencies.state.prepareMaintenanceRerun({
            maintenance: params.maintenance,
            oldTaskArns: currentTasks.map((task) => task.taskArn),
            reservationId,
            reservationOwnerToken,
            deadline: params.now + 24 * 60 * 60_000,
            now: params.now,
          });
        } catch (error) {
          await this.releaseDeployment({
            reservationId,
            ownerToken: reservationOwnerToken,
          });
          throw error;
        }
        writes.push({
          maintenanceId: params.maintenance.maintenanceId,
          result: "MAINTENANCE_RERUN_RESERVED",
        });
        return { writes, applied: true };
      }
      await this.releaseDeployment({
        reservationId: params.maintenance.reservationId,
        ownerToken: params.maintenance.reservationOwnerToken,
      });
      await this.dependencies.state.updateMaintenanceRedeploy({
        maintenance: params.maintenance,
        state: "COMPLETE",
        now: params.now,
      });
      writes.push({
        maintenanceId: params.maintenance.maintenanceId,
        result: "MAINTENANCE_COMPLETE",
      });
      return { writes, applied: true };
    }
    const newTasks = params.tasks.filter(
      (task) =>
        task.serviceArn === service.serviceArn &&
        !params.maintenance.oldTaskArns.includes(task.taskArn),
    );
    const serviceTasks = params.tasks.filter(
      (task) => task.serviceArn === service.serviceArn,
    );
    if (
      newTasks.length === 0 ||
      serviceTasks.length <= service.desiredCount ||
      !siblingsProtected(service, params.tasks, params.now + 2 * 60_000)
    ) {
      return { writes };
    }
    if (params.activeDrain) return { writes };
    const selected = oldTasks
      .filter((task) => Boolean(workerForTask(task, params.workers, false)))
      .sort((left, right) => left.taskArn.localeCompare(right.taskArn))[0];
    if (!selected) return { writes };

    const intentId = randomUUID();
    const drain: TemporalDrainRecord = {
      kind: "MAINTENANCE",
      taskArn: selected.taskArn,
      clusterArn: service.clusterArn,
      serviceArn: service.serviceArn,
      poolId: service.poolId,
      buildId: service.buildId,
      intentId,
      cycleId: params.cycleId,
      authorityGeneration: params.authorityGeneration,
      ledgerGeneration: params.ledgerGeneration,
      priorDesiredCount: service.desiredCount,
      targetDesiredCount: service.desiredCount,
      state: "INTENT",
      createdAt: params.now,
      deadline:
        params.now +
        Math.max(
          TEMPORAL_SCALE_IN_INTENT_TIMEOUT_SECONDS,
          TEMPORAL_STABLE_POOLS[service.poolId].shutdownGraceSeconds + 5 * 60,
        ) *
          1_000,
    };
    await this.dependencies.state.putDrainIntent({
      drain,
      now: params.now,
    });
    await this.dependencies.state.updateMaintenanceRedeploy({
      maintenance: params.maintenance,
      state: "DRAINING",
      now: params.now,
    });
    writes.push({
      maintenanceId: params.maintenance.maintenanceId,
      intentId,
      taskArn: selected.taskArn,
      result: "MAINTENANCE_DRAIN_CREATED",
    });
    return { writes };
  }

  // Two disjoint lanes partitioned by buildState. The batch retirement lane
  // (DRAINED builds) runs FIRST — before maintenance routing and the live
  // protected-scale-in lane — and the live lane never touches DRAINED
  // services again. The retirement lane's ledger releases bump the ledger
  // generation, so the live lane receives the lane's updated ledger view
  // rather than the stale pre-lane snapshot.
  private async reconcileProtectedScaleIn(params: {
    cycleId: string;
    authorityGeneration: number;
    ledger: CapacityLedger;
    demands: PoolDemand[];
    observations: QueueCapacityObservation[];
    workers: WorkerProcessObservation[];
    tasks: ManagedTemporalTask[];
    staleReadCount: number;
    incompleteWorkerDeployments: string[];
    scaleIn: NonNullable<ReconcilerState["scaleIn"]>;
    maintenance?: MaintenanceRedeploy;
    now: number;
  }): Promise<{
    writes: Array<Record<string, unknown>>;
    partial?: boolean;
    applied?: boolean;
  }> {
    const activeDrains = await this.dependencies.state.listActiveDrains();
    const retirement = await this.reconcileDrainedRetirements({
      cycleId: params.cycleId,
      authorityGeneration: params.authorityGeneration,
      ledger: params.ledger,
      demands: params.demands,
      tasks: params.tasks,
      staleReadCount: params.staleReadCount,
      activeDrains,
      now: params.now,
    });
    const live = await this.reconcileLiveScaleIn({
      ...params,
      ledger: retirement.ledger,
      activeDrains,
    });
    return {
      writes: [...retirement.writes, ...live.writes],
      ...(retirement.partial || live.partial ? { partial: true } : {}),
      ...(retirement.applied || live.applied ? { applied: true } : {}),
    };
  }

  // The batch retirement lane (R1): every service of a DRAINED build is
  // zeroed wholesale — one RETIREMENT record per service, one desiredCount=0
  // write, one ledger-allocation release transaction per service, all
  // services in the same cycle. DRAINED is Temporal's own assertion that no
  // pollers or pinned workflows remain, so no telemetry, protection, or
  // per-task drain handshake is required: task protection is stripped
  // best-effort (ECS will not stop protected tasks) but a lapsed protection
  // never blocks the lane — the failure mode where one protection-lapsed
  // DRAINED candidate aborted scale-in for the whole environment is dead.
  private async reconcileDrainedRetirements(params: {
    cycleId: string;
    authorityGeneration: number;
    ledger: CapacityLedger;
    demands: PoolDemand[];
    tasks: ManagedTemporalTask[];
    staleReadCount: number;
    activeDrains: TemporalDrainRecord[];
    now: number;
  }): Promise<{
    writes: Array<Record<string, unknown>>;
    partial?: boolean;
    applied?: boolean;
    ledger: CapacityLedger;
  }> {
    const writes: Array<Record<string, unknown>> = [];
    let partial = false;
    let applied = false;
    let ledger = params.ledger;
    const retirements = await this.dependencies.state.listActiveRetirements();
    const serviceByArn = new Map(
      params.demands.map((demand) => [
        demand.service.serviceArn,
        demand.service,
      ]),
    );
    const legacyDrainArns = new Set(
      params.activeDrains.map((drain) => drain.serviceArn),
    );

    const releaseAllocation = async (
      record: TemporalRetirementRecord,
      priorDesiredCount: number,
      taskVcpu: number,
    ) => {
      const released = await this.dependencies.state.claimRetirementRelease({
        record,
        cycleId: params.cycleId,
        authorityGeneration: params.authorityGeneration,
        ledger,
        releasedVcpu: priorDesiredCount * taskVcpu,
        now: params.now,
      });
      ledger = released.ledger;
    };

    // The actuation shared by fresh intents and ZEROING records resumed from
    // a prior cycle that crashed or deferred between intent and actuation.
    const actuate = async (
      record: TemporalRetirementRecord,
      service: ManagedTemporalService,
    ) => {
      // Strip protection best-effort: ECS will not stop protected tasks, but
      // requiring live protection is exactly the F2 failure mode this lane
      // kills; a strip failure only delays running->0 until expiry.
      for (const task of params.tasks.filter(
        (candidate) =>
          candidate.serviceArn === service.serviceArn &&
          candidate.protectionEnabled,
      )) {
        try {
          await this.dependencies.aws.updateTaskProtection({
            clusterArn: service.clusterArn,
            taskArn: task.taskArn,
            protectionEnabled: false,
          });
        } catch (error) {
          console.error("Retirement task-protection strip failed; continuing", {
            serviceArn: service.serviceArn,
            taskArn: task.taskArn,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
      try {
        const requestId = await this.dependencies.aws.zeroDesiredCount(service);
        writes.push({
          intentId: record.intentId,
          serviceArn: service.serviceArn,
          priorDesired: service.desiredCount,
          desired: 0,
          requestId,
          result: "RETIREMENT_ZERO_APPLIED",
        });
      } catch (error) {
        // Same posture as the scale-out actuation loop: one service's ECS
        // write failure (drift or transient) journals as FAILED and defers to
        // the next cycle instead of aborting the batch.
        partial = true;
        writes.push({
          intentId: record.intentId,
          serviceArn: service.serviceArn,
          result: "RETIREMENT_ZERO_FAILED",
          error: error instanceof Error ? error.message : String(error),
        });
        return;
      }
      await releaseAllocation(
        record,
        record.priorDesiredCount,
        service.cpuUnits / 1024,
      );
      applied = true;
    };

    // Advance ZEROING records first: confirm, abort on a live rollback, fail
    // on deadline, or resume. Each record advances in its own contention
    // isolation so one lost race defers that record, not the batch.
    const resumable: TemporalRetirementRecord[] = [];
    const advanceRecord = async (record: TemporalRetirementRecord) => {
      const service = serviceByArn.get(record.serviceArn);
      if (!service) {
        if (record.releasedLedgerGeneration === undefined) {
          // The service is gone but the ledger release never committed: the
          // allocation would sit as a phantom floor until the next wholesale
          // plan write. The service's task shape is no longer observable, so
          // price the release from the pool config.
          await releaseAllocation(
            record,
            record.priorDesiredCount,
            TEMPORAL_STABLE_POOLS[record.poolId].cpu / 1024,
          );
        }
        // The iac retire verb deleted the zeroed service — terminal success.
        await this.dependencies.state.completeRetirement({
          record,
          terminalState: "APPLIED",
          reason: "SERVICE_DELETED",
          now: params.now,
        });
        writes.push({
          intentId: record.intentId,
          serviceArn: record.serviceArn,
          result: "RETIREMENT_SERVICE_DELETED",
        });
        applied = true;
        return;
      }
      if (
        service.buildState === "CURRENT" ||
        service.buildState === "RAMPING" ||
        service.buildState === "DRAINING"
      ) {
        // Break-glass rollback (SetCurrent back to this build) while a
        // retirement was in flight: the build is live again, so abort
        // instead of yo-yoing zero writes against the rollback. No release
        // here — a live service's allocation is owned by the scale-out plan
        // write, which rewrites the map wholesale. DRAINED->REGISTRATION is
        // NOT an abort: a deregistered version can never route work, so the
        // zero should proceed.
        await this.dependencies.state.completeRetirement({
          record,
          terminalState: "FAILED",
          reason: "BUILD_STATE_CHANGED",
          now: params.now,
        });
        writes.push({
          intentId: record.intentId,
          serviceArn: record.serviceArn,
          buildState: service.buildState,
          result: "RETIREMENT_BUILD_STATE_CHANGED",
        });
        partial = true;
        return;
      }
      if (
        service.desiredCount === 0 &&
        service.runningCount === 0 &&
        service.pendingCount === 0
      ) {
        if (record.releasedLedgerGeneration === undefined) {
          // The ECS zero landed but the ledger release never committed
          // (crash or lost race between the two): the allocation is still a
          // phantom floor — release it before closing the record.
          await releaseAllocation(
            record,
            record.priorDesiredCount,
            service.cpuUnits / 1024,
          );
        }
        await this.dependencies.state.completeRetirement({
          record,
          terminalState: "APPLIED",
          now: params.now,
        });
        writes.push({
          intentId: record.intentId,
          serviceArn: record.serviceArn,
          result: "RETIREMENT_CONFIRMED",
        });
        applied = true;
        return;
      }
      if (record.verifyDeadline < params.now) {
        if (
          service.desiredCount === 0 &&
          record.releasedLedgerGeneration === undefined
        ) {
          // The zero landed but the release is still owed (deferred
          // contention or a crash) and protection re-renewal kept tasks
          // running past the deadline. FAILED is terminal and desired=0
          // means the candidate filter never re-selects this service, so
          // failing without the release would leak the allocation as a
          // phantom floor until service deletion.
          await releaseAllocation(
            record,
            record.priorDesiredCount,
            service.cpuUnits / 1024,
          );
        }
        await this.dependencies.state.completeRetirement({
          record,
          terminalState: "FAILED",
          reason:
            service.desiredCount > 0
              ? "RETIREMENT_ZERO_NEVER_APPLIED"
              : "RETIREMENT_RUNNING_NOT_STOPPED",
          now: params.now,
        });
        writes.push({
          intentId: record.intentId,
          serviceArn: record.serviceArn,
          result: "RETIREMENT_VERIFY_TIMEOUT",
        });
        partial = true;
        return;
      }
      if (service.desiredCount > 0) resumable.push(record);
    };
    for (const record of retirements) {
      try {
        await advanceRecord(record);
      } catch (error) {
        if (!isRetriableDynamoContention(error)) throw error;
        partial = true;
        writes.push({
          intentId: record.intentId,
          serviceArn: record.serviceArn,
          result: "RETIREMENT_DEFERRED_CONTENTION",
        });
      }
    }

    // Select fresh candidates: every DRAINED service still carrying desired
    // capacity, minus in-flight retirements, legacy per-task drain records
    // (transition safety), and deploys in progress. New intents require this
    // cycle's Temporal read to be complete — a stale read cannot assert
    // DRAINED.
    const inFlight = new Set(retirements.map((record) => record.serviceArn));
    const candidates =
      params.staleReadCount > 0
        ? []
        : params.demands
            .map((demand) => demand.service)
            .filter(
              (service) =>
                service.buildState === "DRAINED" &&
                service.desiredCount > 0 &&
                !service.deploymentInProgress &&
                !inFlight.has(service.serviceArn) &&
                !legacyDrainArns.has(service.serviceArn),
            )
            .sort((left, right) =>
              left.serviceArn.localeCompare(right.serviceArn),
            );

    // Sanity floor (DESIGN.md §3, mirroring the retire verb's
    // RETIRE_MAX_BUILDS_PER_RUN): an absurd batch is evidence of a wrong
    // worldview — refuse loudly rather than zero it.
    const candidateBuilds = new Set(
      candidates.map((service) => service.buildId),
    );
    let newCandidates = candidates;
    if (candidateBuilds.size > TEMPORAL_RETIREMENT_MAX_BUILDS_PER_CYCLE) {
      console.error(
        "Refusing drained-retirement batch over the sanity floor; verify the world before raising it",
        {
          builds: candidateBuilds.size,
          max: TEMPORAL_RETIREMENT_MAX_BUILDS_PER_CYCLE,
        },
      );
      writes.push({
        result: "RETIREMENT_FLOOR_REFUSED",
        builds: candidateBuilds.size,
        max: TEMPORAL_RETIREMENT_MAX_BUILDS_PER_CYCLE,
      });
      partial = true;
      newCandidates = [];
    }

    for (const record of resumable) {
      const service = serviceByArn.get(record.serviceArn);
      if (!service) continue;
      try {
        await actuate(record, service);
      } catch (error) {
        if (!isRetriableDynamoContention(error)) throw error;
        partial = true;
        writes.push({
          intentId: record.intentId,
          serviceArn: record.serviceArn,
          result: "RETIREMENT_DEFERRED_CONTENTION",
        });
      }
    }
    for (const service of newCandidates) {
      const record: TemporalRetirementRecord = {
        kind: "RETIREMENT",
        serviceArn: service.serviceArn,
        clusterArn: service.clusterArn,
        poolId: service.poolId,
        buildId: service.buildId,
        intentId: randomUUID(),
        cycleId: params.cycleId,
        authorityGeneration: params.authorityGeneration,
        ledgerGeneration: ledger.generation,
        priorDesiredCount: service.desiredCount,
        state: "ZEROING",
        createdAt: params.now,
        verifyDeadline:
          params.now + TEMPORAL_RETIREMENT_VERIFY_TIMEOUT_SECONDS * 1_000,
      };
      try {
        // Intent observable before execution, then actuate in the same cycle.
        await this.dependencies.state.putRetirementIntent({
          record,
          now: params.now,
        });
        writes.push({
          intentId: record.intentId,
          serviceArn: service.serviceArn,
          result: "RETIREMENT_INTENT_CREATED",
        });
        await actuate(record, service);
      } catch (error) {
        // One service's transient DynamoDB contention defers that service to
        // the next cycle; it must not abort the rest of the batch.
        if (!isRetriableDynamoContention(error)) throw error;
        partial = true;
        writes.push({
          intentId: record.intentId,
          serviceArn: record.serviceArn,
          result: "RETIREMENT_DEFERRED_CONTENTION",
        });
      }
    }
    return {
      writes,
      ...(partial ? { partial: true } : {}),
      ...(applied ? { applied: true } : {}),
      ledger,
    };
  }

  private async reconcileLiveScaleIn(params: {
    cycleId: string;
    authorityGeneration: number;
    ledger: CapacityLedger;
    demands: PoolDemand[];
    observations: QueueCapacityObservation[];
    workers: WorkerProcessObservation[];
    tasks: ManagedTemporalTask[];
    staleReadCount: number;
    incompleteWorkerDeployments: string[];
    scaleIn: NonNullable<ReconcilerState["scaleIn"]>;
    maintenance?: MaintenanceRedeploy;
    activeDrains: TemporalDrainRecord[];
    now: number;
  }): Promise<{
    writes: Array<Record<string, unknown>>;
    partial?: boolean;
    applied?: boolean;
  }> {
    const writes: Array<Record<string, unknown>> = [];
    const activeDrains = params.activeDrains;
    const activeDrain = activeDrains.toSorted(
      (left, right) => left.createdAt - right.createdAt,
    )[0];
    const activeScaleInServiceArns = new Set(
      activeDrains
        .filter((drain) => drain.kind === "SCALE_IN")
        .map((drain) => drain.serviceArn),
    );
    if (activeDrain) {
      const service = params.demands.find(
        (demand) => demand.service.serviceArn === activeDrain.serviceArn,
      )?.service;
      const demand = params.demands.find(
        (candidate) => candidate.service.serviceArn === activeDrain.serviceArn,
      );
      const task = params.tasks.find(
        (candidate) => candidate.taskArn === activeDrain.taskArn,
      );
      const state = params.scaleIn[activeDrain.serviceArn] ?? {};
      if (!service) {
        if (
          activeDrain.state === "APPLYING" ||
          activeDrain.state === "VERIFYING"
        ) {
          if (activeDrain.kind === "SCALE_IN") {
            const taskVcpu =
              TEMPORAL_STABLE_POOLS[activeDrain.poolId].cpu / 1024;
            await this.dependencies.state.recoverApplyingDecrease({
              drain: activeDrain,
              serviceArn: activeDrain.serviceArn,
              priorAllocationVcpu: activeDrain.priorDesiredCount * taskVcpu,
              now: params.now,
            });
          } else {
            await this.dependencies.state.completeDrain({
              drain: activeDrain,
              terminalState: "FAILED",
              reason: "SERVICE_MISSING",
              now: params.now,
            });
          }
        } else {
          await this.dependencies.state.cancelDrain({
            drain: activeDrain,
            reason: "SERVICE_MISSING",
            now: params.now,
          });
        }
        return { writes, partial: true };
      }
      if (
        activeDrain.state === "VERIFYING" ||
        activeDrain.state === "APPLYING"
      ) {
        if (!task) {
          const terminal = await this.dependencies.aws.readTaskTerminalState({
            clusterArn: activeDrain.clusterArn,
            taskArn: activeDrain.taskArn,
          });
          if (!terminal.terminal) return { writes };
          if (
            activeDrain.kind === "SCALE_IN" &&
            service.desiredCount !== activeDrain.targetDesiredCount
          ) {
            await this.dependencies.state.recoverApplyingDecrease({
              drain: activeDrain,
              serviceArn: service.serviceArn,
              priorAllocationVcpu:
                service.desiredCount * (service.cpuUnits / 1024),
              now: params.now,
            });
            params.scaleIn[activeDrain.serviceArn] = {
              ...state,
              activeIntentId: undefined,
              disabledReason: "TASK_STOPPED_BEFORE_DECREMENT",
            };
            return { writes, partial: true };
          }
          await this.dependencies.state.completeDrain({
            drain: activeDrain,
            terminalState: "APPLIED",
            now: params.now,
          });
          if (activeDrain.kind === "SCALE_IN") {
            params.scaleIn[activeDrain.serviceArn] = {
              ...state,
              lastScaleInAt: params.now,
              activeIntentId: undefined,
            };
          }
          writes.push({
            intentId: activeDrain.intentId,
            taskArn: activeDrain.taskArn,
            result: "TERMINATION_CONFIRMED",
          });
          return { writes, applied: true };
        }
        if (
          activeDrain.kind === "MAINTENANCE" &&
          activeDrain.state === "APPLYING"
        ) {
          await this.dependencies.aws.updateTaskProtection({
            clusterArn: service.clusterArn,
            taskArn: activeDrain.taskArn,
            protectionEnabled: false,
          });
          await this.dependencies.state.markDrainVerifying({
            drain: activeDrain,
            now: params.now,
          });
          writes.push({
            intentId: activeDrain.intentId,
            taskArn: activeDrain.taskArn,
            result: "MAINTENANCE_PROTECTION_REMOVED",
          });
          return { writes, applied: true };
        }
        if (
          activeDrain.kind === "SCALE_IN" &&
          service.desiredCount !== activeDrain.targetDesiredCount
        ) {
          await this.dependencies.state.recoverApplyingDecrease({
            drain: activeDrain,
            serviceArn: service.serviceArn,
            priorAllocationVcpu:
              service.desiredCount * (service.cpuUnits / 1024),
            now: params.now,
          });
          await this.dependencies.aws.updateTaskProtection({
            clusterArn: service.clusterArn,
            taskArn: activeDrain.taskArn,
            protectionEnabled: true,
            expiresInMinutes: 60,
          });
          params.scaleIn[activeDrain.serviceArn] = {
            ...state,
            activeIntentId: undefined,
            disabledReason: "DECREASE_APPLY_RECOVERED",
          };
          writes.push({
            intentId: activeDrain.intentId,
            taskArn: activeDrain.taskArn,
            result: "APPLYING_ROLLED_BACK",
          });
          return { writes, partial: true };
        }
        if (
          activeDrain.kind === "SCALE_IN" &&
          activeDrain.state === "APPLYING" &&
          service.desiredCount === activeDrain.targetDesiredCount
        ) {
          await this.dependencies.state.markDrainVerifying({
            drain: activeDrain,
            now: params.now,
          });
          writes.push({
            intentId: activeDrain.intentId,
            taskArn: activeDrain.taskArn,
            result: "DECREMENT_CONFIRMED",
          });
          return { writes, applied: true };
        }
        if (
          activeDrain.verifyingAt &&
          params.now - activeDrain.verifyingAt >
            TEMPORAL_SCALE_IN_VERIFY_TIMEOUT_SECONDS * 1_000
        ) {
          await this.dependencies.state.completeDrain({
            drain: activeDrain,
            terminalState: "FAILED",
            reason: "SELECTED_TASK_NOT_TERMINATED",
            now: params.now,
          });
          writes.push({
            intentId: activeDrain.intentId,
            taskArn: activeDrain.taskArn,
            result: "TERMINATION_TIMEOUT",
          });
          if (activeDrain.kind === "SCALE_IN") {
            params.scaleIn[activeDrain.serviceArn] = {
              ...state,
              activeIntentId: undefined,
              disabledReason: "SELECTED_TASK_NOT_TERMINATED",
            };
          }
          return { writes, partial: true };
        }
        return { writes };
      }

      const demandRebounded =
        activeDrain.kind === "SCALE_IN" &&
        (!demand ||
          !scaleInEligible(
            demand,
            observationsForService(service, params.observations),
          ));
      if (
        activeDrain.deadline < params.now ||
        demandRebounded ||
        service.desiredCount !== activeDrain.priorDesiredCount
      ) {
        const cancelReason =
          activeDrain.deadline < params.now
            ? "DRAIN_DEADLINE_EXPIRED"
            : "DEMAND_OR_SERVICE_STATE_CHANGED";
        await this.dependencies.state.cancelDrain({
          drain: activeDrain,
          reason: cancelReason,
          now: params.now,
        });
        if (activeDrain.kind === "SCALE_IN") {
          params.scaleIn[activeDrain.serviceArn] = {
            ...state,
            eligibleSince: undefined,
            activeIntentId: undefined,
          };
        }
        // The reason must ride the write record: the cycle counts
        // DRAIN_DEADLINE_EXPIRED cancellations into the DrainDeadlineExpired
        // metric, the pageable signal for drains that never complete.
        writes.push({
          intentId: activeDrain.intentId,
          taskArn: activeDrain.taskArn,
          result: "CANCELLED",
          reason: cancelReason,
        });
        return { writes };
      }
      if (activeDrain.state !== "READY") return { writes };
      const freshControl = await this.dependencies.state.readControlSnapshot();
      if (
        freshControl.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA" ||
        freshControl.authority.generation !== params.authorityGeneration
      ) {
        return { writes, partial: true };
      }
      const freshAws = await this.dependencies.aws.read(
        freshControl.ledger.activeReservationVcpu,
      );
      const freshService = freshAws.services.find(
        (candidate) => candidate.serviceArn === activeDrain.serviceArn,
      );
      const freshTask = freshAws.tasks.find(
        (candidate) => candidate.taskArn === activeDrain.taskArn,
      );
      if (!freshTask) {
        await this.dependencies.state.completeDrain({
          drain: activeDrain,
          terminalState: "FAILED",
          reason: "READY_TASK_MISSING",
          now: params.now,
        });
        params.scaleIn[activeDrain.serviceArn] = {
          ...state,
          activeIntentId: undefined,
          disabledReason: "READY_TASK_MISSING",
        };
        return { writes, partial: true };
      }
      const serviceStateSafe =
        freshService !== undefined &&
        freshService.desiredCount === activeDrain.priorDesiredCount &&
        (activeDrain.kind === "MAINTENANCE"
          ? freshService.runningCount > freshService.desiredCount
          : freshService.pendingCount === 0 &&
            freshService.runningCount === freshService.desiredCount &&
            !freshService.deploymentInProgress);
      if (
        !serviceStateSafe ||
        !freshTask.protectionEnabled ||
        (freshTask.protectionExpirationDate ?? 0) < params.now + 2 * 60_000 ||
        !siblingsProtected(
          freshService,
          freshAws.tasks,
          params.now + 2 * 60_000,
        )
      ) {
        if (
          !freshTask.protectionEnabled ||
          (freshTask.protectionExpirationDate ?? 0) < params.now + 2 * 60_000
        ) {
          await this.dependencies.state.completeDrain({
            drain: activeDrain,
            terminalState: "FAILED",
            reason: "READY_TASK_PROTECTION_STALE",
            now: params.now,
          });
          params.scaleIn[activeDrain.serviceArn] = {
            ...state,
            activeIntentId: undefined,
            disabledReason: "READY_TASK_PROTECTION_STALE",
          };
        } else {
          await this.dependencies.state.cancelDrain({
            drain: activeDrain,
            reason: "SERVICE_CHANGED_DURING_FINAL_REVALIDATION",
            now: params.now,
          });
        }
        return { writes, partial: true };
      }
      const freshTemporal = await this.dependencies.temporal.read(
        freshAws.services,
      );
      if (
        (activeDrain.kind === "SCALE_IN" && freshTemporal.staleReadCount > 0) ||
        freshTemporal.incompleteWorkerDeployments.includes(
          freshService.deploymentName,
        )
      ) {
        writes.push({
          intentId: activeDrain.intentId,
          taskArn: activeDrain.taskArn,
          result: "WORKER_TELEMETRY_INCOMPLETE",
        });
        return { writes, partial: true };
      }
      const freshDemandService = freshTemporal.services.find(
        (candidate) => candidate.serviceArn === activeDrain.serviceArn,
      );
      if (!freshDemandService) {
        return { writes, partial: true };
      }
      if (
        freshDemandService.buildState !== "DRAINED" &&
        freshTemporal.workerProcesses.some(
          (worker) => worker.taskArn === activeDrain.taskArn,
        )
      ) {
        return { writes };
      }
      if (activeDrain.kind === "SCALE_IN") {
        if (
          !scaleInEligible(
            calculatePoolDemand(
              freshDemandService,
              observationsForService(
                freshDemandService,
                freshTemporal.observations,
              ),
            ),
            observationsForService(
              freshDemandService,
              freshTemporal.observations,
            ),
          )
        ) {
          await this.dependencies.state.cancelDrain({
            drain: activeDrain,
            reason: "DEMAND_REBOUNDED_DURING_FINAL_REVALIDATION",
            now: Date.now(),
          });
          return { writes, partial: true };
        }
      }
      let refreshed: TemporalDrainRecord;
      try {
        refreshed = await this.dependencies.state.refreshReadyDrainFence({
          drain: activeDrain,
          cycleId: params.cycleId,
          authorityGeneration: params.authorityGeneration,
          ledgerGeneration: freshControl.ledger.generation,
          now: params.now,
        });
      } catch (error) {
        // The fence asserts the ledger generation read at readControlSnapshot
        // above, but two slow network reads (aws.read, temporal.read) sit
        // between that read and this transaction. An out-of-cycle
        // reserve/release (admitReservation / releaseReservation) bumping the
        // ledger generation — or this cycle losing its lock — makes the fence
        // conditions fail as a benign race. Mirror the scale-out path's
        // generation-drift tolerance: defer this drain to the next cycle
        // instead of aborting the whole reconcile. Genuine DynamoDB failures
        // (validation, throttling, missing table, auth) are not CCF and still
        // throw.
        if (!isConditionalCheckContention(error)) throw error;
        writes.push({
          intentId: activeDrain.intentId,
          taskArn: activeDrain.taskArn,
          result: "FENCE_CONTENTION_RETRY",
        });
        return { writes, partial: true };
      }
      if (activeDrain.kind === "MAINTENANCE") {
        await this.dependencies.state.claimMaintenanceDrain({
          drain: refreshed,
          cycleId: params.cycleId,
          authorityGeneration: params.authorityGeneration,
          ledgerGeneration: freshControl.ledger.generation,
          now: params.now,
        });
        try {
          await this.dependencies.aws.updateTaskProtection({
            clusterArn: freshService.clusterArn,
            taskArn: activeDrain.taskArn,
            protectionEnabled: false,
          });
          await this.dependencies.state.markDrainVerifying({
            drain: activeDrain,
            now: Date.now(),
          });
          writes.push({
            intentId: activeDrain.intentId,
            taskArn: activeDrain.taskArn,
            result: "MAINTENANCE_TASK_RELEASED",
          });
          return { writes, applied: true };
        } catch (error) {
          await Promise.allSettled([
            this.dependencies.aws.updateTaskProtection({
              clusterArn: freshService.clusterArn,
              taskArn: activeDrain.taskArn,
              protectionEnabled: true,
              expiresInMinutes: 60,
            }),
            this.dependencies.state.completeDrain({
              drain: activeDrain,
              terminalState: "FAILED",
              reason: error instanceof Error ? error.message : String(error),
              now: Date.now(),
            }),
          ]);
          throw error;
        }
      }
      const taskVcpu = freshService.cpuUnits / 1024;
      const targetAllocationVcpu = activeDrain.targetDesiredCount * taskVcpu;
      const ledgerGeneration =
        await this.dependencies.state.claimProtectedDecrease({
          drain: refreshed,
          cycleId: params.cycleId,
          authorityGeneration: params.authorityGeneration,
          ledger: freshControl.ledger,
          serviceArn: freshService.serviceArn,
          taskVcpu,
          targetAllocationVcpu,
          now: params.now,
        });
      let decrementAttempted = false;
      try {
        await this.dependencies.aws.updateTaskProtection({
          clusterArn: freshService.clusterArn,
          taskArn: activeDrain.taskArn,
          protectionEnabled: false,
        });
        decrementAttempted = true;
        const requestId = await this.dependencies.aws.decreaseDesiredCount(
          freshService,
          activeDrain.targetDesiredCount,
        );
        await this.dependencies.state.markDrainVerifying({
          drain: activeDrain,
          now: Date.now(),
        });
        params.scaleIn[freshService.serviceArn] = {
          ...state,
          lastScaleInAt: params.now,
          activeIntentId: activeDrain.intentId,
          waveStartedAt:
            state.waveStartedAt &&
            params.now - state.waveStartedAt <
              TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS * 1_000
              ? state.waveStartedAt
              : params.now,
          waveStartDesiredCount:
            state.waveStartedAt &&
            params.now - state.waveStartedAt <
              TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS * 1_000
              ? (state.waveStartDesiredCount ?? freshService.desiredCount)
              : freshService.desiredCount,
          waveDecrements:
            (state.waveStartedAt &&
            params.now - state.waveStartedAt <
              TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS * 1_000
              ? (state.waveDecrements ?? 0)
              : 0) + 1,
        };
        writes.push({
          intentId: activeDrain.intentId,
          taskArn: activeDrain.taskArn,
          priorDesired: freshService.desiredCount,
          desired: activeDrain.targetDesiredCount,
          requestId,
          result: "DECREMENT_APPLIED",
        });
        return { writes, applied: true };
      } catch (error) {
        if (!decrementAttempted) {
          const recovery = await Promise.allSettled([
            this.dependencies.aws.updateTaskProtection({
              clusterArn: freshService.clusterArn,
              taskArn: activeDrain.taskArn,
              protectionEnabled: true,
              expiresInMinutes: 60,
            }),
            this.dependencies.state.rollbackProtectedDecrease({
              drain: activeDrain,
              ledgerGeneration,
              serviceArn: freshService.serviceArn,
              taskVcpu,
              priorAllocationVcpu: freshService.desiredCount * taskVcpu,
              reason: error instanceof Error ? error.message : String(error),
              now: Date.now(),
            }),
          ]);
          if (recovery.some((result) => result.status === "rejected")) {
            throw new AggregateError(
              [
                error,
                ...recovery.flatMap((result) =>
                  result.status === "rejected" ? [result.reason] : [],
                ),
              ],
              "Protected scale-in failed and rollback was incomplete",
            );
          }
        } else {
          writes.push({
            intentId: activeDrain.intentId,
            taskArn: activeDrain.taskArn,
            result: "DECREMENT_OUTCOME_AMBIGUOUS",
          });
          return { writes, partial: true };
        }
        throw error;
      }
    }

    if (params.maintenance) {
      return this.reconcileMaintenance({
        cycleId: params.cycleId,
        authorityGeneration: params.authorityGeneration,
        ledgerGeneration: params.ledger.generation,
        maintenance: params.maintenance,
        services: params.demands,
        workers: params.workers,
        tasks: params.tasks,
        activeDrain,
        now: params.now,
      });
    }

    if (
      params.staleReadCount > 0 ||
      params.incompleteWorkerDeployments.length > 0
    ) {
      return { writes, partial: true };
    }

    const candidates = params.demands
      .filter((demand) => {
        const observations = observationsForService(
          demand.service,
          params.observations,
        );
        const state = params.scaleIn[demand.service.serviceArn] ?? {};
        if (state.disabledReason) return false;
        if (activeScaleInServiceArns.has(demand.service.serviceArn)) {
          return false;
        }
        const eligible = scaleInEligible(demand, observations);
        const waveActive =
          state.waveStartedAt !== undefined &&
          params.now - state.waveStartedAt <
            TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS * 1_000;
        const waveStartDesiredCount = waveActive
          ? (state.waveStartDesiredCount ?? demand.service.desiredCount)
          : demand.service.desiredCount;
        const waveDecrements = waveActive ? (state.waveDecrements ?? 0) : 0;
        const waveLimit = Math.max(
          1,
          Math.floor(
            waveStartDesiredCount * TEMPORAL_SCALE_IN_MAX_WAVE_FRACTION,
          ),
        );
        if (waveDecrements >= waveLimit) return false;
        params.scaleIn[demand.service.serviceArn] = {
          ...state,
          ...(waveActive
            ? {}
            : {
                waveStartedAt: undefined,
                waveStartDesiredCount: undefined,
                waveDecrements: undefined,
              }),
          eligibleSince: eligible
            ? (state.eligibleSince ?? params.now)
            : undefined,
        };
        return (
          eligible &&
          params.now -
            (params.scaleIn[demand.service.serviceArn]?.eligibleSince ??
              params.now) >=
            TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS * 1_000 &&
          params.now - (state.lastScaleInAt ?? 0) >=
            TEMPORAL_SCALE_IN_COOLDOWN_SECONDS * 1_000
        );
      })
      .sort((left, right) =>
        left.service.serviceArn.localeCompare(right.service.serviceArn),
      );
    const candidate = candidates[0];
    if (!candidate) return { writes };

    const serviceTasks = params.tasks.filter(
      (task) => task.serviceArn === candidate.service.serviceArn,
    );
    if (
      !siblingsProtected(
        candidate.service,
        params.tasks,
        params.now + 2 * 60_000,
      )
    ) {
      return { writes, partial: true };
    }
    const protectedServiceTasks = serviceTasks
      .filter(
        (task) =>
          task.protectionEnabled &&
          (task.protectionExpirationDate ?? 0) >= params.now + 2 * 60_000,
      )
      .sort((left, right) => left.taskArn.localeCompare(right.taskArn));
    const idleWorkerTask = protectedServiceTasks.find((task) =>
      Boolean(workerForTask(task, params.workers)),
    );
    const hasFreshWorkerForTask = (task: ManagedTemporalTask) =>
      params.workers.some(
        (worker) =>
          worker.taskArn === task.taskArn &&
          worker.buildId === task.buildId &&
          worker.poolId === task.poolId &&
          worker.fresh,
      );
    const selected = idleWorkerTask;
    if (!selected) return { writes };
    const selectedRequiresWorkerDrain = hasFreshWorkerForTask(selected);

    const intentId = randomUUID();
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn: selected.taskArn,
      clusterArn: candidate.service.clusterArn,
      serviceArn: candidate.service.serviceArn,
      poolId: candidate.service.poolId,
      buildId: candidate.service.buildId,
      intentId,
      cycleId: params.cycleId,
      authorityGeneration: params.authorityGeneration,
      ledgerGeneration: params.ledger.generation,
      priorDesiredCount: candidate.service.desiredCount,
      targetDesiredCount: candidate.service.desiredCount - 1,
      state: selectedRequiresWorkerDrain ? "INTENT" : "READY",
      createdAt: params.now,
      deadline:
        params.now +
        Math.max(
          TEMPORAL_SCALE_IN_INTENT_TIMEOUT_SECONDS,
          TEMPORAL_STABLE_POOLS[candidate.service.poolId].shutdownGraceSeconds +
            5 * 60,
        ) *
          1_000,
      ...(selectedRequiresWorkerDrain
        ? {}
        : {
            readyAt: params.now,
            protectionExpiresAt: selected.protectionExpirationDate,
            stoppedTaskQueues: [],
          }),
    };
    await this.dependencies.state.putDrainIntent({
      drain,
      now: params.now,
    });
    params.scaleIn[candidate.service.serviceArn] = {
      ...params.scaleIn[candidate.service.serviceArn],
      activeIntentId: intentId,
    };
    writes.push({
      intentId,
      serviceArn: candidate.service.serviceArn,
      taskArn: selected.taskArn,
      result: selectedRequiresWorkerDrain
        ? "DRAIN_INTENT_CREATED"
        : "DRAIN_READY_CREATED",
    });
    return { writes };
  }

  // Best-effort CycleResult emission for the quiet pre-cycle exits; a metric
  // failure must never turn a benign no-op exit back into a Lambda error.
  private async emitQuietExitMetric(result: "LOCK_CONTENDED" | "SUPERSEDED") {
    try {
      await this.dependencies.aws.emitCycleResultMetric({ result });
    } catch (error) {
      console.error("Quiet-exit cycle metric emission failed", error);
    }
  }

  async reconcile(input: ReconcileInput): Promise<ReconcileOutput> {
    const now = Date.now();
    const cycleId = randomUUID();
    const output = (overrides?: Partial<ReconcileOutput>): ReconcileOutput => ({
      operation: "reconcile",
      stateMachineArn: input.stateMachineArn,
      chainGeneration: input.chainGeneration,
      cycleIndex: input.cycleIndex + 1,
      rotate: input.cycleIndex + 1 >= this.config.chainRotationCycles,
      terminate: false,
      ...overrides,
    });
    await this.dependencies.state.initialize();
    try {
      await this.dependencies.chain.heartbeat({
        generation: input.chainGeneration,
        executionArn: input.executionArn,
        now,
      });
    } catch (error) {
      if (!isConditionalCheckContention(error)) throw error;
      // The heartbeat is fenced on generation + executionArn: losing it means
      // another execution owns the chain and this one is a superseded
      // duplicate. That is a resolution, not a failure — terminate so the
      // Step Functions definition routes this execution to Succeed instead of
      // Recover-looping on a crash every cycle until rotation.
      console.error(
        "Controller chain heartbeat superseded; terminating this duplicate execution",
        {
          executionArn: input.executionArn,
          chainGeneration: input.chainGeneration,
        },
      );
      await this.emitQuietExitMetric("SUPERSEDED");
      return output({ rotate: false, terminate: true });
    }
    await this.dependencies.state.expireReservations(now);
    const control = await this.dependencies.state.readControlSnapshot();
    try {
      await this.dependencies.state.claimCycle({
        cycleId,
        authorityGeneration: control.authority.generation,
        ledgerGeneration: control.ledger.generation,
        now,
      });
    } catch (error) {
      // Broader than the heartbeat guard above (which stays strictly
      // conditional-check: a SUPERSEDED verdict must never rest on a
      // throttle): two invokes racing the claim in the same instant surface
      // as TransactionConflict rather than a failed condition, and capacity
      // throttling is equally "not my turn" — both are absorbed here.
      if (!isRetriableDynamoContention(error)) throw error;
      // Losing the cycle claim is the expected race, not an error: another
      // invoke holds the RECONCILER lock (a redeploy's overlapping invoke, or
      // a timed-out cycle's lock still running out RECONCILER_LOCK_MS), or the
      // authority/ledger generation moved between snapshot and claim. This was
      // the 10-15 min TransactionCanceledException crash-burst on every
      // controller deploy; it is now a quiet "not my turn" no-op and the chain
      // simply retries next cycle. Genuinely unexpected failures (validation,
      // missing table, denied credentials) still throw above.
      console.error(
        "Reconcile cycle claim lost to a concurrent holder; skipping this cycle",
        {
          cycleId,
          executionArn: input.executionArn,
          error: error instanceof Error ? error.message : String(error),
        },
      );
      await this.emitQuietExitMetric("LOCK_CONTENDED");
      // rotate is forced false: a LOSING invocation at the rotation boundary
      // must not start a successor — the holder's own cycle output carries
      // the rotation, and a loser rotating would deliberately mint a
      // duplicate execution.
      return output({ rotate: false });
    }
    let result: ReconcilerState["lastResult"] = "FAILED";
    let inputHash = "unavailable";
    const audit: Record<string, unknown> = {
      cycleId,
      chainGeneration: input.chainGeneration,
      cycleIndex: input.cycleIndex,
      manifestVersion: TEMPORAL_CAPACITY_MANIFEST_VERSION,
    };
    let lastAwsSnapshot:
      | Awaited<ReturnType<AwsCapacityReader["read"]>>["snapshot"]
      | undefined;
    let ungrantedProdReplicas = 0;
    let staleTemporalInputs = 0;
    let staleWorkerHeartbeats = 0;
    let pendingTasks = 0;
    let desiredNotReadyReplicas = 0;
    let drainDeadlineExpired = 0;
    let drainedAwaitingRetirement = 0;
    let retirementVerifyTimeouts = 0;
    let queueObservations: QueueCapacityObservation[] = [];
    let serviceTimes = control.reconciler.serviceTimes;
    const scaleIn = { ...control.reconciler.scaleIn };
    try {
      const awsRead = await this.dependencies.aws.read(
        control.ledger.activeReservationVcpu,
      );
      lastAwsSnapshot = awsRead.snapshot;
      // Persist the fresh capacity picture so deploy-time reserve/release can
      // admit/release against it without each running their own full
      // account-wide ECS inventory scan. Best-effort: a write failure must not
      // fail the reconcile cycle (reserve/release fall back to a live read).
      try {
        await this.dependencies.state.writeReservationSnapshot({
          snapshot: awsRead.snapshot,
          services: awsRead.services,
          capturedAt: awsRead.snapshot.capturedAt,
        });
      } catch (error) {
        console.error(
          "Capacity reservation snapshot persistence failed",
          error,
        );
      }
      pendingTasks = awsRead.services.reduce(
        (total, service) => total + service.pendingCount,
        0,
      );
      desiredNotReadyReplicas = awsRead.services.reduce(
        (total, service) =>
          total + Math.max(0, service.desiredCount - service.runningCount),
        0,
      );
      const ledgerPendingVcpu = awsRead.services.reduce((total, service) => {
        const observedVcpu =
          Math.max(
            service.desiredCount,
            service.runningCount + service.pendingCount,
          ) *
          (service.cpuUnits / 1024);
        return (
          total +
          Math.max(
            0,
            (control.ledger.allocations[service.serviceArn] ?? 0) -
              observedVcpu,
          )
        );
      }, 0);
      const servicesWithCommitments = awsRead.services.map((service) => ({
        ...service,
        committedDesiredCount: Math.max(
          service.desiredCount,
          Math.ceil(
            (control.ledger.allocations[service.serviceArn] ?? 0) /
              (service.cpuUnits / 1024),
          ),
        ),
      }));
      const temporalRead = await this.dependencies.temporal.read(
        servicesWithCommitments,
      );
      queueObservations = temporalRead.observations;
      const activeDrains = await this.dependencies.state.listActiveDrains();
      // Drop scale-in state for services that no longer exist. Every worker
      // deploy mints a fresh set of suffix-hashed ECS services, and nothing
      // else ever deletes their scaleIn entries, so the map accretes one entry
      // per retired service forever — the growth that pushed the RECONCILER
      // item past DynamoDB's 400KB cap and failed every completeCycle write
      // (2026-07-14 incident: 1,892 entries for 268 live services). scaleIn
      // keys are only ever written for ARNs drawn from this same inventory or
      // from an active drain record, so their union bounds the map to the
      // live fleet. An inventory read failure aborts the cycle above before
      // reaching this prune, so a partial view can never wipe live state.
      const liveScaleInArns = new Set(
        awsRead.services.map((service) => service.serviceArn),
      );
      for (const drain of activeDrains) liveScaleInArns.add(drain.serviceArn);
      for (const serviceArn of Object.keys(scaleIn)) {
        if (!liveScaleInArns.has(serviceArn)) delete scaleIn[serviceArn];
      }
      const activeBuildPools = new Map<string, Set<TemporalStablePoolId>>();
      for (const service of temporalRead.services) {
        if (
          service.buildState !== "CURRENT" &&
          service.buildState !== "RAMPING" &&
          service.buildState !== "DRAINING"
        ) {
          continue;
        }
        const key = `${service.environment}#${service.deploymentName}#${service.buildId}`;
        const pools =
          activeBuildPools.get(key) ?? new Set<TemporalStablePoolId>();
        pools.add(service.poolId);
        activeBuildPools.set(key, pools);
      }
      for (const [build, pools] of activeBuildPools) {
        const [environment, deploymentName, buildId] = build.split("#");
        const buildServices = temporalRead.services.filter(
          (service) =>
            service.environment === environment &&
            service.deploymentName === deploymentName &&
            service.buildId === buildId,
        );
        const recoveringPools = new Set(
          activeDrains
            .filter(
              (drain) =>
                (drain.state === "APPLYING" || drain.state === "VERIFYING") &&
                drain.buildId === buildId &&
                buildServices.some(
                  (service) => service.clusterArn === drain.clusterArn,
                ),
            )
            .map((drain) => drain.poolId),
        );
        const missing = TEMPORAL_STABLE_POOL_IDS.filter(
          (poolId) =>
            // Optional pools (v3) are absent from active builds that predate
            // them; their absence must not fail the reconcile cycle.
            !isTemporalPoolOptional(poolId) &&
            !pools.has(poolId) &&
            !recoveringPools.has(poolId),
        );
        if (missing.length > 0) {
          throw new Error(
            `Active Temporal build ${build} is missing pool services: ${missing.join(",")}`,
          );
        }
      }
      // Builds fully zeroed by the retirement lane but not yet deleted by the
      // iac retire verb: the verb's defer rule clears once desired+running
      // reach zero, so a sustained nonzero count means the reaper is wedged.
      const drainedBuildResidualCapacity = new Map<string, number>();
      for (const service of temporalRead.services) {
        if (service.buildState !== "DRAINED") continue;
        const key = `${service.environment}#${service.deploymentName}#${service.buildId}`;
        drainedBuildResidualCapacity.set(
          key,
          (drainedBuildResidualCapacity.get(key) ?? 0) +
            service.desiredCount +
            service.runningCount +
            service.pendingCount,
        );
      }
      drainedAwaitingRetirement = [
        ...drainedBuildResidualCapacity.values(),
      ].filter((residual) => residual === 0).length;
      staleTemporalInputs =
        temporalRead.observations.filter((observation) => !observation.fresh)
          .length + temporalRead.staleReadCount;
      staleWorkerHeartbeats = temporalRead.observations.filter(
        (observation) => !observation.workerTelemetryFresh,
      ).length;
      serviceTimes = updateServiceTimes({
        observations: temporalRead.observations,
        previous: serviceTimes,
        now: Date.now(),
      });
      const demands = temporalRead.services.map((service) =>
        calculatePoolDemand(
          service,
          observationsForService(service, temporalRead.observations),
        ),
      );
      const allocation = allocateGlobalCapacity(demands, {
        ...awsRead.snapshot,
        activeReservationVcpu:
          awsRead.snapshot.activeReservationVcpu + ledgerPendingVcpu,
      });
      ungrantedProdReplicas = allocation.ungrantedProdReplicas;
      inputHash = stableHash({
        inventoryHash: awsRead.inventoryHash,
        observations: temporalRead.observations,
        authority: control.authority,
        ledger: control.ledger,
      });
      Object.assign(audit, {
        inputHash,
        inventoryHash: awsRead.inventoryHash,
        authority: control.authority,
        snapshot: awsRead.snapshot,
        demands,
        allocation,
      });

      const maintenance = (
        await this.dependencies.state.listActiveMaintenanceRedeploys()
      ).sort((left, right) => left.createdAt - right.createdAt)[0];
      const drainRequiresCompletion = activeDrains.some(
        (drain) => drain.state === "APPLYING" || drain.state === "VERIFYING",
      );
      const updates = updatesInPriorityOrder(allocation.grants);
      if (control.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA") {
        result = "NON_AUTHORITATIVE";
      } else if (updates.length === 0 || drainRequiresCompletion) {
        // Isolate the per-drain scale-in step: a single drain's transient
        // DynamoDB contention (a lost fence/claim race, a concurrent
        // transaction, or throttling) must never abort the global dev+prod
        // reconcile cycle — it would surface as a Lambda Invoke Error and the
        // cycle would do nothing for every environment. Convert such a failure
        // to a PARTIAL result and continue so the cycle completes and the drain
        // retries next cycle. Genuine systemic errors (validation, missing
        // table, denied credentials, or any non-DynamoDB bug) fall outside the
        // contention allowlist and still fail loudly.
        try {
          const scaleInResult = await this.reconcileProtectedScaleIn({
            cycleId,
            authorityGeneration: control.authority.generation,
            ledger: control.ledger,
            demands,
            observations: temporalRead.observations,
            workers: temporalRead.workerProcesses,
            tasks: awsRead.tasks,
            staleReadCount: temporalRead.staleReadCount,
            incompleteWorkerDeployments:
              temporalRead.incompleteWorkerDeployments,
            scaleIn,
            maintenance,
            now: Date.now(),
          });
          audit.scaleInWrites = scaleInResult.writes;
          drainDeadlineExpired = scaleInResult.writes.filter(
            (write) =>
              write.result === "CANCELLED" &&
              write.reason === "DRAIN_DEADLINE_EXPIRED",
          ).length;
          retirementVerifyTimeouts = scaleInResult.writes.filter(
            (write) => write.result === "RETIREMENT_VERIFY_TIMEOUT",
          ).length;
          result = scaleInResult.partial
            ? "PARTIAL"
            : scaleInResult.applied
              ? "APPLIED"
              : "NOOP";
        } catch (error) {
          if (!isRetriableDynamoContention(error)) throw error;
          result = "PARTIAL";
          audit.scaleInDeferred =
            error instanceof Error ? error.message : String(error);
          console.error(
            "Protected scale-in step deferred after transient DynamoDB contention; cycle continues and the drain retries next cycle",
            { cycleId, error },
          );
        }
      } else {
        for (const drain of activeDrains) {
          if (
            drain.kind === "SCALE_IN" &&
            (drain.state === "INTENT" || drain.state === "READY")
          ) {
            await this.dependencies.state.cancelDrain({
              drain,
              reason: "SCALE_OUT_PENDING",
              now: Date.now(),
            });
          }
        }
        const additionalManagedVcpu = updates.reduce(
          (total, grant) => total + grant.additionalVcpu,
          0,
        );
        const ledgerGeneration =
          await this.dependencies.state.claimCapacityPlan({
            cycleId,
            authorityGeneration: control.authority.generation,
            ledger: control.ledger,
            additionalManagedVcpu,
            observedManagedVcpu: awsRead.snapshot.managedCommittedVcpu,
            pendingLedgerVcpu: ledgerPendingVcpu,
            allocations: allocationsForGrants(allocation.grants),
            inventoryHash: awsRead.inventoryHash,
            now: Date.now(),
          });
        const writes: Array<Record<string, unknown>> = [];
        let partial = false;
        const freshControl =
          await this.dependencies.state.readControlSnapshot();
        if (freshControl.ledger.generation !== ledgerGeneration) {
          partial = true;
        }
        const freshAws = await this.dependencies.aws.read(
          freshControl.ledger.activeReservationVcpu,
        );
        const plannedManagedVcpu =
          awsRead.snapshot.managedCommittedVcpu +
          ledgerPendingVcpu +
          additionalManagedVcpu;
        const freshCommittedVcpu =
          Math.max(plannedManagedVcpu, freshAws.snapshot.managedCommittedVcpu) +
          freshAws.snapshot.unmanagedCommittedVcpu +
          freshAws.snapshot.activeReservationVcpu;
        const quotaCeilingVcpu =
          freshAws.snapshot.quotaVcpu - freshAws.snapshot.hardReserveVcpu;
        // Environment-budget twin of the quota ceiling: env-scoped committed
        // (managed + reservations, no unmanaged) must stay inside the static
        // partition even if inputs moved since allocation.
        const freshEnvCommittedVcpu =
          Math.max(plannedManagedVcpu, freshAws.snapshot.managedCommittedVcpu) +
          freshAws.snapshot.activeReservationVcpu;
        const envBudgetCeilingVcpu =
          this.config.environmentVcpuBudget ?? Number.POSITIVE_INFINITY;
        const liveServices = new Map(
          freshAws.services.map((service) => [service.serviceArn, service]),
        );
        const expectedDesired = new Map(
          updates.map((grant) => [
            grant.service.serviceArn,
            grant.service.desiredCount,
          ]),
        );
        const changedService = updates.find((grant) => {
          const live = liveServices.get(grant.service.serviceArn);
          return (
            !live ||
            live.desiredCount !==
              expectedDesired.get(grant.service.serviceArn) ||
            live.taskDefinitionArn !== grant.service.taskDefinitionArn ||
            live.cpuUnits !== grant.service.cpuUnits
          );
        });
        if (
          partial ||
          freshCommittedVcpu > quotaCeilingVcpu ||
          freshEnvCommittedVcpu > envBudgetCeilingVcpu ||
          changedService !== undefined
        ) {
          partial = true;
          writes.push({
            result: "AWS_INPUT_CHANGED",
            freshCommittedVcpu,
            quotaCeilingVcpu,
            ...(this.config.environmentVcpuBudget !== undefined
              ? { freshEnvCommittedVcpu, envBudgetCeilingVcpu }
              : {}),
            ...(changedService
              ? { serviceArn: changedService.service.serviceArn }
              : {}),
          });
        }
        for (const grant of partial ? [] : updates) {
          if (
            !(await this.dependencies.state.verifyWriteFence({
              cycleId,
              authorityGeneration: control.authority.generation,
              ledgerGeneration,
              now: Date.now(),
            }))
          ) {
            partial = true;
            writes.push({
              serviceArn: grant.service.serviceArn,
              result: "FENCE_CHANGED",
            });
            break;
          }
          try {
            const currentControl =
              await this.dependencies.state.readControlSnapshot();
            if (currentControl.ledger.generation !== ledgerGeneration) {
              throw new Error(
                "Capacity ledger changed before actuation; retrying from fresh inputs",
              );
            }
            const currentAws = await this.dependencies.aws.read(
              currentControl.ledger.activeReservationVcpu,
            );
            const currentCommittedVcpu =
              Math.max(
                plannedManagedVcpu,
                currentAws.snapshot.managedCommittedVcpu,
              ) +
              currentAws.snapshot.unmanagedCommittedVcpu +
              currentAws.snapshot.activeReservationVcpu;
            const currentCeilingVcpu =
              currentAws.snapshot.quotaVcpu -
              currentAws.snapshot.hardReserveVcpu;
            const liveService = currentAws.services.find(
              (service) => service.serviceArn === grant.service.serviceArn,
            );
            if (!liveService) {
              throw new Error(
                `Managed service ${grant.service.serviceArn} disappeared before actuation`,
              );
            }
            if (
              liveService.desiredCount !==
              expectedDesired.get(grant.service.serviceArn)
            ) {
              throw new Error(
                `Managed service ${grant.service.serviceArn} desired count changed before actuation`,
              );
            }
            if (currentCommittedVcpu > currentCeilingVcpu) {
              throw new Error(
                `Fargate capacity changed before actuation: committed=${currentCommittedVcpu} ceiling=${currentCeilingVcpu}`,
              );
            }
            const currentEnvCommittedVcpu =
              Math.max(
                plannedManagedVcpu,
                currentAws.snapshot.managedCommittedVcpu,
              ) + currentAws.snapshot.activeReservationVcpu;
            if (currentEnvCommittedVcpu > envBudgetCeilingVcpu) {
              throw new Error(
                `Environment capacity changed before actuation: committed=${currentEnvCommittedVcpu} budget=${envBudgetCeilingVcpu}`,
              );
            }
            if (
              liveService.taskDefinitionArn !==
                grant.service.taskDefinitionArn ||
              liveService.cpuUnits !== grant.service.cpuUnits
            ) {
              throw new Error(
                `Managed service ${grant.service.serviceArn} task shape changed before actuation`,
              );
            }
            if (
              !(await this.dependencies.state.verifyWriteFence({
                cycleId,
                authorityGeneration: control.authority.generation,
                ledgerGeneration,
                now: Date.now(),
              }))
            ) {
              throw new Error(
                "Capacity write fence changed during final AWS revalidation",
              );
            }
            const requestId = await this.dependencies.aws.updateDesiredCount(
              liveService,
              grant.granted,
            );
            expectedDesired.set(grant.service.serviceArn, grant.granted);
            writes.push({
              serviceArn: grant.service.serviceArn,
              priorDesired: grant.priorDesired,
              desired: grant.granted,
              requestId,
              result: "APPLIED",
            });
          } catch (error) {
            partial = true;
            writes.push({
              serviceArn: grant.service.serviceArn,
              result: "FAILED",
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }
        audit.writes = writes;
        result = partial ? "PARTIAL" : "APPLIED";
      }
    } catch (error) {
      audit.error = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      await this.dependencies.state.completeCycle({
        cycleId,
        authorityGeneration: control.authority.generation,
        result,
        inputHash,
        audit,
        serviceTimes,
        scaleIn,
        ungrantedProdReplicas,
        staleTemporalInputs,
        now: Date.now(),
      });
      if (lastAwsSnapshot) {
        try {
          await this.dependencies.aws.emitCycleMetrics({
            result,
            managedCommittedVcpu: lastAwsSnapshot.managedCommittedVcpu,
            accountUsageVcpu: lastAwsSnapshot.accountUsageVcpu,
            quotaVcpu: lastAwsSnapshot.quotaVcpu,
            ungrantedProdReplicas,
            cycleDurationMs: Date.now() - now,
            staleTemporalInputs,
            staleWorkerHeartbeats,
            pendingTasks,
            desiredNotReadyReplicas,
            drainDeadlineExpired,
            drainedAwaitingRetirement,
            retirementVerifyTimeouts,
          });
        } catch (error) {
          console.error(
            "Capacity cycle metrics emission failed after cycle completion",
            error,
          );
        }
      }
      if (queueObservations.length > 0) {
        try {
          await this.dependencies.aws.emitQueueBacklogMetrics({
            observations: queueObservations,
          });
        } catch (error) {
          console.error(
            "Queue backlog metrics emission failed after cycle completion",
            error,
          );
        }
      }
    }

    return output();
  }

  // Capacity inputs for deploy-time reserve/release. Prefers the snapshot the
  // reconcile loop persists each cycle (zero ECS calls, so deploys never
  // compete for the account ECS API rate budget or block on the slow
  // full-account scan). A stale-but-present snapshot falls back to a single
  // optimized live read floored by the last known unmanaged usage; a cold
  // cache (no snapshot yet) falls back to the robust full read so we never
  // admit against unknown capacity. Never fails closed. The reservation ledger
  // value is always the fresh, transactionally-read one supplied by the
  // caller; only the slow-moving capacity figures come from the cache.
  private async resolveReservationCapacity(
    activeReservationVcpu: number,
  ): Promise<{
    services: ManagedTemporalService[];
    snapshot: CapacitySnapshot;
    source: "cache" | "live";
  }> {
    const cached = await this.dependencies.state.readReservationSnapshot();
    if (
      cached &&
      Date.now() - cached.capturedAt <= this.config.reservationSnapshotMaxAgeMs
    ) {
      return {
        services: cached.services,
        snapshot: { ...cached.snapshot, activeReservationVcpu },
        source: "cache",
      };
    }
    if (cached) {
      // Stale but present: the optimized read's CloudWatch-only usage can lag,
      // so floor unmanaged at the last full read's value to avoid over-admit.
      const live = await this.dependencies.aws.readForReservation(
        activeReservationVcpu,
        { unmanagedFloorVcpu: cached.snapshot.unmanagedCommittedVcpu },
      );
      return { ...live, source: "live" };
    }
    // Cold cache (no snapshot persisted yet, e.g. right after a controller
    // deploy and before the first reconcile cycle): there is no trustworthy
    // unmanaged floor, and the optimized read would treat a missing CloudWatch
    // datapoint as zero usage and over-admit. Pay for one robust full read —
    // its directFargateInventory scan makes account usage authoritative — in
    // this rare bootstrap case rather than admit against unknown capacity.
    const full = await this.dependencies.aws.read(activeReservationVcpu);
    return { services: full.services, snapshot: full.snapshot, source: "live" };
  }

  async reserveDeployment(params: {
    reservationId: string;
    ownerToken: string;
    environment: CapacityEnvironment;
    buildId: string;
    pools: Array<TemporalStablePoolId | "preview-composite">;
    requestedVcpu: number;
    expiresAt: number;
  }) {
    const now = Date.now();
    if (params.expiresAt <= now || params.expiresAt > now + 60 * 60_000) {
      throw new Error("Deployment reservation expiry must be within one hour");
    }
    // An environment-scoped controller only admits its own environment's
    // deployments — a misrouted reservation must fail loudly, not consume a
    // foreign budget. Preview workers have no controller of their own and
    // ride their host environment's instance (dev today, staging once
    // previews move there); prod never hosts previews.
    if (this.config.environmentScope !== undefined) {
      const allowed =
        params.environment === this.config.environmentScope ||
        (params.environment === "preview" &&
          this.config.environmentScope !== "prod");
      if (!allowed) {
        throw new Error(
          `Reservation environment ${params.environment} is not admissible on the ${this.config.environmentScope}-scoped controller`,
        );
      }
    }
    await this.dependencies.state.initialize();
    await this.dependencies.state.expireReservations(now);
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const existing = await this.dependencies.state.readReservation(
        params.reservationId,
      );
      if (existing) {
        if (existing.ownerToken !== params.ownerToken) {
          throw new Error("Deployment reservation owner token does not match");
        }
        if (
          existing.environment !== params.environment ||
          existing.buildId !== params.buildId ||
          existing.requestedVcpu !== params.requestedVcpu ||
          existing.expiresAt !== params.expiresAt ||
          JSON.stringify([...existing.pools].sort()) !==
            JSON.stringify([...params.pools].sort())
        ) {
          throw new Error(
            `Reservation ${existing.reservationId} retry does not match the original request`,
          );
        }
        if (existing.state === "ACTIVE" || existing.state === "CONSUMING") {
          return {
            reservationId: existing.reservationId,
            ledgerGeneration: existing.ledgerGeneration,
          };
        }
        throw new Error(
          `Reservation ${existing.reservationId} is already ${existing.state}`,
        );
      }
      const control = await this.dependencies.state.readControlSnapshot();
      const awsRead = await this.resolveReservationCapacity(
        control.ledger.activeReservationVcpu,
      );
      const pendingLedgerVcpu = awsRead.services.reduce((total, service) => {
        const observedVcpu =
          Math.max(
            service.desiredCount,
            service.runningCount + service.pendingCount,
          ) *
          (service.cpuUnits / 1024);
        return (
          total +
          Math.max(
            0,
            (control.ledger.allocations[service.serviceArn] ?? 0) -
              observedVcpu,
          )
        );
      }, 0);
      const quotaReservableVcpu = Math.max(
        0,
        awsRead.snapshot.quotaVcpu -
          awsRead.snapshot.hardReserveVcpu -
          awsRead.snapshot.unmanagedCommittedVcpu -
          awsRead.snapshot.managedCommittedVcpu -
          control.ledger.activeReservationVcpu -
          pendingLedgerVcpu -
          (params.environment === "prod"
            ? 0
            : Math.max(
                0,
                awsRead.snapshot.prodGuaranteedEnvelopeVcpu -
                  awsRead.services
                    .filter((service) => service.environment === "prod")
                    .reduce(
                      (total, service) =>
                        total +
                        service.desiredCount * (service.cpuUnits / 1024),
                      0,
                    ),
              )),
      );
      // Environment-scoped controllers additionally clamp admission to the
      // static per-environment budget. Managed committed, ledger reservations,
      // and pending ledger deltas are all env-scoped in scoped mode, so this
      // is exactly "does the deployment fit inside the partition".
      const budgetReservableVcpu =
        this.config.environmentVcpuBudget === undefined
          ? Number.POSITIVE_INFINITY
          : Math.max(
              0,
              this.config.environmentVcpuBudget -
                awsRead.snapshot.managedCommittedVcpu -
                control.ledger.activeReservationVcpu -
                pendingLedgerVcpu,
            );
      const maxReservableVcpu = Math.min(
        quotaReservableVcpu,
        budgetReservableVcpu,
      );
      if (params.requestedVcpu > maxReservableVcpu) {
        throw new Error(
          `Insufficient Fargate capacity for reservation ${params.reservationId}: requested=${params.requestedVcpu} available=${maxReservableVcpu}`,
        );
      }
      try {
        const ledgerGeneration = await this.dependencies.state.admitReservation(
          {
            reservation: {
              reservationId: params.reservationId,
              ownerToken: params.ownerToken,
              environment: params.environment,
              buildId: params.buildId,
              pools: params.pools,
              requestedVcpu: params.requestedVcpu,
              expiresAt: params.expiresAt,
            },
            expectedLedgerGeneration: control.ledger.generation,
            expectedActiveReservationVcpu: control.ledger.activeReservationVcpu,
            now,
          },
        );
        return { reservationId: params.reservationId, ledgerGeneration };
      } catch (error) {
        if (
          !this.dependencies.state.isConditionalFailure(error) ||
          attempt === 5
        ) {
          throw error;
        }
      }
    }
    throw new Error("Reservation admission retries exhausted");
  }

  async releaseDeployment(params: {
    reservationId: string;
    ownerToken: string;
  }) {
    await this.dependencies.state.initialize();
    const reservation = await this.dependencies.state.readReservation(
      params.reservationId,
    );
    if (!reservation) return { released: false, reason: "NOT_FOUND" };
    if (reservation.ownerToken !== params.ownerToken) {
      throw new Error("Deployment reservation owner token does not match");
    }
    if (reservation.state === "RELEASED" || reservation.state === "EXPIRED") {
      return { released: false, reason: reservation.state };
    }
    let unconsumed = false;
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const control = await this.dependencies.state.readControlSnapshot();
      const awsRead = await this.resolveReservationCapacity(
        control.ledger.activeReservationVcpu,
      );
      if (reservation.environment !== "preview") {
        const consumedVcpuFrom = (services: ManagedTemporalService[]) =>
          services
            .filter(
              (service) =>
                service.environment === reservation.environment &&
                service.buildId === reservation.buildId &&
                reservation.pools.includes(service.poolId) &&
                service.runningCount + service.pendingCount > 0,
            )
            .reduce(
              (total, service) =>
                total +
                Math.max(
                  service.desiredCount,
                  service.runningCount + service.pendingCount,
                ) *
                  (service.cpuUnits / 1024),
              0,
            );
        let consumedVcpu = consumedVcpuFrom(awsRead.services);
        // The cached snapshot may predate the new build's service stabilizing
        // (deploy already passed `wait services-stable` before releasing). A
        // stale-but-in-window snapshot would wrongly report the reservation as
        // unconsumed and fail the deploy's release step, so confirm against a
        // live (still optimized) read before refusing.
        if (
          consumedVcpu < reservation.requestedVcpu &&
          awsRead.source === "cache"
        ) {
          const live = await this.dependencies.aws.readForReservation(
            control.ledger.activeReservationVcpu,
            { unmanagedFloorVcpu: awsRead.snapshot.unmanagedCommittedVcpu },
          );
          consumedVcpu = consumedVcpuFrom(live.services);
        }
        // An unconsumed reservation is the *safe* direction: rollout surge we
        // reserved but the new build never claimed (e.g. a floor-only or
        // contracted pool, or a pool the inventory read excludes). Refusing to
        // release it here hard-fails the deploy's release step *after* services
        // already stabilized, which strands the build unpromoted — the exact
        // sequence that took prod down (deploy 0457f57252a2). The reservation
        // would auto-expire regardless, so release it now and surface the
        // shortfall as a signal instead of aborting the rollout.
        unconsumed = consumedVcpu < reservation.requestedVcpu;
        if (unconsumed) {
          console.warn(
            JSON.stringify({
              event: "capacity.reservation.released_unconsumed",
              reservationId: reservation.reservationId,
              environment: reservation.environment,
              buildId: reservation.buildId,
              requestedVcpu: reservation.requestedVcpu,
              observedVcpu: consumedVcpu,
              attempt,
            }),
          );
        }
      }
      try {
        const ledgerGeneration =
          await this.dependencies.state.releaseReservation({
            reservation,
            expectedLedgerGeneration: control.ledger.generation,
            terminalState: "RELEASED",
            now: Date.now(),
          });
        return unconsumed
          ? { released: true, ledgerGeneration, reason: "UNCONSUMED" as const }
          : { released: true, ledgerGeneration };
      } catch (error) {
        if (
          !this.dependencies.state.isConditionalFailure(error) ||
          attempt === 5
        ) {
          throw error;
        }
      }
    }
    throw new Error("Reservation release retries exhausted");
  }

  async updateManagedService(
    params: Parameters<AwsCapacityReader["updateManagedService"]>[0] & {
      reservationId?: string;
      reservationOwnerToken?: string;
    },
  ) {
    await this.dependencies.state.initialize();
    const control = await this.dependencies.state.readControlSnapshot();
    if (
      params.desiredCount !== undefined &&
      params.taskDefinitionArn !== undefined &&
      !params.forceNewDeployment
    ) {
      throw new Error(
        "Task-definition rollout updates must explicitly request a force deployment",
      );
    }
    if (
      control.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA" &&
      params.desiredCount !== undefined
    ) {
      throw new Error(
        "Only task-definition or force-redeployment maintenance is allowed before controller writer cutover",
      );
    }
    if (params.desiredCount !== undefined) {
      if (!params.reservationId || !params.reservationOwnerToken) {
        throw new Error(
          "Desired-count maintenance updates require a capacity reservation",
        );
      }
      const reservation = await this.dependencies.state.readReservation(
        params.reservationId,
      );
      if (
        !reservation ||
        reservation.ownerToken !== params.reservationOwnerToken ||
        (reservation.state !== "ACTIVE" &&
          !(
            reservation.state === "CONSUMING" &&
            reservation.serviceArns.includes(params.serviceArn)
          )) ||
        reservation.expiresAt <= Date.now()
      ) {
        throw new Error("Capacity reservation is absent, stale, or not owned");
      }
      const service = (
        await this.dependencies.aws.read(
          (
            await this.dependencies.state.readControlSnapshot()
          ).ledger.activeReservationVcpu,
        )
      ).services.find(
        (candidate) => candidate.serviceArn === params.serviceArn,
      );
      if (!service) {
        throw new Error(`Managed service ${params.serviceArn} was not found`);
      }
      if (
        reservation.environment !== service.environment ||
        reservation.buildId !== service.buildId ||
        !reservation.pools.includes(service.poolId)
      ) {
        throw new Error(
          `Reservation ${reservation.reservationId} does not cover ${params.serviceArn}`,
        );
      }
      const additionalVcpu =
        Math.max(0, params.desiredCount - service.desiredCount) *
        (service.cpuUnits / 1024);
      for (let attempt = 1; attempt <= 5; attempt += 1) {
        if (
          reservation.state === "CONSUMING" &&
          reservation.serviceArns.includes(service.serviceArn)
        ) {
          break;
        }
        try {
          await this.dependencies.state.consumeReservation({
            reservationId: reservation.reservationId,
            ownerToken: reservation.ownerToken,
            serviceArn: service.serviceArn,
            additionalVcpu,
            now: Date.now(),
          });
          break;
        } catch (error) {
          if (
            !this.dependencies.state.isConditionalFailure(error) ||
            attempt === 5
          ) {
            throw error;
          }
          const current = await this.dependencies.state.readReservation(
            reservation.reservationId,
          );
          if (current?.serviceArns.includes(service.serviceArn)) break;
          if (
            !current ||
            current.ownerToken !== reservation.ownerToken ||
            current.state !== "ACTIVE" ||
            current.expiresAt <= Date.now()
          ) {
            throw new Error(
              `Reservation ${reservation.reservationId} changed or expired during consumption`,
              { cause: error },
            );
          }
        }
      }
    } else if (
      control.authority.writerKind === "STEP_FUNCTIONS_LAMBDA" &&
      (params.taskDefinitionArn !== undefined ||
        params.forceNewDeployment === true)
    ) {
      throw new Error(
        "Managed task-definition and force redeploys require an explicit desired count and rollout reservation",
      );
    }
    return this.dependencies.aws.updateManagedService(params);
  }

  async checkLoadGate(params: {
    stageJamsPerMinute: number;
    projectedActionsPerSecond: number;
    namespaceActionsLimitPerSecond: number;
    projectedRequestsPerSecond: number;
    namespaceRequestsLimitPerSecond: number;
    projectedOperationsPerSecond: number;
    namespaceOperationsLimitPerSecond: number;
  }) {
    let control: Awaited<ReturnType<CapacityStateStore["readControlSnapshot"]>>;
    try {
      control = await this.dependencies.state.readControlSnapshot();
    } catch {
      await this.dependencies.aws.emitLoadGateMetrics({ allowed: false });
      return {
        allowed: false,
        reasons: ["CAPACITY_CONTROL_STATE_UNAVAILABLE"],
        warnings: [],
        stageJamsPerMinute: params.stageJamsPerMinute,
        projectedActionsPerSecond: params.projectedActionsPerSecond,
        checkedAt: Date.now(),
      };
    }
    const reasons: string[] = [];
    const warnings: string[] = [];
    const namespaceUtilization = [
      {
        kind: "APS",
        projected: params.projectedActionsPerSecond,
        limit: params.namespaceActionsLimitPerSecond,
      },
      {
        kind: "RPS",
        projected: params.projectedRequestsPerSecond,
        limit: params.namespaceRequestsLimitPerSecond,
      },
      {
        kind: "OPS",
        projected: params.projectedOperationsPerSecond,
        limit: params.namespaceOperationsLimitPerSecond,
      },
    ];
    for (const input of namespaceUtilization) {
      if (
        !Number.isFinite(input.projected) ||
        !Number.isFinite(input.limit) ||
        input.projected < 0 ||
        input.limit <= 0
      ) {
        reasons.push(`INVALID_PROJECTED_${input.kind}`);
        continue;
      }
      const utilization = input.projected / input.limit;
      if (utilization >= 0.7) {
        reasons.push(`${input.kind}_PROJECTED_AT_OR_ABOVE_70_PERCENT`);
      } else if (utilization >= 0.6) {
        warnings.push(`${input.kind}_PROJECTED_AT_OR_ABOVE_60_PERCENT`);
      }
    }
    if (control.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA") {
      reasons.push("CONTROLLER_NOT_AUTHORITATIVE");
    }
    if (
      !control.reconciler.lastCompletedAt ||
      Date.now() - control.reconciler.lastCompletedAt >
        this.config.cycleStaleAfterMs
    ) {
      reasons.push("CONTROLLER_CYCLE_STALE");
    }
    if (
      control.reconciler.lastResult === "PARTIAL" ||
      control.reconciler.lastResult === "FAILED"
    ) {
      reasons.push(`CONTROLLER_${control.reconciler.lastResult}`);
    }
    if ((control.reconciler.lastUngrantedProdReplicas ?? 0) > 0) {
      reasons.push("UNGRANTED_PRODUCTION_DEMAND");
    }
    if (
      Object.values(control.reconciler.scaleIn ?? {}).some(
        (state) => state.disabledReason !== undefined,
      )
    ) {
      reasons.push("PROTECTED_SCALE_IN_DISABLED");
    }
    let awsRead: Awaited<ReturnType<AwsCapacityReader["read"]>>;
    try {
      awsRead = await this.dependencies.aws.read(
        control.ledger.activeReservationVcpu,
      );
    } catch (error) {
      console.error("Capacity load gate AWS read failed", error);
      try {
        await this.dependencies.aws.emitLoadGateMetrics({ allowed: false });
      } catch (metricsError) {
        console.error(
          "Capacity load gate denial metric emission failed",
          metricsError,
        );
      }
      return {
        allowed: false,
        reasons: ["AWS_CAPACITY_READ_UNAVAILABLE"],
        warnings,
        stageJamsPerMinute: params.stageJamsPerMinute,
        projectedActionsPerSecond: params.projectedActionsPerSecond,
        checkedAt: Date.now(),
      };
    }
    if (awsRead.snapshot.accountUsageVcpu / awsRead.snapshot.quotaVcpu >= 0.7) {
      reasons.push("FARGATE_USAGE_AT_OR_ABOVE_70_PERCENT");
    }
    if (
      awsRead.snapshot.accountUsageVcpu +
        awsRead.snapshot.activeReservationVcpu +
        awsRead.snapshot.hardReserveVcpu >
      awsRead.snapshot.quotaVcpu
    ) {
      reasons.push("FARGATE_RESERVE_BREACH");
    }
    let temporalRead: Awaited<ReturnType<TemporalCapacityReader["read"]>>;
    try {
      temporalRead = await this.dependencies.temporal.read(awsRead.services);
    } catch (error) {
      console.error("Capacity load gate Temporal read failed", error);
      try {
        await this.dependencies.aws.emitLoadGateMetrics({ allowed: false });
      } catch (metricsError) {
        console.error(
          "Capacity load gate denial metric emission failed",
          metricsError,
        );
      }
      return {
        allowed: false,
        reasons: ["TEMPORAL_CAPACITY_READ_UNAVAILABLE"],
        warnings,
        stageJamsPerMinute: params.stageJamsPerMinute,
        projectedActionsPerSecond: params.projectedActionsPerSecond,
        checkedAt: Date.now(),
      };
    }
    if (temporalRead.staleReadCount > 0) {
      reasons.push("TEMPORAL_CAPACITY_READ_INCOMPLETE");
    }
    for (const service of temporalRead.services.filter(
      (candidate) =>
        candidate.environment === "prod" && candidate.buildState !== "DRAINED",
    )) {
      if (
        !temporalRead.observations.some(
          (observation) =>
            observation.environment === "prod" &&
            observation.deploymentName === service.deploymentName &&
            observation.buildId === service.buildId &&
            observation.poolId === service.poolId &&
            observation.fresh,
        )
      ) {
        reasons.push(`PROD_TEMPORAL_INPUT_MISSING:${service.serviceName}`);
      }
    }
    for (const observation of temporalRead.observations) {
      if (observation.environment !== "prod") continue;
      if (!observation.fresh) {
        reasons.push(`PROD_TEMPORAL_INPUT_STALE:${observation.taskQueue}`);
      }
      const threshold =
        TEMPORAL_STABLE_POOLS[observation.poolId].emergencyBacklogAgeSeconds;
      if (observation.backlogAgeSeconds >= threshold) {
        reasons.push(`PROD_BACKLOG_AGE_BREACH:${observation.taskQueue}`);
      }
    }
    const allowed = reasons.length === 0;
    await this.dependencies.aws.emitLoadGateMetrics({ allowed });
    return {
      allowed,
      reasons: [...new Set(reasons)].sort(),
      warnings: [...new Set(warnings)].sort(),
      stageJamsPerMinute: params.stageJamsPerMinute,
      projectedActionsPerSecond: params.projectedActionsPerSecond,
      checkedAt: Date.now(),
    };
  }
}

export const poolManifest = TEMPORAL_STABLE_POOLS;
