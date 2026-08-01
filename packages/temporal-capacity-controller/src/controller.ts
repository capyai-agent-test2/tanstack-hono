import { randomUUID } from "node:crypto";

import {
  TEMPORAL_CAPACITY_GRANT_TTL_MS,
  TEMPORAL_CAPACITY_MANIFEST_VERSION,
  TEMPORAL_SCALE_IN_COOLDOWN_SECONDS,
  TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS,
  TEMPORAL_SCALE_IN_INTENT_TIMEOUT_SECONDS,
  TEMPORAL_SCALE_IN_MAX_WAVE_FRACTION,
  TEMPORAL_SCALE_IN_SLOT_UTILIZATION,
  TEMPORAL_SCALE_IN_VERIFY_TIMEOUT_SECONDS,
  TEMPORAL_STABLE_POOL_IDS,
  TEMPORAL_STABLE_POOLS,
  isTemporalPoolOptional,
  temporalPoolIdForServiceName,
  type RetiringBuildEntry,
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
  CapacityGrant,
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
  RetirementAbortInput,
  RetirementBeginInput,
  RetirementCloseInput,
  RetirementReleaseInput,
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
    | "beginRetirement"
    | "releaseRetirementLedger"
    | "abortRetirement"
    | "closeRetirement"
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
  // DRAINED builds are owned by the iac retire verb's marker-fenced burial
  // (retirement-v2 §2) and never enter the live protected scale-in pipeline.
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

export const retiringBuildKey = (deploymentName: string, buildId: string) =>
  `${deploymentName}#${buildId}`;

// Retirement keep-out (retirement-v2 §4.3, I2): for every marker entry with
// state=OPEN ∧ expiresAt >= now, the build's services are not the
// controller's problem — the retire verb owns them. BURIED/ABORTED are
// terminal (pruned by the verb's next begin/close); an expired OPEN marker
// means the verb run died and the controller RESUMES ownership (harmless in
// every dangerous direction: the build is DRAINED, floor 0, no telemetry
// synthesis) — stickiness would recreate the F1 crashed-owner freeze.
// The state switch is exhaustive (I4): a new marker state without a
// classification here fails compilation.
export type RetirementKeepOut = {
  // `${deploymentName}#${buildId}` → marker-listed service ARNs for every
  // kept-out build (OPEN ∧ unexpired).
  openBuilds: Map<string, string[]>;
  // OPEN entries whose expiresAt has passed (lapsed — alarmable signal; the
  // controller has resumed ownership of those builds).
  expiredMarkers: number;
  // Age of the oldest OPEN, unexpired marker; >2 cron periods = wedged
  // reaper (replaces R1's drainedAwaitingRetirement).
  oldestOpenMarkerAgeSeconds: number;
};

export const computeRetirementKeepOut = (
  builds: Record<string, RetiringBuildEntry>,
  now: number,
): RetirementKeepOut => {
  const openBuilds = new Map<string, string[]>();
  let expiredMarkers = 0;
  let oldestOpenMarkerAgeSeconds = 0;
  for (const [buildId, entry] of Object.entries(builds)) {
    switch (entry.state) {
      case "OPEN": {
        if (entry.expiresAt < now) {
          expiredMarkers += 1;
          break;
        }
        openBuilds.set(
          retiringBuildKey(entry.deploymentName, buildId),
          Object.keys(entry.services),
        );
        oldestOpenMarkerAgeSeconds = Math.max(
          oldestOpenMarkerAgeSeconds,
          Math.floor((now - entry.createdAt) / 1_000),
        );
        break;
      }
      case "BURIED":
      case "ABORTED":
        break;
      default: {
        const unreachable: never = entry.state;
        throw new Error(`Unhandled retirement marker state ${unreachable}`);
      }
    }
  }
  return { openBuilds, expiredMarkers, oldestOpenMarkerAgeSeconds };
};

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

  // The live protected scale-in lane. DRAINED builds never enter it
  // (scaleInEligible hard-false) and marker-kept-out builds are excluded from
  // candidate selection below — retirement left the cycle entirely
  // (retirement-v2 §2, I3): the iac retire verb owns zero → release →
  // quiesce → delete for every build it has marked, and the R1 in-cycle
  // batch retirement lane is deleted.
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
    // Kept-out services (OPEN ∧ unexpired markers, minus live-conflict
    // overrides): never selected for scale-in or maintenance.
    keepOutServiceArns: ReadonlySet<string>;
    now: number;
  }): Promise<{
    writes: Array<Record<string, unknown>>;
    partial?: boolean;
    applied?: boolean;
  }> {
    const writes: Array<Record<string, unknown>> = [];
    const activeDrains = await this.dependencies.state.listActiveDrains();
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
      // Keep-out (retirement-v2 §4.3): a maintenance record targeting a
      // marked service defers untouched until the marker resolves — the verb
      // owns the service's desiredCount/existence while the marker is OPEN.
      if (params.keepOutServiceArns.has(params.maintenance.serviceArn)) {
        writes.push({
          maintenanceId: params.maintenance.maintenanceId,
          serviceArn: params.maintenance.serviceArn,
          result: "MAINTENANCE_DEFERRED_RETIRING",
        });
        return { writes, partial: true };
      }
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
        // Keep-out (retirement-v2 §4.3): marked services are excluded from
        // live scale-in candidacy while their burial marker is OPEN.
        if (params.keepOutServiceArns.has(demand.service.serviceArn)) {
          return false;
        }
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
    let retirementMarkerLiveConflicts = 0;
    let allocationDriftVcpu = 0;
    let grantPendingVcpu = 0;
    let grantsExpired = 0;
    let queueObservations: QueueCapacityObservation[] = [];
    let serviceTimes = control.reconciler.serviceTimes;
    const scaleIn = { ...control.reconciler.scaleIn };
    // Retirement keep-out (retirement-v2 §4.3): OPEN ∧ unexpired markers,
    // read atomically with the rest of the control snapshot.
    const keepOut = computeRetirementKeepOut(
      control.retiringBuilds.builds,
      now,
    );
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
        // Marked services never take the committed-allocation floor: the
        // ratcheted allocation re-upping a retiring build's demand is exactly
        // the F5 vector the keep-out kills (retirement-v2 §4.3).
        committedDesiredCount: keepOut.openBuilds.has(
          retiringBuildKey(service.deploymentName, service.buildId),
        )
          ? service.desiredCount
          : Math.max(
              service.desiredCount,
              Math.ceil(
                (control.ledger.allocations[service.serviceArn] ?? 0) /
                  (service.cpuUnits / 1024),
              ),
            ),
      }));
      const temporalRead = await this.dependencies.temporal.read(
        servicesWithCommitments,
        // Mid-burial builds are exempt from the reader's active-build
        // fail-loud check (I8): partial service deletion under an OPEN
        // marker must degrade that build, never kill the env cycle.
        { keepOutBuilds: new Set(keepOut.openBuilds.keys()) },
      );
      queueObservations = temporalRead.observations;
      // Live-state override (rollback safety valve, retirement-v2 §4.3): a
      // marked build observed CURRENT/RAMPING/DRAINING is live again —
      // break-glass SetCurrent to a mid-burial build must never find its
      // capacity frozen by a dead verb run. The controller ignores the
      // keep-out for that build and emits RETIREMENT_MARKER_LIVE_CONFLICT
      // (alarm >= 1); the verb's T5 recheck aborts its half of the race.
      const liveConflictBuildKeys = new Set<string>();
      for (const service of temporalRead.services) {
        const key = retiringBuildKey(service.deploymentName, service.buildId);
        if (!keepOut.openBuilds.has(key)) continue;
        if (
          service.buildState === "CURRENT" ||
          service.buildState === "RAMPING" ||
          service.buildState === "DRAINING"
        ) {
          liveConflictBuildKeys.add(key);
        }
      }
      retirementMarkerLiveConflicts = liveConflictBuildKeys.size;
      if (retirementMarkerLiveConflicts > 0) {
        console.error(
          "Retirement marker conflicts with live build state; keep-out overridden for the conflicted build(s)",
          { builds: [...liveConflictBuildKeys] },
        );
      }
      const keepOutServiceArns = new Set<string>();
      for (const [key, serviceArns] of keepOut.openBuilds) {
        if (liveConflictBuildKeys.has(key)) continue;
        for (const serviceArn of serviceArns) {
          keepOutServiceArns.add(serviceArn);
        }
      }
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
      // Keep-out (retirement-v2 §4.3): marked services' scaleIn entries are
      // pruned — the verb owns them and any residual wave/eligibility state
      // is stale by definition.
      for (const serviceArn of keepOutServiceArns) {
        delete scaleIn[serviceArn];
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
        // Marked builds are exempt from the required-pool fail-loud check
        // below (I8): a mid-burial build whose services are half-deleted
        // must not fail the env-wide cycle, even when a rollback made it
        // active again (the conflict metric above is the signal).
        if (
          keepOut.openBuilds.has(
            retiringBuildKey(service.deploymentName, service.buildId),
          )
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
      // Keep-out (retirement-v2 §4.3): marked services are excluded from
      // allocation — the controller never scales a build the verb is
      // burying. Live-conflict overrides were already subtracted above, so a
      // rolled-back build's capacity is not frozen by a dead verb run.
      const allocatableDemands = demands.filter(
        (demand) => !keepOutServiceArns.has(demand.service.serviceArn),
      );
      const allocation = allocateGlobalCapacity(allocatableDemands, {
        ...awsRead.snapshot,
        activeReservationVcpu:
          awsRead.snapshot.activeReservationVcpu + ledgerPendingVcpu,
      });
      // ─── Ledger v2 Phase A (retirement-v2 §3) ───
      // AllocationDriftVcpu = Σ max(0, charged − observed): the phantom mass
      // the Phase B flip will reclaim, measured before any behavior change.
      allocationDriftVcpu = ledgerPendingVcpu;
      // Reconcile the grant book against this cycle's existing single
      // inventory read (no new reads): delete when observed >= granted, the
      // service is gone, or the grant expired. Expiry is the typed
      // GrantExpired signal — ECS never delivered — that the allocations
      // ratchet silently absorbed.
      const observedVcpuByArn = new Map(
        awsRead.services.map((service) => [
          service.serviceArn,
          Math.max(
            service.desiredCount,
            service.runningCount + service.pendingCount,
          ) *
            (service.cpuUnits / 1024),
        ]),
      );
      const carriedGrants: Record<string, CapacityGrant> = {};
      for (const [serviceArn, grant] of Object.entries(
        control.ledger.grants ?? {},
      )) {
        const observedVcpu = observedVcpuByArn.get(serviceArn);
        if (observedVcpu === undefined) continue;
        if (observedVcpu >= grant.vcpu) continue;
        if (grant.expiresAt < now) {
          grantsExpired += 1;
          continue;
        }
        carriedGrants[serviceArn] = grant;
        grantPendingVcpu += Math.max(0, grant.vcpu - observedVcpu);
      }
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
            keepOutServiceArns,
            now: Date.now(),
          });
          audit.scaleInWrites = scaleInResult.writes;
          drainDeadlineExpired = scaleInResult.writes.filter(
            (write) =>
              write.result === "CANCELLED" &&
              write.reason === "DRAIN_DEADLINE_EXPIRED",
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
        // Phase A grant book (retirement-v2 §3.1): carried-forward unexpired
        // grants plus a fresh TTL'd grant for every service this plan grants
        // above its observed desired count. A re-grant at the same or lower
        // vcpu keeps the original expiry — the TTL bounds how long ECS
        // non-delivery can charge admission, and refreshing it on every
        // cycle would recreate the ratchet with extra steps.
        const nextGrants: Record<string, CapacityGrant> = { ...carriedGrants };
        for (const grant of updates) {
          const serviceArn = grant.service.serviceArn;
          const grantedVcpu = grant.granted * (grant.service.cpuUnits / 1024);
          const existing = carriedGrants[serviceArn];
          nextGrants[serviceArn] = {
            vcpu: grantedVcpu,
            expiresAt:
              existing && existing.vcpu >= grantedVcpu
                ? existing.expiresAt
                : now + TEMPORAL_CAPACITY_GRANT_TTL_MS,
          };
        }
        // A generation race with an out-of-cycle admission write
        // (admitReservation / releaseReservation / a retirement-release
        // landing between this cycle's snapshot and the plan claim) is the
        // expected benign contention, not a Lambda error: resolve it as a
        // PARTIAL cycle and retry from fresh inputs next cycle — the same
        // isolation posture as the protected scale-in step (adversarial-gate
        // fix, 2026-07-18).
        let planClaim: number | undefined;
        try {
          planClaim = await this.dependencies.state.claimCapacityPlan({
            cycleId,
            authorityGeneration: control.authority.generation,
            ledger: control.ledger,
            additionalManagedVcpu,
            observedManagedVcpu: awsRead.snapshot.managedCommittedVcpu,
            pendingLedgerVcpu: ledgerPendingVcpu,
            allocations: allocationsForGrants(allocation.grants),
            grants: nextGrants,
            inventoryHash: awsRead.inventoryHash,
            now: Date.now(),
          });
        } catch (error) {
          if (!isRetriableDynamoContention(error)) throw error;
          audit.planDeferred =
            error instanceof Error ? error.message : String(error);
          console.error(
            "Capacity plan claim lost a generation race to an out-of-cycle write; cycle resolves PARTIAL and retries from fresh inputs",
            { cycleId, error },
          );
        }
        if (planClaim === undefined) {
          result = "PARTIAL";
        } else {
          const ledgerGeneration = planClaim;
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
            Math.max(
              plannedManagedVcpu,
              freshAws.snapshot.managedCommittedVcpu,
            ) +
            freshAws.snapshot.unmanagedCommittedVcpu +
            freshAws.snapshot.activeReservationVcpu;
          const quotaCeilingVcpu =
            freshAws.snapshot.quotaVcpu - freshAws.snapshot.hardReserveVcpu;
          // Environment-budget twin of the quota ceiling: env-scoped committed
          // (managed + reservations, no unmanaged) must stay inside the static
          // partition even if inputs moved since allocation.
          const freshEnvCommittedVcpu =
            Math.max(
              plannedManagedVcpu,
              freshAws.snapshot.managedCommittedVcpu,
            ) + freshAws.snapshot.activeReservationVcpu;
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
            openRetirementMarkerAgeSeconds: keepOut.oldestOpenMarkerAgeSeconds,
            expiredRetirementMarkers: keepOut.expiredMarkers,
            retirementMarkerLiveConflicts,
            allocationDriftVcpu,
            grantPendingVcpu,
            grantsExpired,
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

  // ─── Retirement v2 marker ops (design doc 2026-07-18 §2.5, §4.2) ───
  // Synchronous data-plane operations invoked by the iac retire verb through
  // the reserve/release invoke-retry protocol (Q1 ruling: the verb keeps the
  // burial sequencing; this Lambda is the dumb fenced writer). Each retries
  // generation drift up to 5 times like reserveDeployment, and every op is
  // idempotent under byte-identical resend.

  private assertRetirementScope(environment: CapacityEnvironment) {
    if (
      this.config.environmentScope !== undefined &&
      environment !== this.config.environmentScope
    ) {
      throw new Error(
        `Retirement environment ${environment} is not admissible on the ${this.config.environmentScope}-scoped controller`,
      );
    }
  }

  // T0 BEGIN. Returns begun=false with a typed reason when a FOREIGN, live
  // marker holds the build — the verb defers that build (overlap safety),
  // it is not an error. Re-beginning a build whose OPEN marker expired
  // carries the dead run's releasedLedgerGeneration stamps forward so a
  // resumed T3 stays idempotent (no double managedCommittedVcpu decrement);
  // terminal entries do NOT carry stamps — a BURIED build's redeployed
  // same-ARN successor owes its own releases.
  async beginRetirement(params: RetirementBeginInput) {
    const now = Date.now();
    this.assertRetirementScope(params.environment);
    await this.dependencies.state.initialize();
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const control = await this.dependencies.state.readControlSnapshot();
      if (control.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA") {
        throw new Error(
          "Retirement marker ops require controller writer authority",
        );
      }
      const existing = control.retiringBuilds.builds[params.buildId];
      if (
        existing &&
        existing.state === "OPEN" &&
        existing.intentId !== params.intentId &&
        existing.expiresAt >= now
      ) {
        return { begun: false as const, reason: "MARKER_HELD" as const };
      }
      const carriedStamps =
        existing && existing.state === "OPEN" ? existing.services : {};
      const pruneBuildIds = Object.entries(control.retiringBuilds.builds)
        .filter(([, entry]) => entry.state !== "OPEN")
        .map(([buildId]) => buildId);
      try {
        await this.dependencies.state.beginRetirement({
          authorityGeneration: control.authority.generation,
          buildId: params.buildId,
          entry: {
            intentId: params.intentId,
            deploymentName: params.deploymentName,
            environment: params.environment,
            services: Object.fromEntries(
              params.services.map((service) => [
                service.serviceArn,
                {
                  priorDesired: service.priorDesired,
                  ...(carriedStamps[service.serviceArn]
                    ?.releasedLedgerGeneration !== undefined
                    ? {
                        releasedLedgerGeneration:
                          carriedStamps[service.serviceArn]
                            ?.releasedLedgerGeneration,
                      }
                    : {}),
                },
              ]),
            ),
            expiresAt: params.expiresAt,
          },
          pruneBuildIds,
          now,
        });
        return { begun: true as const };
      } catch (error) {
        if (
          !this.dependencies.state.isConditionalFailure(error) ||
          attempt === 5
        ) {
          throw error;
        }
      }
    }
    throw new Error("Retirement begin retries exhausted");
  }

  // T3 RELEASE: deploy admission unfreezes here — seconds after the zero,
  // not after tasks die, not after a cron cycle. Idempotent by the
  // releasedLedgerGeneration stamp written in the same transaction.
  async releaseRetirement(params: RetirementReleaseInput) {
    const now = Date.now();
    await this.dependencies.state.initialize();
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const control = await this.dependencies.state.readControlSnapshot();
      if (control.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA") {
        throw new Error(
          "Retirement marker ops require controller writer authority",
        );
      }
      const entry = control.retiringBuilds.builds[params.buildId];
      if (!entry || entry.intentId !== params.intentId) {
        throw new Error(
          `Retirement marker for build ${params.buildId} is absent or owned by another run`,
        );
      }
      const service = entry.services[params.serviceArn];
      if (!service) {
        throw new Error(
          `Service ${params.serviceArn} is not part of the retirement marker for build ${params.buildId}`,
        );
      }
      if (service.releasedLedgerGeneration !== undefined) {
        return {
          released: false as const,
          alreadyReleased: true as const,
          ledgerGeneration: service.releasedLedgerGeneration,
        };
      }
      if (entry.state !== "OPEN") {
        throw new Error(
          `Retirement marker for build ${params.buildId} is ${entry.state}; release is only valid on OPEN`,
        );
      }
      // Pool identity (and so vCPU pricing) derives from the service name.
      // Unknown segments price at zero: the allocations clamp and grant
      // delete still land, and managedCommittedVcpu recomputes wholesale
      // from observation at the next plan write.
      const poolId = temporalPoolIdForServiceName(params.serviceArn);
      const taskVcpu = poolId ? TEMPORAL_STABLE_POOLS[poolId].cpu / 1024 : 0;
      try {
        const ledgerGeneration =
          await this.dependencies.state.releaseRetirementLedger({
            authorityGeneration: control.authority.generation,
            ledger: control.ledger,
            buildId: params.buildId,
            intentId: params.intentId,
            serviceArn: params.serviceArn,
            releasedVcpu: service.priorDesired * taskVcpu,
            now,
          });
        return { released: true as const, ledgerGeneration };
      } catch (error) {
        if (
          !this.dependencies.state.isConditionalFailure(error) ||
          attempt === 5
        ) {
          throw error;
        }
      }
    }
    throw new Error("Retirement release retries exhausted");
  }

  // Terminal edges (I5). Both are idempotent under resend: an entry already
  // in the requested terminal state under the same intentId succeeds, and an
  // entry pruned after termination reports done rather than failing the
  // verb's retry protocol.
  async abortRetirement(params: RetirementAbortInput) {
    const now = Date.now();
    await this.dependencies.state.initialize();
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const control = await this.dependencies.state.readControlSnapshot();
      if (control.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA") {
        throw new Error(
          "Retirement marker ops require controller writer authority",
        );
      }
      const entry = control.retiringBuilds.builds[params.buildId];
      if (!entry) return { aborted: true as const, reason: params.reason };
      if (entry.intentId !== params.intentId) {
        throw new Error(
          `Retirement marker for build ${params.buildId} is owned by another run`,
        );
      }
      if (entry.state === "ABORTED") {
        return { aborted: true as const, reason: params.reason };
      }
      if (entry.state === "BURIED") {
        throw new Error(
          `Retirement marker for build ${params.buildId} is BURIED; abort after close is a protocol error`,
        );
      }
      try {
        await this.dependencies.state.abortRetirement({
          authorityGeneration: control.authority.generation,
          buildId: params.buildId,
          intentId: params.intentId,
          reason: params.reason,
          now,
        });
        return { aborted: true as const, reason: params.reason };
      } catch (error) {
        if (
          !this.dependencies.state.isConditionalFailure(error) ||
          attempt === 5
        ) {
          throw error;
        }
      }
    }
    throw new Error("Retirement abort retries exhausted");
  }

  // T7 CLOSE: marker closure — the last obligation of the burial.
  async closeRetirement(params: RetirementCloseInput) {
    const now = Date.now();
    await this.dependencies.state.initialize();
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const control = await this.dependencies.state.readControlSnapshot();
      if (control.authority.writerKind !== "STEP_FUNCTIONS_LAMBDA") {
        throw new Error(
          "Retirement marker ops require controller writer authority",
        );
      }
      const entry = control.retiringBuilds.builds[params.buildId];
      if (!entry) return { closed: true as const };
      if (entry.intentId !== params.intentId) {
        throw new Error(
          `Retirement marker for build ${params.buildId} is owned by another run`,
        );
      }
      if (entry.state === "BURIED") return { closed: true as const };
      if (entry.state === "ABORTED") {
        throw new Error(
          `Retirement marker for build ${params.buildId} is ABORTED; close after abort is a protocol error`,
        );
      }
      const pruneBuildIds = Object.entries(control.retiringBuilds.builds)
        .filter(([, candidate]) => candidate.state !== "OPEN")
        .map(([buildId]) => buildId);
      try {
        await this.dependencies.state.closeRetirement({
          authorityGeneration: control.authority.generation,
          buildId: params.buildId,
          intentId: params.intentId,
          pruneBuildIds,
          now,
        });
        return { closed: true as const };
      } catch (error) {
        if (
          !this.dependencies.state.isConditionalFailure(error) ||
          attempt === 5
        ) {
          throw error;
        }
      }
    }
    throw new Error("Retirement close retries exhausted");
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
      // Marker-exempt like the reconcile read (retirement-v2 §4.3, I8): a
      // mid-burial build must not flip the load gate closed env-wide.
      temporalRead = await this.dependencies.temporal.read(awsRead.services, {
        keepOutBuilds: new Set(
          computeRetirementKeepOut(
            control.retiringBuilds.builds,
            Date.now(),
          ).openBuilds.keys(),
        ),
      });
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
