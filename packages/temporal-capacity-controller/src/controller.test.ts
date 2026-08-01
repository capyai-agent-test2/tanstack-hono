import {
  ConditionalCheckFailedException,
  TransactionCanceledException,
  TransactionConflictException,
} from "@aws-sdk/client-dynamodb";
import { describe, expect, it } from "vitest";

import {
  TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS,
  TEMPORAL_STABLE_POOL_IDS,
  TEMPORAL_STABLE_POOLS,
  type RetiringBuildEntry,
  type TemporalStablePoolId,
} from "@capy/shared/temporal/capacity";

import {
  CapacityController,
  computeRetirementKeepOut,
  type ControllerDependencies,
} from "./controller.js";
import type {
  CapacityGrant,
  CapacityLedger,
  CapacityReservation,
  ControllerConfig,
  ManagedTemporalTask,
  ManagedTemporalService,
  MaintenanceRedeploy,
  QueueCapacityObservation,
  ReconcilerState,
  TemporalDrainRecord,
  WorkerProcessObservation,
  WriterAuthority,
} from "./types.js";

const config: ControllerConfig = {
  accountId: "123456789012",
  region: "us-west-2",
  tableName: "control",
  drainTableName: "drains",
  controlPartitionKey: "CONTROL#123456789012#us-west-2",
  clusters: [],
  hardReserveVcpu: 10,
  prodGuaranteedEnvelopeVcpu: 20,
  serviceQuotaCode: "L-3032A538",
  workerHeartbeatFreshnessMs: 15_000,
  cycleLockMs: 8_000,
  cycleStaleAfterMs: 90_000,
  reservationSnapshotMaxAgeMs: 90_000,
  auditTtlSeconds: 86_400,
  chainRotationCycles: 4_000,
};

const service = (desiredCount: number): ManagedTemporalService => ({
  environment: "prod",
  deploymentName: "capy-temporal-worker-prod",
  buildId: "build-1",
  buildState: "CURRENT",
  poolId: "parent",
  clusterArn: "cluster",
  clusterName: "cluster",
  serviceArn: "arn:service/parent",
  serviceName: "parent",
  taskDefinitionArn: "task-definition",
  cpuUnits: 2048,
  desiredCount,
  runningCount: desiredCount,
  pendingCount: 0,
  deploymentInProgress: false,
});

const observation = (): QueueCapacityObservation => ({
  environment: "prod",
  deploymentName: "capy-temporal-worker-prod",
  buildId: "build-1",
  buildState: "CURRENT",
  poolId: "parent",
  taskQueue: "jam-run",
  taskType: "activity",
  backlogCount: 1,
  backlogAgeSeconds: 1,
  tasksAddRate: 0,
  tasksDispatchRate: 0,
  activeSlots: 0,
  availableSlots: 32,
  processedTasks: 0,
  observedAt: Date.now(),
  fresh: true,
  workerTelemetryFresh: true,
});

const authority = (
  writerKind: WriterAuthority["writerKind"],
): WriterAuthority => ({
  generation: 1,
  writerKind,
  transitionId: "test",
  effectiveAt: Date.now(),
  checksum: "test",
});

const ledger: CapacityLedger = {
  generation: 1,
  managedCommittedVcpu: 2,
  activeReservationVcpu: 0,
  allocations: {},
  inventoryHash: "inventory",
  updatedAt: Date.now(),
};

const dependencies = (params: {
  writerKind: WriterAuthority["writerKind"];
  desiredCount: number;
  fence?: boolean;
  observation?: QueueCapacityObservation;
  drains?: TemporalDrainRecord[];
  workerProcesses?: WorkerProcessObservation[];
  tasks?: ManagedTemporalTask[];
  reservation?: CapacityReservation;
  maintenance?: MaintenanceRedeploy;
  omittedPoolIds?: TemporalStablePoolId[];
  decreaseError?: Error;
  incompleteWorkerDeployments?: string[];
  staleReadCount?: number;
  serviceOverride?: Partial<ManagedTemporalService>;
  scaleIn?: ReconcilerState["scaleIn"];
  fenceError?: Error;
  claimDecreaseError?: Error;
  heartbeatError?: Error;
  claimCycleError?: Error;
  retiringBuilds?: Record<string, RetiringBuildEntry>;
  ledgerGrants?: Record<string, CapacityGrant>;
  ledgerAllocations?: Record<string, number>;
  extraServices?: ManagedTemporalService[];
  beginError?: Error;
  releaseError?: Error;
  planClaimError?: Error;
}) => {
  const updates: number[] = [];
  const protections: boolean[] = [];
  const results: ReconcilerState["lastResult"][] = [];
  const completedScaleIn: Array<ReconcilerState["scaleIn"]> = [];
  const drainIntents: TemporalDrainRecord[] = [];
  const cancelledDrains: Array<{ intentId: string; reason: string }> = [];
  const markerBegins: Array<{ buildId: string; intentId: string }> = [];
  const markerReleases: Array<{
    buildId: string;
    serviceArn: string;
    releasedVcpu: number;
  }> = [];
  const markerAborts: Array<{ buildId: string; reason: string }> = [];
  const markerCloses: Array<{ buildId: string }> = [];
  const capacityPlans: Array<{
    allocations: Record<string, number>;
    grants: Record<string, CapacityGrant>;
  }> = [];
  const temporalReadOptions: Array<
    { keepOutBuilds?: ReadonlySet<string> } | undefined
  > = [];
  let ledgerGeneration = ledger.generation;
  const maintenanceIds: string[] = [];
  const recoveredDrains: string[] = [];
  const maintenanceLaunchAttempts: string[] = [];
  const managedServiceUpdates: string[] = [];
  const cycleMetrics: Array<{
    drainDeadlineExpired: number;
    openRetirementMarkerAgeSeconds: number;
    expiredRetirementMarkers: number;
    retirementMarkerLiveConflicts: number;
    allocationDriftVcpu: number;
    grantPendingVcpu: number;
    grantsExpired: number;
  }> = [];
  const cycleResultMetrics: string[] = [];
  const currentService = {
    ...service(params.desiredCount),
    ...params.serviceOverride,
  };
  const services: ManagedTemporalService[] = [
    ...TEMPORAL_STABLE_POOL_IDS.filter(
      (poolId) => !params.omittedPoolIds?.includes(poolId),
    ).map((poolId) => {
      if (poolId === "parent") return currentService;
      const pool = TEMPORAL_STABLE_POOLS[poolId];
      return {
        ...currentService,
        poolId,
        serviceArn: `arn:service/${poolId}`,
        serviceName: poolId,
        cpuUnits: pool.cpu,
        desiredCount: pool.prodFloor,
        runningCount: pool.prodFloor,
      };
    }),
    ...(params.extraServices ?? []),
  ];
  const observations: QueueCapacityObservation[] = services.flatMap((current) =>
    Object.entries(TEMPORAL_STABLE_POOLS[current.poolId].queues).flatMap(
      ([taskQueue, slots]) =>
        (["activity", "workflow"] as const).map((taskType) => ({
          environment: current.environment,
          deploymentName: current.deploymentName,
          buildId: current.buildId,
          buildState: current.buildState,
          poolId: current.poolId,
          taskQueue: taskQueue as QueueCapacityObservation["taskQueue"],
          taskType,
          backlogCount:
            current.poolId === "parent" && taskType === "activity" ? 1 : 0,
          backlogAgeSeconds:
            current.poolId === "parent" && taskType === "activity" ? 1 : 0,
          tasksAddRate: 0,
          tasksDispatchRate: 0,
          activeSlots: 0,
          availableSlots:
            taskType === "activity"
              ? slots.activitySlots * current.desiredCount
              : slots.workflowSlots * current.desiredCount,
          processedTasks: 0,
          observedAt: Date.now(),
          fresh: true,
          workerTelemetryFresh: true,
        })),
    ),
  );
  const effectiveObservations = observations.map((item) =>
    params.observation &&
    item.poolId === "parent" &&
    item.taskQueue === "jam-run" &&
    item.taskType === "activity"
      ? params.observation
      : item,
  );
  const value: ControllerDependencies = {
    state: {
      async initialize() {},
      async readControlSnapshot() {
        return {
          authority: authority(params.writerKind),
          ledger: {
            ...ledger,
            generation: ledgerGeneration,
            allocations: params.ledgerAllocations ?? ledger.allocations,
            ...(params.ledgerGrants ? { grants: params.ledgerGrants } : {}),
          },
          reconciler: {
            capability: "PROTECTED_SCALE_IN" as const,
            scaleIn: params.scaleIn,
          },
          retiringBuilds: { builds: params.retiringBuilds ?? {} },
        };
      },
      async readReservationSnapshot() {
        return undefined;
      },
      async writeReservationSnapshot() {},
      async claimCycle() {
        if (params.claimCycleError) throw params.claimCycleError;
      },
      async claimCapacityPlan(input) {
        if (params.planClaimError) throw params.planClaimError;
        capacityPlans.push({
          allocations: input.allocations,
          grants: input.grants,
        });
        ledgerGeneration = 2;
        return 2;
      },
      async verifyWriteFence() {
        return params.fence ?? true;
      },
      async completeCycle(input) {
        results.push(input.result);
        completedScaleIn.push(input.scaleIn);
      },
      async expireReservations() {},
      async admitReservation() {
        return 2;
      },
      async readReservation() {
        return params.reservation;
      },
      async consumeReservation() {
        if (!params.reservation) throw new Error("not expected");
        return params.reservation;
      },
      async releaseReservation() {
        return 2;
      },
      isConditionalFailure(_error: unknown): _error is never {
        return false;
      },
      async listActiveDrains() {
        return params.drains ?? [];
      },
      async putDrainIntent(input) {
        drainIntents.push(input.drain);
      },
      async beginRetirement(input) {
        if (params.beginError) throw params.beginError;
        markerBegins.push({
          buildId: input.buildId,
          intentId: input.entry.intentId,
        });
      },
      async releaseRetirementLedger(input) {
        if (params.releaseError) throw params.releaseError;
        markerReleases.push({
          buildId: input.buildId,
          serviceArn: input.serviceArn,
          releasedVcpu: input.releasedVcpu,
        });
        ledgerGeneration = input.ledger.generation + 1;
        return ledgerGeneration;
      },
      async abortRetirement(input) {
        markerAborts.push({ buildId: input.buildId, reason: input.reason });
      },
      async closeRetirement(input) {
        markerCloses.push({ buildId: input.buildId });
      },
      async cancelDrain(input) {
        cancelledDrains.push({
          intentId: input.drain.intentId,
          reason: input.reason,
        });
      },
      async refreshReadyDrainFence(input) {
        if (params.fenceError) throw params.fenceError;
        return input.drain;
      },
      async claimProtectedDecrease() {
        if (params.claimDecreaseError) throw params.claimDecreaseError;
        return 3;
      },
      async markDrainVerifying() {},
      async completeDrain() {},
      async rollbackProtectedDecrease() {
        return 3;
      },
      async recoverApplyingDecrease(input) {
        recoveredDrains.push(input.drain.intentId);
        return 3;
      },
      async putMaintenanceRedeploy(input) {
        maintenanceIds.push(input.maintenance.maintenanceId);
      },
      async readMaintenanceRedeploy() {
        return params.maintenance;
      },
      async requestMaintenanceRerun() {},
      async markMaintenanceLaunchAttempt(input) {
        maintenanceLaunchAttempts.push(input.maintenance.maintenanceId);
      },
      async prepareMaintenanceRerun() {},
      async listActiveMaintenanceRedeploys() {
        return params.maintenance ? [params.maintenance] : [];
      },
      async updateMaintenanceRedeploy() {},
      async claimMaintenanceDrain() {},
    },
    aws: {
      async read() {
        return {
          services,
          tasks: params.tasks ?? [],
          snapshot: {
            quotaVcpu: 100,
            accountUsageVcpu: params.desiredCount * 2,
            managedCommittedVcpu: params.desiredCount * 2,
            unmanagedCommittedVcpu: 0,
            activeReservationVcpu: 0,
            hardReserveVcpu: 10,
            prodGuaranteedEnvelopeVcpu: 20,
            capturedAt: Date.now(),
          },
          inventoryHash: "inventory",
        };
      },
      async readForReservation(activeReservationVcpu: number) {
        return {
          services,
          snapshot: {
            quotaVcpu: 100,
            accountUsageVcpu: params.desiredCount * 2,
            managedCommittedVcpu: params.desiredCount * 2,
            unmanagedCommittedVcpu: 0,
            activeReservationVcpu,
            hardReserveVcpu: 10,
            prodGuaranteedEnvelopeVcpu: 20,
            capturedAt: Date.now(),
          },
        };
      },
      async updateDesiredCount(_service, desiredCount) {
        updates.push(desiredCount);
        return "request-id";
      },
      async decreaseDesiredCount(_service, desiredCount) {
        updates.push(desiredCount);
        if (params.decreaseError) throw params.decreaseError;
        return "request-id";
      },
      async updateTaskProtection(input) {
        protections.push(input.protectionEnabled);
        return {
          taskArn: input.taskArn,
          protectionEnabled: input.protectionEnabled,
        };
      },
      async readTaskTerminalState() {
        return {
          terminal: true,
          lastStatus: "STOPPED",
          desiredStatus: "STOPPED",
        };
      },
      async updateManagedService(input) {
        managedServiceUpdates.push(input.serviceArn);
        return { requestId: "request-id" };
      },
      async emitCycleMetrics(metrics) {
        cycleMetrics.push({
          drainDeadlineExpired: metrics.drainDeadlineExpired,
          openRetirementMarkerAgeSeconds:
            metrics.openRetirementMarkerAgeSeconds,
          expiredRetirementMarkers: metrics.expiredRetirementMarkers,
          retirementMarkerLiveConflicts: metrics.retirementMarkerLiveConflicts,
          allocationDriftVcpu: metrics.allocationDriftVcpu,
          grantPendingVcpu: metrics.grantPendingVcpu,
          grantsExpired: metrics.grantsExpired,
        });
      },
      async emitCycleResultMetric(metric) {
        cycleResultMetrics.push(metric.result);
      },
      async emitLoadGateMetrics() {},
      async emitQueueBacklogMetrics() {},
    },
    temporal: {
      async read(_services, options) {
        temporalReadOptions.push(options);
        return {
          observations: effectiveObservations,
          services,
          staleReadCount: params.staleReadCount ?? 0,
          workerProcesses: params.workerProcesses ?? [],
          incompleteWorkerDeployments: params.incompleteWorkerDeployments ?? [],
        };
      },
    },
    chain: {
      async heartbeat() {
        if (params.heartbeatError) throw params.heartbeatError;
      },
    },
  };
  return {
    value,
    updates,
    protections,
    results,
    completedScaleIn,
    drainIntents,
    cancelledDrains,
    markerBegins,
    markerReleases,
    markerAborts,
    markerCloses,
    capacityPlans,
    temporalReadOptions,
    maintenanceIds,
    maintenanceLaunchAttempts,
    managedServiceUpdates,
    recoveredDrains,
    cycleMetrics,
    cycleResultMetrics,
  };
};

const input = {
  operation: "reconcile" as const,
  stateMachineArn:
    "arn:aws:states:us-west-2:123456789012:stateMachine:controller",
  executionArn:
    "arn:aws:states:us-west-2:123456789012:execution:controller:capacity-1",
  chainGeneration: 1,
  cycleIndex: 10,
};

describe("increase-only controller state machine", () => {
  it("journals non-authoritative cycles without mutating ECS", async () => {
    const test = dependencies({
      writerKind: "APPLICATION_AUTO_SCALING",
      desiredCount: 1,
    });
    await new CapacityController(config, test.value).reconcile(input);
    expect(test.updates).toEqual([]);
    expect(test.results).toEqual(["NON_AUTHORITATIVE"]);
  });

  it("applies a fenced production floor increase", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
    });
    await new CapacityController(config, test.value).reconcile(input);
    expect(test.updates).toEqual([2]);
    expect(test.results).toEqual(["APPLIED"]);
  });

  it("never represents a desired-count decrease", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
    });
    const output = await new CapacityController(config, test.value).reconcile(
      input,
    );
    expect(test.updates).toEqual([]);
    expect(test.results).toEqual(["NOOP"]);
    expect(output).toMatchObject({
      cycleIndex: input.cycleIndex + 1,
      rotate: false,
      terminate: false,
    });
  });

  it("resolves a lost cycle-claim race as a quiet no-op instead of a Lambda crash", async () => {
    // Another invoke holds the RECONCILER lock (redeploy overlap, or a
    // timed-out cycle's lock running out RECONCILER_LOCK_MS). This raced as a
    // raw TransactionCanceledException crash for 10-15 min on every controller
    // deploy; it must resolve as "not my turn" with only a metric to show.
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      claimCycleError: new TransactionCanceledException({
        $metadata: {},
        message: "ConditionalCheckFailed",
        CancellationReasons: [
          { Code: "None" },
          { Code: "None" },
          { Code: "ConditionalCheckFailed" },
        ],
      }),
    });
    const output = await new CapacityController(config, test.value).reconcile(
      input,
    );
    expect(output).toMatchObject({
      cycleIndex: input.cycleIndex + 1,
      rotate: false,
      terminate: false,
    });
    // The contended cycle belongs to the lock holder: no ECS mutation, no
    // completeCycle journal entry, only the LOCK_CONTENDED result metric.
    expect(test.updates).toEqual([]);
    expect(test.results).toEqual([]);
    expect(test.cycleResultMetrics).toEqual(["LOCK_CONTENDED"]);
  });

  it("absorbs a first-instant TransactionConflict on the cycle claim as the same quiet no-op", async () => {
    // Two invokes racing the claim in the same instant surface as
    // TransactionConflict (a concurrent transaction on the RECONCILER item)
    // rather than a failed condition — equally "not my turn".
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      claimCycleError: new TransactionConflictException({
        $metadata: {},
        message: "Transaction is ongoing for the item",
      }),
    });
    const output = await new CapacityController(config, test.value).reconcile(
      input,
    );
    expect(output).toMatchObject({ rotate: false, terminate: false });
    expect(test.updates).toEqual([]);
    expect(test.results).toEqual([]);
    expect(test.cycleResultMetrics).toEqual(["LOCK_CONTENDED"]);
  });

  it("suppresses rotation on a lost cycle claim even at the rotation boundary", async () => {
    // A LOSING invocation at the rotation boundary must not start a
    // successor: the lock holder's own cycle output carries the rotation,
    // and a loser rotating would deliberately mint a duplicate execution.
    const boundaryInput = {
      ...input,
      cycleIndex: config.chainRotationCycles - 1,
    };
    const contended = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      claimCycleError: new TransactionCanceledException({
        $metadata: {},
        message: "ConditionalCheckFailed",
        CancellationReasons: [
          { Code: "None" },
          { Code: "None" },
          { Code: "ConditionalCheckFailed" },
        ],
      }),
    });
    const lostOutput = await new CapacityController(
      config,
      contended.value,
    ).reconcile(boundaryInput);
    expect(lostOutput).toMatchObject({
      cycleIndex: config.chainRotationCycles,
      rotate: false,
      terminate: false,
    });

    // Control: the same boundary on a cycle that WON its claim does rotate.
    const won = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
    });
    const wonOutput = await new CapacityController(config, won.value).reconcile(
      boundaryInput,
    );
    expect(wonOutput).toMatchObject({ rotate: true, terminate: false });
  });

  it("terminates a superseded execution when the fenced chain heartbeat is lost", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      heartbeatError: new ConditionalCheckFailedException({
        $metadata: {},
        message: "The conditional request failed",
      }),
    });
    const output = await new CapacityController(config, test.value).reconcile(
      input,
    );
    expect(output).toMatchObject({ rotate: false, terminate: true });
    expect(test.updates).toEqual([]);
    expect(test.results).toEqual([]);
    expect(test.cycleResultMetrics).toEqual(["SUPERSEDED"]);
  });

  it("still fails loudly when the cycle claim hits a genuine systemic error", async () => {
    const claimCycleError = new TransactionCanceledException({
      $metadata: {},
      message: "ValidationError",
      CancellationReasons: [{ Code: "ValidationError" }],
    });
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      claimCycleError,
    });
    await expect(
      new CapacityController(config, test.value).reconcile(input),
    ).rejects.toBe(claimCycleError);
    expect(test.cycleResultMetrics).toEqual([]);
  });

  it("still fails loudly when the chain heartbeat hits a genuine systemic error", async () => {
    const heartbeatError = new Error("DynamoDB is unavailable");
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      heartbeatError,
    });
    await expect(
      new CapacityController(config, test.value).reconcile(input),
    ).rejects.toBe(heartbeatError);
    expect(test.cycleResultMetrics).toEqual([]);
  });

  it("prunes scale-in state for retired services from the completed cycle", async () => {
    // Every worker deploy mints new suffix-hashed ECS services, so without
    // this prune the persisted scaleIn map grows by one entry per retired
    // service forever and eventually breaches DynamoDB's 400KB item cap.
    const now = Date.now();
    const retiredArn =
      "arn:aws:ecs:us-west-2:123456789012:service/cluster/retired-deploy";
    const drainingRetiredArn =
      "arn:aws:ecs:us-west-2:123456789012:service/cluster/draining-retired";
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      drains: [
        {
          kind: "SCALE_IN",
          taskArn: "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-9",
          clusterArn: "cluster",
          serviceArn: drainingRetiredArn,
          poolId: "parent",
          buildId: "build-0",
          intentId: "intent-draining-retired",
          cycleId: "prior-cycle",
          authorityGeneration: 1,
          ledgerGeneration: 1,
          priorDesiredCount: 2,
          targetDesiredCount: 1,
          state: "INTENT",
          createdAt: now - 60_000,
          deadline: now + 60_000,
        },
      ],
      scaleIn: {
        "arn:service/parent": { lastScaleInAt: now },
        [retiredArn]: {
          eligibleSince: now - 60_000,
          activeIntentId: "intent-retired",
        },
        [drainingRetiredArn]: { activeIntentId: "intent-draining-retired" },
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.completedScaleIn).toHaveLength(1);
    const completed = test.completedScaleIn[0] ?? {};
    // Live inventory entry and the entry an active drain still references
    // survive; only the entry for a service that no longer exists is dropped.
    expect(completed).toHaveProperty(["arn:service/parent"]);
    expect(completed).toHaveProperty([drainingRetiredArn]);
    expect(completed).not.toHaveProperty([retiredArn]);
  });

  it("aborts actuation when the authority or ledger fence changes", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      fence: false,
    });
    await new CapacityController(config, test.value).reconcile(input);
    expect(test.updates).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("counts a deadline-expired drain cancellation into the cycle metrics", async () => {
    // This failure class (drain intents dying quietly at their deadline) ran
    // silent for 19 hours on 2026-07-15; the emitted count is what pages.
    const now = Date.now();
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [
        {
          kind: "SCALE_IN",
          taskArn: "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1",
          clusterArn: "cluster",
          serviceArn: "arn:service/parent",
          poolId: "parent",
          buildId: "build-1",
          intentId: "intent-1",
          cycleId: "prior-cycle",
          authorityGeneration: 1,
          ledgerGeneration: 1,
          priorDesiredCount: 4,
          targetDesiredCount: 3,
          state: "INTENT",
          createdAt: now - 13 * 60 * 60_000,
          deadline: now - 60_000,
        },
      ],
    });
    await new CapacityController(config, test.value).reconcile(input);
    expect(test.cancelledDrains).toEqual([
      { intentId: "intent-1", reason: "DRAIN_DEADLINE_EXPIRED" },
    ]);
    expect(test.cycleMetrics).toEqual([
      expect.objectContaining({ drainDeadlineExpired: 1 }),
    ]);
  });

  it("decrements only after exact drain readiness and sibling protection", async () => {
    const now = Date.now();
    const taskArn = "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1";
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "intent-1",
      cycleId: "prior-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "READY",
      createdAt: now - 60_000,
      deadline: now + 60_000,
      readyAt: now,
      protectionExpiresAt: now + 10 * 60_000,
    };
    const tasks = Array.from({ length: 4 }, (_, index) => ({
      taskArn:
        index === 0
          ? taskArn
          : `arn:aws:ecs:us-west-2:123456789012:task/cluster/task-${index + 1}`,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      serviceName: "parent",
      poolId: "parent" as const,
      buildId: "build-1",
      lastStatus: "RUNNING",
      desiredStatus: "RUNNING",
      healthStatus: "HEALTHY",
      protectionEnabled: true,
      protectionExpirationDate: now + 10 * 60_000,
    }));
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [drain],
      tasks,
      observation: {
        ...observation(),
        backlogCount: 0,
        backlogAgeSeconds: 0,
      },
    });
    await new CapacityController(config, test.value).reconcile(input);
    expect(test.protections).toEqual([false]);
    expect(test.updates).toEqual([3]);
    expect(test.results).toEqual(["APPLIED"]);
  });

  const readyScaleInScenario = (now: number) => {
    const taskArn = "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1";
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "intent-1",
      cycleId: "prior-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "READY",
      createdAt: now - 60_000,
      deadline: now + 60_000,
      readyAt: now,
      protectionExpiresAt: now + 10 * 60_000,
    };
    const tasks = Array.from({ length: 4 }, (_, index) => ({
      taskArn:
        index === 0
          ? taskArn
          : `arn:aws:ecs:us-west-2:123456789012:task/cluster/task-${index + 1}`,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      serviceName: "parent",
      poolId: "parent" as const,
      buildId: "build-1",
      lastStatus: "RUNNING",
      desiredStatus: "RUNNING",
      healthStatus: "HEALTHY",
      protectionEnabled: true,
      protectionExpirationDate: now + 10 * 60_000,
    }));
    return { drain, tasks };
  };

  const cancellation = (...codes: string[]) =>
    new TransactionCanceledException({
      $metadata: {},
      message: codes.join(","),
      CancellationReasons: codes.map((Code) => ({ Code })),
    });

  const fenceScenario = (fenceError?: Error, claimDecreaseError?: Error) => {
    const now = Date.now();
    const { drain, tasks } = readyScaleInScenario(now);
    return dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [drain],
      tasks,
      observation: {
        ...observation(),
        backlogCount: 0,
        backlogAgeSeconds: 0,
      },
      fenceError,
      claimDecreaseError,
    });
  };

  it("tolerates a benign ledger-generation race on the ready-drain fence as PARTIAL and retries next cycle (regression: fence ConditionalCheckFailed no longer aborts the global cycle)", async () => {
    // A reserve/release bumps the ledger generation between the snapshot read
    // and the fence transaction: the CAPACITY_LEDGER condition fails.
    const test = fenceScenario(
      cancellation("None", "ConditionalCheckFailed", "None", "None"),
    );
    await expect(
      new CapacityController(config, test.value).reconcile(input),
    ).resolves.toBeDefined();
    expect(test.updates).toEqual([]);
    expect(test.protections).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("bails the ready-drain fence cleanly to PARTIAL when this cycle lost its lock (RECONCILER ConditionalCheckFailed)", async () => {
    const test = fenceScenario(
      cancellation("None", "None", "ConditionalCheckFailed", "None"),
    );
    await expect(
      new CapacityController(config, test.value).reconcile(input),
    ).resolves.toBeDefined();
    expect(test.updates).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("rethrows a genuine (non-CCF) DynamoDB failure from the ready-drain fence", async () => {
    const fenceError = cancellation("None", "ValidationError", "None", "None");
    const test = fenceScenario(fenceError);
    await expect(
      new CapacityController(config, test.value).reconcile(input),
    ).rejects.toBe(fenceError);
    expect(test.updates).toEqual([]);
  });

  it("isolates a single drain's transient contention so the global reconcile cycle still completes as PARTIAL", async () => {
    // A TransactionConflict is not a CCF, so the fence handler rethrows it; the
    // reconcile-level per-drain isolation converts it to PARTIAL and the global
    // cycle still completes (completeCycle journals a result) rather than
    // aborting with a Lambda Invoke Error for every environment.
    const test = fenceScenario(
      new TransactionConflictException({
        $metadata: {},
        message: "Transaction is ongoing for the item",
      }),
    );
    await expect(
      new CapacityController(config, test.value).reconcile(input),
    ).resolves.toBeDefined();
    expect(test.updates).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("fails the whole cycle loudly when the scale-in step throws a genuine systemic error", async () => {
    const claimDecreaseError = new Error("ResourceNotFound: drains table");
    const test = fenceScenario(undefined, claimDecreaseError);
    await expect(
      new CapacityController(config, test.value).reconcile(input),
    ).rejects.toThrow("ResourceNotFound: drains table");
  });

  it("cancels a legacy drained-service intent and leaves the build to the retire verb", async () => {
    // Transition safety: pre-existing SCALE_IN drains for DRAINED services
    // are no longer eligible (retirement-v2: the iac retire verb owns
    // DRAINED builds end-to-end), so the live lane cancels them as
    // state-changed and performs NO retirement work of its own.
    const now = Date.now();
    const waitingDrain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn: "arn:aws:ecs:us-west-2:123456789012:task/cluster/parent-task",
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "waiting-drained-parent",
      cycleId: "prior-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "INTENT",
      createdAt: now - 60_000,
      deadline: now + 12 * 60 * 60_000,
    };
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      serviceOverride: { buildState: "DRAINED" },
      drains: [waitingDrain],
      scaleIn: {
        "arn:service/parent": { activeIntentId: waitingDrain.intentId },
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.cancelledDrains).toEqual([
      {
        intentId: waitingDrain.intentId,
        reason: "DEMAND_OR_SERVICE_STATE_CHANGED",
      },
    ]);
    // The whole build is DRAINED: the controller performs no retirement of
    // its own — no marker writes, no zero writes — the retire verb owns the
    // burial (retirement-v2 §2).
    expect(test.markerBegins).toHaveLength(0);
    expect(test.markerReleases).toHaveLength(0);
    expect(test.drainIntents).toHaveLength(0);
    expect(test.updates).toEqual([]);
  });

  it("keeps current-build tasks behind worker-reported readiness", async () => {
    const now = Date.now();
    const taskArn =
      "arn:aws:ecs:us-west-2:123456789012:task/cluster/current-task";
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      observation: {
        ...observation(),
        backlogCount: 0,
        backlogAgeSeconds: 0,
        availableSlots: 128,
      },
      tasks: [
        {
          taskArn,
          clusterArn: "cluster",
          serviceArn: "arn:service/parent",
          serviceName: "parent",
          poolId: "parent",
          buildId: "build-1",
          lastStatus: "RUNNING",
          desiredStatus: "RUNNING",
          healthStatus: "HEALTHY",
          protectionEnabled: true,
          protectionExpirationDate: now + 10 * 60_000,
        },
      ],
      workerProcesses: [
        {
          environment: "prod",
          deploymentName: "capy-temporal-worker-prod",
          buildId: "build-1",
          poolId: "parent",
          taskArn,
          workerInstanceKeys: ["worker-current"],
          taskQueues: ["prod-jam-run#activity", "prod-jam-run#workflow"],
          activeActivitySlots: 1,
          activeWorkflowSlots: 1,
          heartbeatAt: now,
          fresh: true,
        },
      ],
      scaleIn: {
        "arn:service/parent": {
          eligibleSince:
            now - TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS * 1_000 - 60_000,
        },
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.drainIntents).toHaveLength(0);
    expect(test.markerBegins).toHaveLength(0);
    expect(test.updates).toEqual([]);
  });

  it("starts a protected maintenance redeploy only from a fully protected service", async () => {
    const now = Date.now();
    const tasks: ManagedTemporalTask[] = Array.from(
      { length: 2 },
      (_, index) => ({
        taskArn: `arn:aws:ecs:us-west-2:123456789012:task/cluster/task-${index}`,
        clusterArn: "cluster",
        serviceArn: "arn:service/parent",
        serviceName: "parent",
        poolId: "parent",
        buildId: "build-1",
        lastStatus: "RUNNING",
        desiredStatus: "RUNNING",
        healthStatus: "HEALTHY",
        protectionEnabled: true,
        protectionExpirationDate: now + 10 * 60_000,
      }),
    );
    const reservation: CapacityReservation = {
      reservationId: "reservation-1",
      ownerToken: "owner-1",
      environment: "prod",
      buildId: "build-1",
      pools: ["parent"],
      requestedVcpu: 4,
      state: "ACTIVE",
      expiresAt: now + 60_000,
      ledgerGeneration: 1,
      serviceArns: [],
      consumedVcpu: 0,
    };
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 2,
      tasks,
      reservation,
    });
    const result = await new CapacityController(
      config,
      test.value,
    ).redeployManagedService({
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      desiredCount: 2,
      reservationId: reservation.reservationId,
      reservationOwnerToken: reservation.ownerToken,
    });
    expect(result.requestId).toBe("request-id");
    expect(test.maintenanceIds).toEqual([result.maintenanceId]);
  });

  it("retries a persisted maintenance request after the launch ambiguity window", async () => {
    const now = Date.now();
    const tasks: ManagedTemporalTask[] = Array.from(
      { length: 2 },
      (_, index) => ({
        taskArn: `arn:aws:ecs:us-west-2:123456789012:task/cluster/task-${index}`,
        clusterArn: "cluster",
        serviceArn: "arn:service/parent",
        serviceName: "parent",
        poolId: "parent",
        buildId: "build-1",
        lastStatus: "RUNNING",
        desiredStatus: "RUNNING",
        healthStatus: "HEALTHY",
        protectionEnabled: true,
        protectionExpirationDate: now + 10 * 60_000,
      }),
    );
    const reservation: CapacityReservation = {
      reservationId: "reservation-retry",
      ownerToken: "owner-retry",
      environment: "prod",
      buildId: "build-1",
      pools: ["parent"],
      requestedVcpu: 4,
      state: "ACTIVE",
      expiresAt: now + 10 * 60_000,
      ledgerGeneration: 1,
      serviceArns: [],
      consumedVcpu: 0,
    };
    const maintenance: MaintenanceRedeploy = {
      maintenanceId: "maintenance-retry",
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      buildId: "build-1",
      poolId: "parent",
      oldTaskArns: tasks.map((task) => task.taskArn),
      desiredCount: 2,
      state: "REQUESTED",
      createdAt: now - 2 * 60_000,
      deadline: now + 60 * 60_000,
      reservationId: reservation.reservationId,
      reservationOwnerToken: reservation.ownerToken,
      launchAttemptedAt: now - 2 * 60_000,
    };
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 2,
      tasks,
      reservation,
      maintenance,
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.maintenanceLaunchAttempts).toEqual([maintenance.maintenanceId]);
    expect(test.managedServiceUpdates).toEqual([maintenance.serviceArn]);
    expect(test.results).toEqual(["APPLIED"]);
  });

  it("defers scale-out while recovering an applying scale-in drain", async () => {
    const now = Date.now();
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn: "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1",
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "intent-applying",
      cycleId: "prior-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "APPLYING",
      createdAt: now - 60_000,
      deadline: now + 60_000,
    };
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [drain],
      tasks: [
        {
          taskArn: drain.taskArn,
          clusterArn: "cluster",
          serviceArn: drain.serviceArn,
          serviceName: "parent",
          poolId: "parent",
          buildId: "build-1",
          lastStatus: "RUNNING",
          desiredStatus: "RUNNING",
          healthStatus: "HEALTHY",
          protectionEnabled: true,
          protectionExpirationDate: now + 10 * 60_000,
        },
      ],
      observation: {
        ...observation(),
        tasksAddRate: 100,
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([]);
    expect(test.recoveredDrains).toEqual([drain.intentId]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("recovers a drain whose pool service disappeared before completeness validation", async () => {
    const now = Date.now();
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn: "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-missing",
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "intent-missing-service",
      cycleId: "prior-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "APPLYING",
      createdAt: now - 60_000,
      deadline: now + 60_000,
    };
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [drain],
      omittedPoolIds: ["parent"],
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.recoveredDrains).toEqual([drain.intentId]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("keeps an applying drain fenced after an ambiguous desired-count response", async () => {
    const now = Date.now();
    const taskArn = "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1";
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "ambiguous-decrement",
      cycleId: "current-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "READY",
      createdAt: now - 60_000,
      deadline: now + 60_000,
      readyAt: now,
      protectionExpiresAt: now + 10 * 60_000,
    };
    const tasks = Array.from({ length: 4 }, (_, index) => ({
      taskArn:
        index === 0
          ? taskArn
          : `arn:aws:ecs:us-west-2:123456789012:task/cluster/task-${index + 1}`,
      clusterArn: "cluster",
      serviceArn: drain.serviceArn,
      serviceName: "parent",
      poolId: "parent" as const,
      buildId: "build-1",
      lastStatus: "RUNNING",
      desiredStatus: "RUNNING",
      healthStatus: "HEALTHY",
      protectionEnabled: true,
      protectionExpirationDate: now + 10 * 60_000,
    }));
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [drain],
      tasks,
      decreaseError: new Error("timeout after send"),
      observation: {
        ...observation(),
        backlogCount: 0,
        backlogAgeSeconds: 0,
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([3]);
    expect(test.protections).toEqual([false]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("does not release a ready drain with incomplete worker telemetry", async () => {
    const now = Date.now();
    const taskArn = "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1";
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "incomplete-telemetry",
      cycleId: "current-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "READY",
      createdAt: now - 60_000,
      deadline: now + 60_000,
      readyAt: now,
      protectionExpiresAt: now + 10 * 60_000,
    };
    const tasks = Array.from({ length: 4 }, (_, index) => ({
      taskArn:
        index === 0
          ? taskArn
          : `arn:aws:ecs:us-west-2:123456789012:task/cluster/task-${index + 1}`,
      clusterArn: "cluster",
      serviceArn: drain.serviceArn,
      serviceName: "parent",
      poolId: "parent" as const,
      buildId: "build-1",
      lastStatus: "RUNNING",
      desiredStatus: "RUNNING",
      healthStatus: "HEALTHY",
      protectionEnabled: true,
      protectionExpirationDate: now + 10 * 60_000,
    }));
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [drain],
      tasks,
      incompleteWorkerDeployments: ["capy-temporal-worker-prod"],
      observation: {
        ...observation(),
        backlogCount: 0,
        backlogAgeSeconds: 0,
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([]);
    expect(test.protections).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("does not create a scale-in intent with incomplete worker telemetry", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      incompleteWorkerDeployments: ["capy-temporal-worker-prod"],
      observation: {
        ...observation(),
        backlogCount: 0,
        backlogAgeSeconds: 0,
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([]);
    expect(test.protections).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("does not release a ready scale-in drain when any Temporal read is stale", async () => {
    const now = Date.now();
    const taskArn = "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1";
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "stale-temporal-read",
      cycleId: "current-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "READY",
      createdAt: now - 60_000,
      deadline: now + 60_000,
      readyAt: now,
      protectionExpiresAt: now + 10 * 60_000,
    };
    const tasks = Array.from({ length: 4 }, (_, index) => ({
      taskArn:
        index === 0
          ? taskArn
          : `arn:aws:ecs:us-west-2:123456789012:task/cluster/task-${index + 1}`,
      clusterArn: "cluster",
      serviceArn: drain.serviceArn,
      serviceName: "parent",
      poolId: "parent" as const,
      buildId: "build-1",
      lastStatus: "RUNNING",
      desiredStatus: "RUNNING",
      healthStatus: "HEALTHY",
      protectionEnabled: true,
      protectionExpirationDate: now + 10 * 60_000,
    }));
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [drain],
      tasks,
      staleReadCount: 1,
      observation: {
        ...observation(),
        backlogCount: 0,
        backlogAgeSeconds: 0,
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([]);
    expect(test.protections).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("retries task-protection removal for an applying maintenance drain", async () => {
    const now = Date.now();
    const drain: TemporalDrainRecord = {
      kind: "MAINTENANCE",
      taskArn: "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1",
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "maintenance-applying",
      cycleId: "prior-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 2,
      targetDesiredCount: 2,
      state: "APPLYING",
      createdAt: now - 60_000,
      deadline: now + 60_000,
    };
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 2,
      drains: [drain],
      tasks: [
        {
          taskArn: drain.taskArn,
          clusterArn: "cluster",
          serviceArn: drain.serviceArn,
          serviceName: "parent",
          poolId: "parent",
          buildId: "build-1",
          lastStatus: "RUNNING",
          desiredStatus: "RUNNING",
          healthStatus: "HEALTHY",
          protectionEnabled: true,
          protectionExpirationDate: now + 10 * 60_000,
        },
      ],
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.protections).toEqual([false]);
    expect(test.results).toEqual(["APPLIED"]);
  });

  it("fails closed when sibling task protection is near expiry", async () => {
    const now = Date.now();
    const taskArn = "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1";
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      poolId: "parent",
      buildId: "build-1",
      intentId: "intent-stale-protection",
      cycleId: "prior-cycle",
      authorityGeneration: 1,
      ledgerGeneration: 1,
      priorDesiredCount: 4,
      targetDesiredCount: 3,
      state: "READY",
      createdAt: now - 60_000,
      deadline: now + 60_000,
      readyAt: now,
      protectionExpiresAt: now + 10 * 60_000,
    };
    const tasks = Array.from({ length: 4 }, (_, index) => ({
      taskArn:
        index === 0
          ? taskArn
          : `arn:aws:ecs:us-west-2:123456789012:task/cluster/task-${index + 1}`,
      clusterArn: "cluster",
      serviceArn: "arn:service/parent",
      serviceName: "parent",
      poolId: "parent" as const,
      buildId: "build-1",
      lastStatus: "RUNNING",
      desiredStatus: "RUNNING",
      healthStatus: "HEALTHY",
      protectionEnabled: true,
      protectionExpirationDate: index === 1 ? now + 30_000 : now + 10 * 60_000,
    }));
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      drains: [drain],
      tasks,
      observation: {
        ...observation(),
        backlogCount: 0,
        backlogAgeSeconds: 0,
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([]);
    expect(test.protections).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });
});

describe("retirement v2 keep-out (design doc 2026-07-18 §4)", () => {
  const NOW_SLACK_MS = 5 * 60_000;

  const marker = (
    buildId: string,
    overrides?: Partial<RetiringBuildEntry>,
  ): Record<string, RetiringBuildEntry> => ({
    [buildId]: {
      intentId: `run-${buildId}`,
      deploymentName: "capy-temporal-worker-prod",
      environment: "prod",
      services: { [`arn:service/${buildId}-vm`]: { priorDesired: 2 } },
      state: "OPEN",
      createdAt: Date.now() - 60_000,
      expiresAt: Date.now() + 45 * 60_000,
      ...overrides,
    },
  });

  const retiringService = (
    buildId: string,
    overrides?: Partial<ManagedTemporalService>,
  ): ManagedTemporalService => ({
    ...service(0),
    buildState: "REGISTRATION",
    buildId,
    // vm rather than parent: the harness synthesizes a nonzero jam-run
    // backlog for parent-pool services, which would read as scale-out
    // demand unrelated to the keep-out under test.
    poolId: "vm",
    serviceArn: `arn:service/${buildId}-vm`,
    serviceName: `${buildId}-vm`,
    desiredCount: 0,
    runningCount: 0,
    ...overrides,
  });

  it("classifies markers exhaustively: open, expired, and terminal", () => {
    const now = Date.now();
    const keepOut = computeRetirementKeepOut(
      {
        "build-open": marker("build-open")["build-open"]!,
        "build-expired": marker("build-expired", {
          expiresAt: now - 1_000,
        })["build-expired"]!,
        "build-buried": marker("build-buried", { state: "BURIED" })[
          "build-buried"
        ]!,
        "build-aborted": marker("build-aborted", {
          state: "ABORTED",
          terminalReason: "TEMPORAL_NOT_DRAINED",
        })["build-aborted"]!,
      },
      now,
    );
    expect([...keepOut.openBuilds.keys()]).toEqual([
      "capy-temporal-worker-prod#build-open",
    ]);
    expect(keepOut.expiredMarkers).toBe(1);
    expect(keepOut.oldestOpenMarkerAgeSeconds).toBeGreaterThanOrEqual(59);
  });

  it("excludes a marked build from allocation: no cycle write ever targets a marked service", async () => {
    // The build's REGISTRATION service sits at desired 0 with floor 1 — the
    // exact shape the allocator would scale up. The OPEN marker keeps the
    // controller out entirely (I2): no desired-count write, no drain intent.
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      extraServices: [retiringService("build-retiring")],
      retiringBuilds: marker("build-retiring"),
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([]);
    expect(test.drainIntents).toHaveLength(0);
    expect(test.results).toEqual(["NOOP"]);
    // The keep-out set rode into the Temporal read (throw exemption, I8).
    expect([...(test.temporalReadOptions[0]?.keepOutBuilds ?? [])]).toEqual([
      "capy-temporal-worker-prod#build-retiring",
    ]);
  });

  it("never re-staffs a DRAINED build from the ratcheted allocation book, even after a marker lapse", async () => {
    // The gate-confirmed lapse-window hole (observed empirically 2026-07-18:
    // the allocator re-staffed zeroed drained builds). A DRAINED service at
    // desired 0 with a stale `allocations` entry and an EXPIRED marker must
    // not be granted back to its committed floor — the allocator's
    // committedDesired clamp is the DRAINED analog of scaleInEligible's
    // hard-false.
    const drained = retiringService("build-lapsed", {
      buildState: "DRAINED",
      desiredCount: 0,
      runningCount: 0,
    });
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      extraServices: [drained],
      ledgerAllocations: { [drained.serviceArn]: 8 },
      retiringBuilds: marker("build-lapsed", {
        expiresAt: Date.now() - 1_000,
      }),
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([]);
    expect(test.results).toEqual(["NOOP"]);
  });

  it("resolves a plan-claim generation race as PARTIAL instead of a Lambda error", async () => {
    // desiredCount 1 forces a scale-out plan; the claim loses its generation
    // fence to an out-of-cycle admission write. The cycle must complete
    // PARTIAL (retry next cycle), not crash the invoke (adversarial-gate
    // fix, 2026-07-18).
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      planClaimError: new TransactionCanceledException({
        $metadata: {},
        message: "ConditionalCheckFailed",
        CancellationReasons: [
          { Code: "None" },
          { Code: "ConditionalCheckFailed" },
        ],
      }),
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([]);
    expect(test.results).toEqual(["PARTIAL"]);
  });

  it("resumes ownership of a build whose marker expired (lapse, not stickiness)", async () => {
    // A crashed verb must not freeze a build forever (the F1 class): past
    // expiresAt the controller scales the floor back up and the lapse is
    // visible as ExpiredRetirementMarkers.
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      extraServices: [retiringService("build-lapsed")],
      retiringBuilds: marker("build-lapsed", {
        expiresAt: Date.now() - 1_000,
      }),
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([1]);
    expect(test.results).toEqual(["APPLIED"]);
    expect(test.cycleMetrics[0]?.expiredRetirementMarkers).toBe(1);
    expect(test.cycleMetrics[0]?.openRetirementMarkerAgeSeconds).toBe(0);
  });

  it("prunes a marked service's scaleIn entry", async () => {
    const target = retiringService("build-retiring");
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      extraServices: [target],
      retiringBuilds: marker("build-retiring"),
      scaleIn: {
        [target.serviceArn]: { lastScaleInAt: 123 },
        "arn:service/parent": { lastScaleInAt: 456 },
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.completedScaleIn[0]).not.toHaveProperty(target.serviceArn);
    expect(test.completedScaleIn[0]).toHaveProperty("arn:service/parent");
  });

  it("keeps the env cycle alive when a marked build is active with missing pool services", async () => {
    // Rollback-during-burial, the F10+F4 composite: the build regressed to
    // CURRENT while its services are half-deleted. Without the marker this
    // kills every cycle env-wide; with it the cycle completes, the keep-out
    // is overridden for the live build, and the conflict metric fires.
    const rolledBack = retiringService("build-rollback", {
      buildState: "CURRENT",
      desiredCount: 2,
      runningCount: 2,
    });
    const withMarker = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      extraServices: [rolledBack],
      retiringBuilds: marker("build-rollback"),
    });
    await new CapacityController(config, withMarker.value).reconcile(input);
    expect(withMarker.results).toEqual(["NOOP"]);
    expect(withMarker.cycleMetrics[0]?.retirementMarkerLiveConflicts).toBe(1);

    // Control: the same world without a marker still fails loud — the
    // exemption is scoped to marked builds, not a blanket softening.
    const withoutMarker = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      extraServices: [rolledBack],
    });
    await expect(
      new CapacityController(config, withoutMarker.value).reconcile(input),
    ).rejects.toThrow(/missing pool services/);
  });

  it("defers a maintenance record targeting a marked service", async () => {
    const target = retiringService("build-retiring", {
      buildState: "REGISTRATION",
      desiredCount: 1,
      runningCount: 1,
    });
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      extraServices: [target],
      retiringBuilds: marker("build-retiring"),
      maintenance: {
        maintenanceId: "maintenance-1",
        clusterArn: "cluster",
        serviceArn: target.serviceArn,
        buildId: target.buildId,
        poolId: "vm",
        oldTaskArns: [],
        desiredCount: 1,
        state: "REQUESTED",
        createdAt: Date.now(),
        deadline: Date.now() + NOW_SLACK_MS,
        reservationId: "reservation-1",
        reservationOwnerToken: "token-1",
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.maintenanceLaunchAttempts).toHaveLength(0);
    expect(test.managedServiceUpdates).toHaveLength(0);
    expect(test.results).toEqual(["PARTIAL"]);
  });
});

describe("ledger v2 phase A dual-book (design doc 2026-07-18 §3)", () => {
  it("measures allocation drift and grant charge from the same inventory read", async () => {
    // parent observed 4 vCPU (desired 4 x cpuUnits 1024 in the harness? no:
    // parent cpuUnits 2048 -> desired 4 x 2 = 8 vCPU) against a ratcheted
    // allocation of 100 -> drift 92. The vm grant (6 vCPU vs 4 observed)
    // carries 2 pending; the expired parent grant counts as GrantExpired.
    const now = Date.now();
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 4,
      ledgerAllocations: { "arn:service/parent": 100 },
      ledgerGrants: {
        "arn:service/parent": { vcpu: 100, expiresAt: now - 1_000 },
        "arn:service/vm": { vcpu: 6, expiresAt: now + 60_000 },
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.cycleMetrics[0]?.allocationDriftVcpu).toBe(92);
    expect(test.cycleMetrics[0]?.grantsExpired).toBe(1);
    expect(test.cycleMetrics[0]?.grantPendingVcpu).toBe(2);
  });

  it("writes a TTL'd grant alongside the allocation for every scale-up the plan grants", async () => {
    // desiredCount 1 -> parent floor increase to 2 (the classic APPLIED
    // path): the plan write must now carry the Phase A grant book with a
    // grant for the parent scale-up, in the same transaction as allocations.
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
    });

    await new CapacityController(config, test.value).reconcile(input);

    expect(test.updates).toEqual([2]);
    expect(test.capacityPlans).toHaveLength(1);
    const plan = test.capacityPlans[0]!;
    const parentGrant = plan.grants["arn:service/parent"];
    expect(parentGrant?.vcpu).toBe(
      2 * (TEMPORAL_STABLE_POOLS.parent.cpu / 1024),
    );
    expect(parentGrant?.expiresAt).toBeGreaterThan(Date.now());
    // Admission behavior is untouched (Phase A only): allocations are still
    // written exactly as before, grants ride along.
    expect(plan.allocations["arn:service/parent"]).toBe(
      2 * (TEMPORAL_STABLE_POOLS.parent.cpu / 1024),
    );
  });

  it("carries an unexpired undelivered grant forward without refreshing its TTL", async () => {
    const now = Date.now();
    const originalExpiry = now + 2 * 60_000;
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      // parent will re-grant 2 (>= observed desired 1); the prior grant at
      // the same vcpu must keep its original expiry — refreshing on every
      // cycle would recreate the ratchet with extra steps.
      ledgerGrants: {
        "arn:service/parent": { vcpu: 4, expiresAt: originalExpiry },
      },
    });

    await new CapacityController(config, test.value).reconcile(input);

    const plan = test.capacityPlans[0]!;
    expect(plan.grants["arn:service/parent"]?.expiresAt).toBe(originalExpiry);
  });
});

describe("retirement v2 marker ops (handler surface)", () => {
  const poolArn =
    "arn:aws:ecs:us-west-2:123456789012:service/cluster/capy-temporal-worker-prod-vm-service-aaaaaaaaaaaa";

  const openEntry = (overrides?: Partial<RetiringBuildEntry>) =>
    ({
      intentId: "run-alive",
      deploymentName: "capy-temporal-worker-prod",
      environment: "prod",
      services: { [poolArn]: { priorDesired: 3 } },
      state: "OPEN",
      createdAt: Date.now() - 60_000,
      expiresAt: Date.now() + 45 * 60_000,
      ...overrides,
    }) satisfies RetiringBuildEntry;

  it("refuses begin with a typed MARKER_HELD when a foreign live marker owns the build", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      retiringBuilds: { "build-1": openEntry() },
    });
    const controller = new CapacityController(config, test.value);

    const outcome = await controller.beginRetirement({
      operation: "retirement-begin",
      buildId: "build-1",
      intentId: "run-other",
      environment: "prod",
      deploymentName: "capy-temporal-worker-prod",
      services: [{ serviceArn: poolArn, priorDesired: 3 }],
      expiresAt: Date.now() + 45 * 60_000,
    });

    expect(outcome).toEqual({ begun: false, reason: "MARKER_HELD" });
    expect(test.markerBegins).toHaveLength(0);
  });

  it("re-begins over an expired marker, carrying the dead run's release stamps forward", async () => {
    // Crash matrix (I6): begin admits an expired OPEN entry, and the T3
    // idempotency stamps must survive the ownership change so a resumed
    // release never double-decrements the ledger.
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      retiringBuilds: {
        "build-1": openEntry({
          intentId: "run-dead",
          expiresAt: Date.now() - 1_000,
          services: {
            [poolArn]: { priorDesired: 3, releasedLedgerGeneration: 7 },
          },
        }),
      },
    });
    const controller = new CapacityController(config, test.value);

    const outcome = await controller.beginRetirement({
      operation: "retirement-begin",
      buildId: "build-1",
      intentId: "run-new",
      environment: "prod",
      deploymentName: "capy-temporal-worker-prod",
      services: [{ serviceArn: poolArn, priorDesired: 3 }],
      expiresAt: Date.now() + 45 * 60_000,
    });

    expect(outcome).toEqual({ begun: true });
    expect(test.markerBegins).toEqual([
      { buildId: "build-1", intentId: "run-new" },
    ]);
    // The stamp survives: the fake records only ids, so assert through the
    // release path — an already-stamped service reports alreadyReleased.
    const release = await controller.releaseRetirement({
      operation: "retirement-release",
      buildId: "build-1",
      intentId: "run-dead",
      serviceArn: poolArn,
    });
    expect(release).toMatchObject({ alreadyReleased: true });
  });

  it("prices a release from the pool config derived from the service name", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      retiringBuilds: { "build-1": openEntry() },
    });
    const controller = new CapacityController(config, test.value);

    const outcome = await controller.releaseRetirement({
      operation: "retirement-release",
      buildId: "build-1",
      intentId: "run-alive",
      serviceArn: poolArn,
    });

    expect(outcome).toMatchObject({ released: true });
    // vm pool: 2048 cpu units -> 2 vCPU x priorDesired 3.
    expect(test.markerReleases).toEqual([
      { buildId: "build-1", serviceArn: poolArn, releasedVcpu: 6 },
    ]);
  });

  it("reports an already-stamped release instead of re-running the ledger transaction", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      retiringBuilds: {
        "build-1": openEntry({
          services: {
            [poolArn]: { priorDesired: 3, releasedLedgerGeneration: 9 },
          },
        }),
      },
    });
    const controller = new CapacityController(config, test.value);

    const outcome = await controller.releaseRetirement({
      operation: "retirement-release",
      buildId: "build-1",
      intentId: "run-alive",
      serviceArn: poolArn,
    });

    expect(outcome).toEqual({
      released: false,
      alreadyReleased: true,
      ledgerGeneration: 9,
    });
    expect(test.markerReleases).toHaveLength(0);
  });

  it("treats abort and close as idempotent under resend", async () => {
    const aborted = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      retiringBuilds: {
        "build-1": openEntry({
          state: "ABORTED",
          terminalReason: "TEMPORAL_NOT_DRAINED",
        }),
      },
    });
    const abortedController = new CapacityController(config, aborted.value);
    await expect(
      abortedController.abortRetirement({
        operation: "retirement-abort",
        buildId: "build-1",
        intentId: "run-alive",
        reason: "TEMPORAL_NOT_DRAINED",
      }),
    ).resolves.toMatchObject({ aborted: true });
    expect(aborted.markerAborts).toHaveLength(0);

    const buried = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      retiringBuilds: { "build-1": openEntry({ state: "BURIED" }) },
    });
    const buriedController = new CapacityController(config, buried.value);
    await expect(
      buriedController.closeRetirement({
        operation: "retirement-close",
        buildId: "build-1",
        intentId: "run-alive",
      }),
    ).resolves.toEqual({ closed: true });
    expect(buried.markerCloses).toHaveLength(0);
    // A pruned (absent) entry also resolves as done.
    await expect(
      buriedController.closeRetirement({
        operation: "retirement-close",
        buildId: "build-gone",
        intentId: "run-alive",
      }),
    ).resolves.toEqual({ closed: true });
  });

  it("rejects cross-terminal transitions: abort after close and close after abort", async () => {
    const buried = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      retiringBuilds: { "build-1": openEntry({ state: "BURIED" }) },
    });
    await expect(
      new CapacityController(config, buried.value).abortRetirement({
        operation: "retirement-abort",
        buildId: "build-1",
        intentId: "run-alive",
        reason: "OPERATOR_REQUESTED",
      }),
    ).rejects.toThrow(/protocol error/);

    const aborted = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      retiringBuilds: {
        "build-1": openEntry({
          state: "ABORTED",
          terminalReason: "TEMPORAL_NOT_DRAINED",
        }),
      },
    });
    await expect(
      new CapacityController(config, aborted.value).closeRetirement({
        operation: "retirement-close",
        buildId: "build-1",
        intentId: "run-alive",
      }),
    ).rejects.toThrow(/protocol error/);
  });

  it("refuses every marker op before controller writer cutover", async () => {
    const test = dependencies({
      writerKind: "APPLICATION_AUTO_SCALING",
      desiredCount: 1,
    });
    const controller = new CapacityController(config, test.value);
    await expect(
      controller.beginRetirement({
        operation: "retirement-begin",
        buildId: "build-1",
        intentId: "run-1",
        environment: "prod",
        deploymentName: "capy-temporal-worker-prod",
        services: [],
        expiresAt: Date.now() + 45 * 60_000,
      }),
    ).rejects.toThrow(/writer authority/);
    await expect(
      controller.releaseRetirement({
        operation: "retirement-release",
        buildId: "build-1",
        intentId: "run-1",
        serviceArn: poolArn,
      }),
    ).rejects.toThrow(/writer authority/);
  });
});

describe("deploy reservation capacity source", () => {
  const cachedSnapshot = (capturedAt: number) => ({
    capturedAt,
    services: [] as ManagedTemporalService[],
    snapshot: {
      quotaVcpu: 100,
      accountUsageVcpu: 2,
      managedCommittedVcpu: 2,
      unmanagedCommittedVcpu: 0,
      activeReservationVcpu: 0,
      hardReserveVcpu: 10,
      prodGuaranteedEnvelopeVcpu: 20,
      capturedAt,
    },
  });

  const reserveParams = {
    reservationId: "deploy-prod-build-1-parent",
    ownerToken: "owner-token",
    environment: "prod" as const,
    buildId: "build-1",
    pools: ["parent"] as TemporalStablePoolId[],
    requestedVcpu: 2,
    expiresAt: Date.now() + 10 * 60_000,
  };

  const instrument = (test: ReturnType<typeof dependencies>) => {
    const calls = { fullReads: 0, liveReservationReads: 0 };
    test.value.aws.read = async (activeReservationVcpu: number) => {
      calls.fullReads += 1;
      return {
        services: [],
        tasks: [],
        snapshot: {
          ...cachedSnapshot(Date.now()).snapshot,
          activeReservationVcpu,
        },
        inventoryHash: "inventory",
      };
    };
    test.value.aws.readForReservation = async (
      activeReservationVcpu: number,
    ) => {
      calls.liveReservationReads += 1;
      return {
        services: [],
        snapshot: {
          ...cachedSnapshot(Date.now()).snapshot,
          activeReservationVcpu,
        },
      };
    };
    return calls;
  };

  it("admits from the persisted snapshot without a full account-wide scan", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
    });
    const calls = instrument(test);
    test.value.state.readReservationSnapshot = async () =>
      cachedSnapshot(Date.now());

    const result = await new CapacityController(
      config,
      test.value,
    ).reserveDeployment(reserveParams);

    expect(result.reservationId).toBe(reserveParams.reservationId);
    expect(calls.fullReads).toBe(0);
    expect(calls.liveReservationReads).toBe(0);
  });

  it("falls back to the optimized live read when the snapshot is stale", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
    });
    const calls = instrument(test);
    test.value.state.readReservationSnapshot = async () =>
      cachedSnapshot(Date.now() - config.reservationSnapshotMaxAgeMs - 1_000);

    await new CapacityController(config, test.value).reserveDeployment(
      reserveParams,
    );

    expect(calls.fullReads).toBe(0);
    expect(calls.liveReservationReads).toBe(1);
  });

  it("uses the robust full read when no snapshot exists yet (cold cache)", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
    });
    const calls = instrument(test);
    test.value.state.readReservationSnapshot = async () => undefined;

    await new CapacityController(config, test.value).reserveDeployment(
      reserveParams,
    );

    // No persisted unmanaged floor, so the optimized metric-only read could
    // under-count and over-admit; the cold path must take the full read.
    expect(calls.fullReads).toBe(1);
    expect(calls.liveReservationReads).toBe(0);
  });

  it("confirms release with a live read before refusing a fresh-but-stale snapshot", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      reservation: {
        reservationId: reserveParams.reservationId,
        ownerToken: reserveParams.ownerToken,
        environment: "prod",
        buildId: "build-1",
        pools: ["parent"],
        requestedVcpu: 2,
        state: "ACTIVE",
        expiresAt: Date.now() + 10 * 60_000,
        ledgerGeneration: 1,
        serviceArns: [],
        consumedVcpu: 0,
      },
    });
    const calls = instrument(test);
    // Cache is fresh-in-window but predates the new service stabilizing, so it
    // reports the reservation as unconsumed (running/pending == 0).
    test.value.state.readReservationSnapshot = async () => ({
      capturedAt: Date.now(),
      services: [service(0)],
      snapshot: cachedSnapshot(Date.now()).snapshot,
    });
    // A live read sees the service now running and consuming its reservation.
    test.value.aws.readForReservation = async (
      activeReservationVcpu: number,
    ) => {
      calls.liveReservationReads += 1;
      return {
        services: [service(1)],
        snapshot: {
          ...cachedSnapshot(Date.now()).snapshot,
          activeReservationVcpu,
        },
      };
    };

    const result = await new CapacityController(
      config,
      test.value,
    ).releaseDeployment({
      reservationId: reserveParams.reservationId,
      ownerToken: reserveParams.ownerToken,
    });

    expect(result.released).toBe(true);
    expect(calls.fullReads).toBe(0);
    expect(calls.liveReservationReads).toBe(1);
  });

  // Finding #46: a fresh pool's rollout reservation was never observed as
  // consumed even though its ECS service was healthy and running at floor.
  // The root cause was upstream of this check (the deploy script tagged the
  // new pool's service capacity-managed=false, so the AWS reader excluded it
  // from `services` entirely -- see .github/scripts/deploy-temporal-worker-
  // topology.sh and temporal-worker-topology.ts). This test pins that
  // releaseDeployment's own consumption accounting treats an optional pool
  // (v3) exactly like any other pool once its service *is* correctly
  // reported: reaching the reserved floor capacity consumes the reservation
  // and release succeeds, with no special-casing by `optional`.
  it("consumes a fresh optional pool's reservation once its service reaches the reserved floor capacity", async () => {
    const v3Pool = TEMPORAL_STABLE_POOLS.v3;
    const requestedVcpu = (v3Pool.devFloor * v3Pool.cpu) / 1024;
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      reservation: {
        reservationId: "deploy-dev-build-2-1-v3",
        ownerToken: "owner-token",
        environment: "dev",
        buildId: "build-2",
        pools: ["v3"],
        requestedVcpu,
        state: "ACTIVE",
        expiresAt: Date.now() + 10 * 60_000,
        ledgerGeneration: 1,
        serviceArns: [],
        consumedVcpu: 0,
      },
    });
    test.value.state.readReservationSnapshot = async () => ({
      capturedAt: Date.now(),
      services: [
        {
          ...service(v3Pool.devFloor),
          poolId: "v3",
          buildId: "build-2",
          environment: "dev",
          serviceArn: "arn:service/v3",
          serviceName: "v3",
          cpuUnits: v3Pool.cpu,
        },
      ],
      snapshot: cachedSnapshot(Date.now()).snapshot,
    });

    const result = await new CapacityController(
      config,
      test.value,
    ).releaseDeployment({
      reservationId: "deploy-dev-build-2-1-v3",
      ownerToken: "owner-token",
    });

    expect(result.released).toBe(true);
  });

  it("releases an unconsumed reservation instead of stranding promotion", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
      reservation: {
        reservationId: reserveParams.reservationId,
        ownerToken: reserveParams.ownerToken,
        environment: "prod",
        buildId: "build-1",
        pools: ["parent"],
        requestedVcpu: 2,
        state: "ACTIVE",
        expiresAt: Date.now() + 10 * 60_000,
        ledgerGeneration: 1,
        serviceArns: [],
        consumedVcpu: 0,
      },
    });
    test.value.state.readReservationSnapshot = async () => ({
      capturedAt: Date.now(),
      services: [service(0)],
      snapshot: cachedSnapshot(Date.now()).snapshot,
    });
    let liveReservationReads = 0;
    test.value.aws.readForReservation = async (
      activeReservationVcpu: number,
    ) => {
      liveReservationReads += 1;
      return {
        services: [service(0)],
        snapshot: {
          ...cachedSnapshot(Date.now()).snapshot,
          activeReservationVcpu,
        },
      };
    };

    const result = await new CapacityController(
      config,
      test.value,
    ).releaseDeployment({
      reservationId: reserveParams.reservationId,
      ownerToken: reserveParams.ownerToken,
    });

    expect(result).toMatchObject({ released: true, reason: "UNCONSUMED" });
    expect(liveReservationReads).toBe(1);
  });
});

describe("environment-scoped reservation admission", () => {
  const scopedConfig = (
    scope: "prod" | "dev" | "staging",
    budget: number,
  ): ControllerConfig => ({
    ...config,
    environmentScope: scope,
    environmentVcpuBudget: budget,
    prodGuaranteedEnvelopeVcpu: 0,
  });

  const freshSnapshot = () => ({
    capturedAt: Date.now(),
    services: [] as ManagedTemporalService[],
    snapshot: {
      quotaVcpu: 1_000,
      accountUsageVcpu: 2,
      managedCommittedVcpu: 2,
      unmanagedCommittedVcpu: 0,
      activeReservationVcpu: 0,
      hardReserveVcpu: 10,
      prodGuaranteedEnvelopeVcpu: 0,
      capturedAt: Date.now(),
    },
  });

  const reserveParams = (
    environment: "prod" | "dev" | "staging" | "preview",
    requestedVcpu = 2,
  ) => ({
    reservationId: `deploy-${environment}-build-1-parent`,
    ownerToken: "owner-token",
    environment,
    buildId: "build-1",
    pools: ["parent"] as TemporalStablePoolId[],
    requestedVcpu,
    expiresAt: Date.now() + 10 * 60_000,
  });

  const scopedController = (
    scope: "prod" | "dev" | "staging",
    budget: number,
  ) => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
    });
    test.value.state.readReservationSnapshot = async () => freshSnapshot();
    return new CapacityController(scopedConfig(scope, budget), test.value);
  };

  it("rejects a reservation for a foreign environment", async () => {
    await expect(
      scopedController("staging", 200).reserveDeployment(reserveParams("prod")),
    ).rejects.toThrow(/not admissible on the staging-scoped controller/);
  });

  it("admits preview reservations on non-prod scopes", async () => {
    const result = await scopedController("dev", 800).reserveDeployment(
      reserveParams("preview"),
    );
    expect(result.reservationId).toBe("deploy-preview-build-1-parent");
  });

  it("rejects preview reservations on the prod-scoped controller", async () => {
    await expect(
      scopedController("prod", 1_200).reserveDeployment(
        reserveParams("preview"),
      ),
    ).rejects.toThrow(/not admissible on the prod-scoped controller/);
  });

  it("clamps admission to the environment budget headroom", async () => {
    // budget 10 - committed 2 = 8 headroom while quota headroom is ~988.
    await expect(
      scopedController("dev", 10).reserveDeployment(reserveParams("dev", 9)),
    ).rejects.toThrow(/Insufficient Fargate capacity/);
    const admitted = await scopedController("dev", 10).reserveDeployment(
      reserveParams("dev", 8),
    );
    expect(admitted.reservationId).toBe("deploy-dev-build-1-parent");
  });

  it("keeps the unscoped controller's admission unchanged", async () => {
    const test = dependencies({
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      desiredCount: 1,
    });
    test.value.state.readReservationSnapshot = async () => freshSnapshot();
    const admitted = await new CapacityController(
      config,
      test.value,
    ).reserveDeployment(reserveParams("dev", 100));
    expect(admitted.reservationId).toBe("deploy-dev-build-1-parent");
  });
});
