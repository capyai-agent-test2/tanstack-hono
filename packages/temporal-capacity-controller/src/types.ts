import type { TemporalStablePoolId } from "@capy/shared/temporal/capacity";
import type { TemporalDrainRecord } from "@capy/shared/temporal/capacity";
import type { TemporalTaskQueue } from "@capy/shared/temporal/task-queues";

export type CapacityEnvironment = "prod" | "dev" | "staging" | "preview";

// Environments a controller instance can be scoped to (preview workers are
// reservation-only and ride their host environment's controller, so "preview"
// is not a valid scope).
export type ControllerEnvironmentScope = "prod" | "dev" | "staging";
export type WorkerBuildState =
  | "REGISTRATION"
  | "CURRENT"
  | "RAMPING"
  | "DRAINING"
  | "DRAINED";
export type TemporalTaskType = "activity" | "workflow";

export type QueueCapacityObservation = {
  environment: CapacityEnvironment;
  deploymentName: string;
  buildId: string;
  buildState: WorkerBuildState;
  poolId: TemporalStablePoolId;
  taskQueue: TemporalTaskQueue;
  taskType: TemporalTaskType;
  backlogCount: number;
  backlogAgeSeconds: number;
  tasksAddRate: number;
  tasksDispatchRate: number;
  activeSlots: number;
  availableSlots: number;
  processedTasks: number;
  processedIntervalSeconds?: number;
  serviceTimeSeconds?: number;
  observedAt: number;
  fresh: boolean;
  workerTelemetryFresh: boolean;
};

export type ManagedTemporalService = {
  environment: CapacityEnvironment;
  deploymentName: string;
  buildId: string;
  buildState: WorkerBuildState;
  poolId: TemporalStablePoolId;
  clusterArn: string;
  clusterName: string;
  serviceArn: string;
  serviceName: string;
  taskDefinitionArn: string;
  cpuUnits: number;
  desiredCount: number;
  committedDesiredCount?: number;
  runningCount: number;
  pendingCount: number;
  deploymentInProgress: boolean;
};

export type ManagedTemporalTask = {
  taskArn: string;
  clusterArn: string;
  serviceArn: string;
  serviceName: string;
  poolId: TemporalStablePoolId;
  buildId: string;
  lastStatus: string;
  desiredStatus: string;
  healthStatus?: string;
  protectionEnabled: boolean;
  protectionExpirationDate?: number;
};

export type WorkerProcessObservation = {
  environment: CapacityEnvironment;
  deploymentName: string;
  buildId: string;
  poolId: TemporalStablePoolId;
  taskArn: string;
  workerInstanceKeys: string[];
  taskQueues: string[];
  activeActivitySlots: number;
  activeWorkflowSlots: number;
  heartbeatAt: number;
  fresh: boolean;
};

export type PoolDemand = {
  service: ManagedTemporalService;
  floor: number;
  rawWanted: number;
  boundedWanted: number;
  queueWants: Array<{
    taskQueue: TemporalTaskQueue;
    taskType: TemporalTaskType;
    wanted: number;
    requiredSlots: number;
    serviceTimeSeconds: number;
    emergencyMargin: number;
  }>;
  staleInput: boolean;
  hardMaxShortfall: number;
};

export type CapacitySnapshot = {
  quotaVcpu: number;
  accountUsageVcpu: number;
  managedCommittedVcpu: number;
  unmanagedCommittedVcpu: number;
  activeReservationVcpu: number;
  hardReserveVcpu: number;
  prodGuaranteedEnvelopeVcpu: number;
  // Static per-environment vCPU budget (env-split partitioning). Present only
  // when the controller runs environment-scoped; undefined preserves the
  // legacy shared-controller behavior everywhere it is consumed.
  environmentVcpuBudget?: number;
  capturedAt: number;
};

// Snapshot of the slow-moving Fargate capacity picture the reconcile loop
// persists each cycle so deploy-time reserve/release can decide admission
// without each running their own full account-wide ECS inventory scan. The
// reservation ledger (activeReservationVcpu) is always re-read transactionally
// at decision time; only the capacity inputs here come from this cache.
export type ReservationCapacityState = {
  snapshot: CapacitySnapshot;
  services: ManagedTemporalService[];
  capturedAt: number;
};

export type PoolGrant = {
  service: ManagedTemporalService;
  wanted: number;
  granted: number;
  priorDesired: number;
  additionalVcpu: number;
  ungranted: number;
  ungrantedReason?: "GLOBAL_CAPACITY" | "HARD_MAX" | "STALE_INPUT";
};

export type AllocationPlan = {
  grants: PoolGrant[];
  allocatableAdditionalVcpu: number;
  protectedProdHeadroomVcpu: number;
  ungrantedProdReplicas: number;
};

export type WriterAuthority = {
  generation: number;
  writerKind: "APPLICATION_AUTO_SCALING" | "STEP_FUNCTIONS_LAMBDA";
  transitionId: string;
  effectiveAt: number;
  checksum: string;
};

export type CapacityLedger = {
  generation: number;
  managedCommittedVcpu: number;
  activeReservationVcpu: number;
  allocations: Record<string, number>;
  inventoryHash: string;
  updatedAt: number;
};

export type ReconcilerState = {
  cycleId?: string;
  authorityGeneration?: number;
  capability: "PROTECTED_SCALE_IN";
  lockExpiresAt?: number;
  lastStartedAt?: number;
  lastCompletedAt?: number;
  lastInputHash?: string;
  lastResult?: "NON_AUTHORITATIVE" | "NOOP" | "APPLIED" | "PARTIAL" | "FAILED";
  lastUngrantedProdReplicas?: number;
  lastStaleTemporalInputs?: number;
  serviceTimes?: Record<
    string,
    {
      ewmaSeconds: number;
      sampleCount: number;
      activeSlots: number;
      updatedAt: number;
    }
  >;
  scaleIn?: Record<
    string,
    {
      eligibleSince?: number;
      lastScaleInAt?: number;
      activeIntentId?: string;
      disabledReason?: string;
      waveStartedAt?: number;
      waveStartDesiredCount?: number;
      waveDecrements?: number;
    }
  >;
};

export type ReservationState =
  | "REQUESTED"
  | "ACTIVE"
  | "CONSUMING"
  | "RELEASED"
  | "EXPIRED";

export type CapacityReservation = {
  reservationId: string;
  ownerToken: string;
  environment: CapacityEnvironment;
  buildId: string;
  pools: Array<TemporalStablePoolId | "preview-composite">;
  requestedVcpu: number;
  state: ReservationState;
  expiresAt: number;
  ledgerGeneration: number;
  serviceArns: string[];
  consumedVcpu: number;
};

export type MaintenanceRedeployState =
  | "REQUESTED"
  | "DEPLOYING"
  | "DRAINING"
  | "COMPLETE"
  | "FAILED";

export type MaintenanceRedeploy = {
  maintenanceId: string;
  clusterArn: string;
  serviceArn: string;
  buildId: string;
  poolId: TemporalStablePoolId;
  oldTaskArns: string[];
  desiredCount: number;
  state: MaintenanceRedeployState;
  createdAt: number;
  deadline: number;
  reservationId: string;
  reservationOwnerToken: string;
  taskDefinitionArn?: string;
  launchAttemptedAt?: number;
  rerunRequested?: boolean;
  deadlineExtensionCount?: number;
  terminalReason?: string;
};

export type ControllerConfig = {
  accountId: string;
  region: string;
  tableName: string;
  drainTableName: string;
  controlPartitionKey: string;
  clusters: Array<{
    environment: ControllerEnvironmentScope;
    clusterName: string;
    deploymentName: string;
    temporalParameterPrefix: string;
  }>;
  // Set when this controller instance manages exactly one environment (the
  // per-env split). Unset = legacy shared controller managing prod + dev.
  environmentScope?: ControllerEnvironmentScope;
  // Static vCPU budget for the scoped environment. Required with
  // environmentScope; both allocation and reservation admission clamp to it.
  environmentVcpuBudget?: number;
  hardReserveVcpu: number;
  prodGuaranteedEnvelopeVcpu: number;
  serviceQuotaCode: string;
  workerHeartbeatFreshnessMs: number;
  cycleLockMs: number;
  cycleStaleAfterMs: number;
  reservationSnapshotMaxAgeMs: number;
  auditTtlSeconds: number;
  chainRotationCycles: number;
};

export type { TemporalDrainRecord };

export type ReconcileInput = {
  operation: "reconcile";
  stateMachineArn: string;
  executionArn: string;
  chainGeneration: number;
  cycleIndex: number;
};

export type EnsureChainInput = {
  operation: "ensure-chain";
  stateMachineArn: string;
};

export type ReserveDeploymentInput = {
  operation: "reserve-deployment";
  reservationId: string;
  ownerToken: string;
  environment: CapacityEnvironment;
  buildId: string;
  pools: Array<TemporalStablePoolId | "preview-composite">;
  requestedVcpu: number;
  expiresAt: number;
};

export type ReleaseDeploymentInput = {
  operation: "release-deployment";
  reservationId: string;
  ownerToken: string;
};

export type UpdateManagedServiceInput = {
  operation: "update-managed-service";
  clusterArn: string;
  serviceArn: string;
  desiredCount?: number;
  taskDefinitionArn?: string;
  forceNewDeployment?: boolean;
  reservationId?: string;
  reservationOwnerToken?: string;
};

export type CheckLoadGateInput = {
  operation: "check-load-gate";
  stageJamsPerMinute: number;
  projectedActionsPerSecond: number;
  namespaceActionsLimitPerSecond: number;
  projectedRequestsPerSecond: number;
  namespaceRequestsLimitPerSecond: number;
  projectedOperationsPerSecond: number;
  namespaceOperationsLimitPerSecond: number;
};

export type RotateChainInput = {
  operation: "rotate-chain";
  stateMachineArn: string;
  chainGeneration: number;
};

export type RedeployManagedServiceInput = {
  operation: "redeploy-managed-service";
  clusterArn: string;
  serviceArn: string;
  desiredCount: number;
  taskDefinitionArn?: string;
  reservationId: string;
  reservationOwnerToken: string;
};

export type ControllerInput =
  | ReconcileInput
  | EnsureChainInput
  | ReserveDeploymentInput
  | ReleaseDeploymentInput
  | UpdateManagedServiceInput
  | RedeployManagedServiceInput
  | CheckLoadGateInput
  | RotateChainInput;

export type ReconcileOutput = {
  operation: "reconcile";
  stateMachineArn: string;
  chainGeneration: number;
  cycleIndex: number;
  rotate: boolean;
  // True when this execution observed it no longer owns the controller chain
  // (its fenced heartbeat lost to a successor). The Step Functions definition
  // branches on it to Succeed, so a superseded duplicate ends deterministically
  // instead of Recover-looping forever.
  terminate: boolean;
};
