import {
  TASK_QUEUE_BACKGROUND,
  TASK_QUEUE_JAM_RUN,
  TASK_QUEUE_JAM_RUN_SUPPORTING,
  TASK_QUEUE_LLM,
  TASK_QUEUE_LLM_ANTHROPIC,
  TASK_QUEUE_MACHINE,
  TASK_QUEUE_ORCHESTRATION,
  TASK_QUEUE_TOOLS,
  TASK_QUEUE_LLM_DEFAULT,
  TASK_QUEUE_LLM_GEMINI,
  TASK_QUEUE_LLM_OPENAI,
  TASK_QUEUE_MAINTENANCE,
  TASK_QUEUE_TOOLS_EXTERNAL,
  TASK_QUEUE_VM_OPS,
  TEMPORAL_TASK_QUEUES,
  type TemporalTaskQueue,
} from "./task-queues.js";

export const TEMPORAL_CAPACITY_MANIFEST_VERSION = 1;
export const TEMPORAL_WORKER_HEARTBEAT_INTERVAL_SECONDS = 5;
export const TEMPORAL_WORKER_HEARTBEAT_FRESHNESS_SECONDS = 15;
export const TEMPORAL_SCALE_IN_ELIGIBILITY_SECONDS = 15 * 60;
export const TEMPORAL_SCALE_IN_COOLDOWN_SECONDS = 5 * 60;
export const TEMPORAL_SCALE_IN_INTENT_TIMEOUT_SECONDS = 20 * 60;
export const TEMPORAL_SCALE_IN_VERIFY_TIMEOUT_SECONDS = 2 * 60;
export const TEMPORAL_SCALE_IN_MAX_WAVE_FRACTION = 0.25;
export const TEMPORAL_SCALE_IN_SLOT_UTILIZATION = 0.3;
// Batch retirement lane (DRAINED builds): after the lane zeroes a service's
// desiredCount, the running count must be observed at zero within this window
// or the RETIREMENT record flips to a typed FAILED — never a silent re-loop.
// DRAINED workers hold no work, so this needs no shutdown-grace coupling.
export const TEMPORAL_RETIREMENT_VERIFY_TIMEOUT_SECONDS = 10 * 60;
// Sanity floor for NEW retirement intents per reconcile cycle, counted in
// builds (the iac retire verb's RETIRE_MAX_BUILDS_PER_RUN doctrine: an absurd
// batch is evidence of a wrong worldview, never a big cleanup day; refuse and
// make a human look). Deliberately a named constant, not an env knob.
// BACKLOG-ERA CEILING (2026-07-18, supervised): the pre-R1 starvation left
// verified backlogs of 15 (prod) / 22 (staging) / 46 (dev) drained builds —
// counted by hand against Temporal drainage status before raising. Restore to
// 12 once all three cleanup.yml legs run green (tracked in the tightening
// list); a steady-state cycle should never see more than a handful.
export const TEMPORAL_RETIREMENT_MAX_BUILDS_PER_CYCLE = 64;

export const TEMPORAL_STABLE_POOL_IDS = [
  "parent",
  "v3",
  "vm",
  "supporting-events",
  "maintenance",
  "background-batch",
] as const;

export type TemporalStablePoolId = (typeof TEMPORAL_STABLE_POOL_IDS)[number];
export type TemporalWorkerRole = TemporalStablePoolId | "all";
export type TemporalDrainState =
  | "INTENT"
  | "READY"
  | "APPLYING"
  | "VERIFYING"
  | "CANCELLED"
  | "APPLIED"
  | "FAILED";

export type TemporalDrainRecord = {
  kind: "SCALE_IN" | "MAINTENANCE";
  taskArn: string;
  clusterArn: string;
  serviceArn: string;
  poolId: TemporalStablePoolId;
  buildId: string;
  intentId: string;
  cycleId: string;
  authorityGeneration: number;
  ledgerGeneration: number;
  priorDesiredCount: number;
  targetDesiredCount: number;
  state: TemporalDrainState;
  createdAt: number;
  deadline: number;
  protectionExpiresAt?: number;
  stoppedTaskQueues?: string[];
  readyAt?: number;
  verifyingAt?: number;
  appliedAt?: number;
  terminalReason?: string;
};

// Batch retirement of a DRAINED build's service: one record per SERVICE (not
// per task), disjoint from the SCALE_IN/MAINTENANCE per-task drain records —
// the sort key namespace (RETIREMENT#) keeps it invisible to the live
// protected-scale-in lane. ZEROING covers intent through actuation; APPLIED
// means desired=0 and running=0 were observed (or the service was deleted by
// the retire verb); FAILED carries a typed terminalReason.
export type TemporalRetirementState = "ZEROING" | "APPLIED" | "FAILED";

export type TemporalRetirementRecord = {
  kind: "RETIREMENT";
  serviceArn: string;
  clusterArn: string;
  poolId: TemporalStablePoolId;
  buildId: string;
  intentId: string;
  cycleId: string;
  authorityGeneration: number;
  ledgerGeneration: number;
  priorDesiredCount: number;
  state: TemporalRetirementState;
  createdAt: number;
  // Deadline for observing running=0 after the zero write; expiry flips the
  // record to FAILED with a typed reason.
  verifyDeadline: number;
  // Set by the ledger-release transaction; its absence on a ZEROING record
  // means the allocation release is still owed (crash/contention between the
  // ECS zero write and the ledger transaction) and must be resumed.
  releasedLedgerGeneration?: number;
  appliedAt?: number;
  terminalReason?: string;
};

export type TemporalQueueSlotVector = {
  activitySlots: number;
  workflowSlots: number;
};

export type TemporalQueueCapacityConfig = TemporalQueueSlotVector & {
  activityServiceTimeSeconds: {
    default: number;
    min: number;
    max: number;
  };
  workflowServiceTimeSeconds: {
    default: number;
    min: number;
    max: number;
  };
};

export type TemporalStablePoolConfig = {
  serviceNameSegment: string;
  architecture: "ARM64";
  cpu: number;
  memoryMiB: number;
  targetUtilization: number;
  drainSloSeconds: number;
  shutdownGraceSeconds: number;
  prodFloor: number;
  devFloor: number;
  /** Staging worker floor — HA-ish like dev, prod scales higher. */
  stagingFloor: number;
  planningTarget: number;
  hardMax: number;
  allocationWeight: number;
  emergencyBacklogAgeSeconds: number;
  /**
   * Task-queue name segment inserted between the environment prefix and each
   * queue this pool owns (e.g. "v3-" yields dev-v3-orchestration). The deploy
   * topology script, the worker queue config, and the capacity controller's
   * queue-name mapping all consult this; pools without it use plain
   * environment-prefixed names (dev-jam-run).
   */
  taskQueuePrefix?: string;
  /**
   * Optional pools may be absent from active builds that predate them. The
   * capacity controller's per-build missing-pool reconcile skips optional
   * pools instead of failing the cycle (e.g. prod builds deployed before the
   * v3 pool existed have no v3 service).
   */
  optional?: boolean;
  queues: Partial<Record<TemporalTaskQueue, TemporalQueueCapacityConfig>>;
};

export const TEMPORAL_STABLE_POOLS = {
  parent: {
    serviceNameSegment: "parent",
    architecture: "ARM64",
    cpu: 2048,
    // 4 GiB is sized for the 32/32 slot vector, not for an unbounded sticky
    // workflow cache: the build-8f1f5db workflow-thread OOM (finding #43)
    // came from the SDK's heap-derived cache default (thousands of cached jam
    // workflows), which worker-config.ts now caps for the parent pool at 50.
    // Revisit sizing only if the pool OOMs again with the cap in place.
    memoryMiB: 4096,
    targetUtilization: 0.8,
    drainSloSeconds: 60,
    shutdownGraceSeconds: 12 * 60 * 60 + 5 * 60,
    prodFloor: 2,
    devFloor: 1,
    stagingFloor: 1,
    hardMax: 68,
    queues: {
      [TASK_QUEUE_JAM_RUN]: {
        activitySlots: 32,
        workflowSlots: 32,
        activityServiceTimeSeconds: { default: 90.24, min: 1, max: 900 },
        workflowServiceTimeSeconds: { default: 0.1, min: 0.01, max: 10 },
      },
    },
    planningTarget: 54,
    allocationWeight: 1,
    emergencyBacklogAgeSeconds: 30,
  },
  // Dedicated pool for the v3 runtime queues (finding #45): these need the
  // full v3 worker runtime (WORKER_ROLE=v3), which the parent-role bootstrap
  // does not provide. Its queues carry the v3 task-queue prefix
  // (dev-v3-orchestration); builds that predate this pool have no v3 service,
  // so the capacity controller treats it as optional.
  v3: {
    serviceNameSegment: "v3",
    architecture: "ARM64",
    cpu: 4096,
    memoryMiB: 8192,
    targetUtilization: 0.8,
    drainSloSeconds: 60,
    shutdownGraceSeconds: 12 * 60 * 60 + 5 * 60,
    prodFloor: 2,
    devFloor: 1,
    stagingFloor: 1,
    hardMax: 68,
    taskQueuePrefix: "v3-",
    optional: true,
    queues: {
      [TASK_QUEUE_ORCHESTRATION]: {
        activitySlots: 32,
        workflowSlots: 32,
        activityServiceTimeSeconds: { default: 30, min: 0.05, max: 900 },
        workflowServiceTimeSeconds: { default: 0.1, min: 0.01, max: 10 },
      },
      [TASK_QUEUE_MACHINE]: {
        activitySlots: 32,
        workflowSlots: 32,
        activityServiceTimeSeconds: { default: 30, min: 0.05, max: 4500 },
        workflowServiceTimeSeconds: { default: 0.1, min: 0.01, max: 10 },
      },
      [TASK_QUEUE_LLM]: {
        activitySlots: 32,
        workflowSlots: 32,
        activityServiceTimeSeconds: { default: 20, min: 0.1, max: 900 },
        workflowServiceTimeSeconds: { default: 0.1, min: 0.01, max: 10 },
      },
      [TASK_QUEUE_TOOLS]: {
        activitySlots: 32,
        workflowSlots: 32,
        activityServiceTimeSeconds: { default: 15, min: 0.05, max: 900 },
        workflowServiceTimeSeconds: { default: 0.1, min: 0.01, max: 10 },
      },
    },
    planningTarget: 54,
    allocationWeight: 1,
    emergencyBacklogAgeSeconds: 30,
  },
  vm: {
    serviceNameSegment: "vm",
    architecture: "ARM64",
    cpu: 2048,
    memoryMiB: 4096,
    targetUtilization: 0.8,
    drainSloSeconds: 90,
    shutdownGraceSeconds: 35 * 60,
    prodFloor: 2,
    devFloor: 1,
    stagingFloor: 1,
    hardMax: 9,
    queues: {
      [TASK_QUEUE_VM_OPS]: {
        activitySlots: 64,
        workflowSlots: 32,
        // max covers long-running VM operations.
        activityServiceTimeSeconds: { default: 9.49, min: 0.05, max: 4500 },
        workflowServiceTimeSeconds: { default: 0.1, min: 0.01, max: 10 },
      },
    },
    planningTarget: 7,
    allocationWeight: 1,
    emergencyBacklogAgeSeconds: 45,
  },
  "supporting-events": {
    serviceNameSegment: "supporting",
    architecture: "ARM64",
    cpu: 1024,
    memoryMiB: 2048,
    targetUtilization: 0.8,
    drainSloSeconds: 30,
    shutdownGraceSeconds: 65 * 60,
    prodFloor: 2,
    devFloor: 1,
    stagingFloor: 1,
    hardMax: 3,
    queues: {
      [TASK_QUEUE_JAM_RUN_SUPPORTING]: {
        activitySlots: 64,
        workflowSlots: 32,
        activityServiceTimeSeconds: { default: 1.78, min: 0.01, max: 30 },
        workflowServiceTimeSeconds: { default: 0.05, min: 0.01, max: 10 },
      },
      [TASK_QUEUE_LLM_DEFAULT]: {
        activitySlots: 16,
        workflowSlots: 32,
        activityServiceTimeSeconds: { default: 0.9, min: 0.05, max: 60 },
        workflowServiceTimeSeconds: { default: 0.05, min: 0.01, max: 10 },
      },
    },
    planningTarget: 2,
    allocationWeight: 1,
    emergencyBacklogAgeSeconds: 15,
  },
  maintenance: {
    serviceNameSegment: "maintenance",
    architecture: "ARM64",
    // PR conflict scans run git clone/fetch/merge-tree subprocesses on this
    // pool; monorepo-scale scans OOM-killed 0.5 vCPU / 1 GiB tasks in prod.
    cpu: 2048,
    memoryMiB: 8192,
    targetUtilization: 0.7,
    drainSloSeconds: 300,
    shutdownGraceSeconds: 95 * 60,
    prodFloor: 2,
    devFloor: 1,
    stagingFloor: 1,
    hardMax: 8,
    queues: {
      [TASK_QUEUE_MAINTENANCE]: {
        activitySlots: 16,
        workflowSlots: 16,
        activityServiceTimeSeconds: { default: 1, min: 0.01, max: 3600 },
        workflowServiceTimeSeconds: { default: 0.1, min: 0.01, max: 30 },
      },
    },
    planningTarget: 1,
    allocationWeight: 0.75,
    emergencyBacklogAgeSeconds: 120,
  },
  "background-batch": {
    serviceNameSegment: "background",
    architecture: "ARM64",
    cpu: 512,
    memoryMiB: 1024,
    targetUtilization: 0.7,
    drainSloSeconds: 300,
    shutdownGraceSeconds: 12 * 60 * 60 + 5 * 60,
    prodFloor: 1,
    devFloor: 1,
    stagingFloor: 1,
    hardMax: 2,
    queues: {
      [TASK_QUEUE_BACKGROUND]: {
        activitySlots: 16,
        workflowSlots: 16,
        activityServiceTimeSeconds: { default: 1, min: 0.01, max: 300 },
        workflowServiceTimeSeconds: { default: 0.1, min: 0.01, max: 30 },
      },
    },
    planningTarget: 1,
    allocationWeight: 0.25,
    emergencyBacklogAgeSeconds: 180,
  },
} as const satisfies Record<TemporalStablePoolId, TemporalStablePoolConfig>;

export const TEMPORAL_DISABLED_QUEUES = [
  TASK_QUEUE_LLM_ANTHROPIC,
  TASK_QUEUE_LLM_OPENAI,
  TASK_QUEUE_LLM_GEMINI,
  TASK_QUEUE_TOOLS_EXTERNAL,
] as const satisfies readonly TemporalTaskQueue[];

const enabledQueueSlotVectors: Partial<
  Record<TemporalTaskQueue, TemporalQueueCapacityConfig>
> = {};
for (const poolId of TEMPORAL_STABLE_POOL_IDS) {
  for (const [queue, slots] of Object.entries(
    TEMPORAL_STABLE_POOLS[poolId].queues,
  )) {
    const taskQueue = queue as TemporalTaskQueue;
    if (enabledQueueSlotVectors[taskQueue]) {
      throw new Error(`Temporal Task Queue ${taskQueue} has multiple owners`);
    }
    enabledQueueSlotVectors[taskQueue] = slots;
  }
}

const expectedEnabledQueues = TEMPORAL_TASK_QUEUES.filter(
  (queue) =>
    !TEMPORAL_DISABLED_QUEUES.includes(
      queue as (typeof TEMPORAL_DISABLED_QUEUES)[number],
    ),
);
for (const queue of expectedEnabledQueues) {
  if (!enabledQueueSlotVectors[queue]) {
    throw new Error(`Temporal Task Queue ${queue} has no stable pool owner`);
  }
}
for (const queue of TEMPORAL_DISABLED_QUEUES) {
  if (enabledQueueSlotVectors[queue]) {
    throw new Error(`Disabled Temporal Task Queue ${queue} has a pool owner`);
  }
}

export const TEMPORAL_ENABLED_QUEUE_SLOT_VECTORS =
  enabledQueueSlotVectors as Readonly<
    Partial<Record<TemporalTaskQueue, TemporalQueueSlotVector>>
  >;

export function getTemporalPoolTaskQueuePrefix(
  poolId: TemporalStablePoolId,
): string {
  const pool: TemporalStablePoolConfig = TEMPORAL_STABLE_POOLS[poolId];
  return pool.taskQueuePrefix ?? "";
}

export function isTemporalPoolOptional(poolId: TemporalStablePoolId): boolean {
  const pool: TemporalStablePoolConfig = TEMPORAL_STABLE_POOLS[poolId];
  return pool.optional === true;
}

export function getTemporalPoolQueues(poolId: TemporalStablePoolId) {
  return Object.keys(
    TEMPORAL_STABLE_POOLS[poolId].queues,
  ) as TemporalTaskQueue[];
}

export function getTemporalQueueSlotVector(
  poolId: TemporalStablePoolId,
  queue: TemporalTaskQueue,
) {
  const queues = TEMPORAL_STABLE_POOLS[poolId].queues as Partial<
    Record<TemporalTaskQueue, TemporalQueueSlotVector>
  >;
  return queues[queue];
}

export function getTemporalDrainSortKey(taskArn: string) {
  const taskId = taskArn.split("/").at(-1);
  if (!taskId || !/^[a-zA-Z0-9_-]+$/.test(taskId)) {
    throw new Error(
      `Invalid ECS task ARN for Temporal drain identity: ${taskArn}`,
    );
  }
  return `DRAIN#${taskId}`;
}

export function getTemporalMaintenanceSortKey(serviceArn: string) {
  const serviceName = serviceArn.split("/").at(-1);
  if (!serviceName || !/^[a-zA-Z0-9_-]+$/.test(serviceName)) {
    throw new Error(
      `Invalid ECS service ARN for Temporal maintenance identity: ${serviceArn}`,
    );
  }
  return `MAINTENANCE#${serviceName}`;
}

// Typed identity guard (no-bare-throw doctrine, matching the shared
// package's SecretEncryptionError shape): a malformed service ARN here means
// the ECS inventory feeding the retirement lane is corrupt — a deterministic
// internal invariant, never an outcome callers branch on.
export class TemporalRetirementIdentityError extends Error {
  readonly serviceArn: string;

  constructor(serviceArn: string) {
    super(
      `Invalid ECS service ARN for Temporal retirement identity: ${serviceArn}`,
    );
    this.name = "TemporalRetirementIdentityError";
    this.serviceArn = serviceArn;
  }
}

export function getTemporalRetirementSortKey(serviceArn: string) {
  const serviceName = serviceArn.split("/").at(-1);
  if (!serviceName || !/^[a-zA-Z0-9_-]+$/.test(serviceName)) {
    throw new TemporalRetirementIdentityError(serviceArn);
  }
  return `RETIREMENT#${serviceName}`;
}
