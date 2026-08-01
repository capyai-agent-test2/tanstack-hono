import { GetParametersCommand, SSMClient } from "@aws-sdk/client-ssm";
import { Connection } from "@temporalio/client";
import proto from "@temporalio/proto";
import { createHash } from "node:crypto";

import {
  getTemporalPoolQueues,
  getTemporalPoolTaskQueuePrefix,
  TEMPORAL_STABLE_POOL_IDS,
  TEMPORAL_STABLE_POOLS,
} from "@capy/shared/temporal/capacity";
import {
  TEMPORAL_TASK_QUEUES,
  type TemporalTaskQueue,
} from "@capy/shared/temporal/task-queues";

import type {
  ControllerConfig,
  ControllerEnvironmentScope,
  ManagedTemporalService,
  QueueCapacityObservation,
  TemporalTaskType,
  WorkerBuildState,
  WorkerProcessObservation,
} from "./types.js";

type TemporalCredentials = {
  address: string;
  namespace: string;
  apiKey: string;
};

const connectionKey = (credentials: TemporalCredentials) => {
  const credentialHash = createHash("sha256")
    .update(credentials.apiKey)
    .digest("hex");
  return `${credentials.address}#${credentials.namespace}#${credentialHash}`;
};

// A warm Lambda can thaw with a cached HTTP/2 channel whose socket is dead;
// calls then queue on the reconnect until their absolute deadlines are already
// expired ("Deadline exceeded after 0.000s, waiting for metadata filters"),
// marking every queue stale for the cycle and denying prod scale-up grants.
const STALE_CHANNEL_GRPC_CODES = new Set([
  4, // DEADLINE_EXCEEDED
  14, // UNAVAILABLE
]);

export const isStaleChannelError = (error: unknown): boolean =>
  typeof error === "object" &&
  error !== null &&
  "code" in error &&
  STALE_CHANNEL_GRPC_CODES.has((error as { code: unknown }).code as number);

export const callWithFreshChannelRetry = async <Channel, Result>(params: {
  acquire: () => Promise<Channel>;
  refresh: (stale: Channel) => Promise<Channel>;
  call: (channel: Channel) => Promise<Result>;
}): Promise<Result> => {
  const channel = await params.acquire();
  try {
    return await params.call(channel);
  } catch (error) {
    if (!isStaleChannelError(error)) throw error;
    return params.call(await params.refresh(channel));
  }
};

const timestampMillis = (
  value:
    | {
        seconds?: number | string | { toString(): string } | null;
        nanos?: number | null;
      }
    | null
    | undefined,
) => {
  if (!value) return 0;
  return Number(value.seconds ?? 0) * 1_000 + Number(value.nanos ?? 0) / 1e6;
};

const durationSeconds = (
  value:
    | {
        seconds?: number | string | { toString(): string } | null;
        nanos?: number | null;
      }
    | null
    | undefined,
) => {
  if (!value) return 0;
  return Number(value.seconds ?? 0) + Number(value.nanos ?? 0) / 1e9;
};

const queueSuffix = (
  environment: ControllerEnvironmentScope,
  taskQueue: string,
): TemporalTaskQueue | undefined => {
  const prefix = `${environment}-`;
  if (!taskQueue.startsWith(prefix)) return undefined;
  const suffix = taskQueue.slice(prefix.length);
  for (const poolId of TEMPORAL_STABLE_POOL_IDS) {
    // Pools may carry their own task-queue prefix segment on top of the
    // environment prefix (the v3 pool's dev-v3-orchestration maps to the
    // orchestration queue).
    const poolPrefix = getTemporalPoolTaskQueuePrefix(poolId);
    if (!suffix.startsWith(poolPrefix)) continue;
    const candidate = suffix.slice(poolPrefix.length);
    if (candidate in TEMPORAL_STABLE_POOLS[poolId].queues) {
      return candidate as TemporalTaskQueue;
    }
  }
  return undefined;
};

const poolForQueue = (taskQueue: TemporalTaskQueue) => {
  for (const poolId of TEMPORAL_STABLE_POOL_IDS) {
    if (taskQueue in TEMPORAL_STABLE_POOLS[poolId].queues) return poolId;
  }
  throw new Error(`No pool owns Temporal Task Queue ${taskQueue}`);
};

const taskType = (value: number): TemporalTaskType | undefined => {
  const type = proto.temporal.api.enums.v1.TaskQueueType;
  if (value === type.TASK_QUEUE_TYPE_ACTIVITY) return "activity";
  if (value === type.TASK_QUEUE_TYPE_WORKFLOW) return "workflow";
  return undefined;
};

const mapWithConcurrency = async <Value, Result>(
  values: Value[],
  concurrency: number,
  operation: (value: Value) => Promise<Result>,
) => {
  const results: Result[] = [];
  for (let index = 0; index < values.length; index += concurrency) {
    results.push(
      ...(await Promise.all(
        values.slice(index, index + concurrency).map(operation),
      )),
    );
  }
  return results;
};

const buildState = (status: number): WorkerBuildState => {
  const values = proto.temporal.api.enums.v1.WorkerDeploymentVersionStatus;
  if (status === values.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT)
    return "CURRENT";
  if (status === values.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING)
    return "RAMPING";
  if (status === values.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING)
    return "DRAINING";
  if (status === values.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED)
    return "DRAINED";
  return "REGISTRATION";
};

export class TemporalCapacityReader {
  private readonly ssm: SSMClient;
  private readonly connections = new Map<string, Promise<Connection>>();

  constructor(private readonly config: ControllerConfig) {
    this.ssm = new SSMClient({ region: config.region });
  }

  private async credentials(prefix: string): Promise<TemporalCredentials> {
    const names = [
      "TEMPORAL_ADDRESS",
      "TEMPORAL_NAMESPACE",
      "TEMPORAL_API_KEY",
    ];
    const response = await this.ssm.send(
      new GetParametersCommand({
        Names: names.map((name) => `${prefix}/${name}`),
        WithDecryption: true,
      }),
    );
    if ((response.InvalidParameters?.length ?? 0) > 0) {
      throw new Error(
        `Temporal controller parameters are missing: ${response.InvalidParameters?.join(",")}`,
      );
    }
    const values = Object.fromEntries(
      (response.Parameters ?? []).map((parameter) => [
        parameter.Name?.slice(prefix.length + 1),
        parameter.Value,
      ]),
    );
    const address = values.TEMPORAL_ADDRESS;
    const namespace = values.TEMPORAL_NAMESPACE;
    const apiKey = values.TEMPORAL_API_KEY;
    if (!address || !namespace || !apiKey) {
      throw new Error(`Temporal credentials under ${prefix} are incomplete`);
    }
    return { address, namespace, apiKey };
  }

  private connection(credentials: TemporalCredentials) {
    const key = connectionKey(credentials);
    const existing = this.connections.get(key);
    if (existing) return existing;
    const pending = Connection.connect({
      address: credentials.address,
      tls: true,
      apiKey: credentials.apiKey,
      metadata: { "temporal-namespace": credentials.namespace },
      connectTimeout: 2_000,
    }).catch((error: unknown) => {
      this.connections.delete(key);
      throw error;
    });
    this.connections.set(key, pending);
    return pending;
  }

  private async refreshConnection(
    credentials: TemporalCredentials,
    stale: Connection,
  ) {
    const key = connectionKey(credentials);
    const cached = await this.connections.get(key)?.catch(() => undefined);
    if (cached === stale) {
      this.connections.delete(key);
      void stale.close().catch(() => undefined);
    }
    return this.connection(credentials);
  }

  async readEnvironment(params: {
    environment: ControllerEnvironmentScope;
    deploymentName: string;
    parameterPrefix: string;
  }) {
    const credentials = await this.credentials(params.parameterPrefix);
    const temporalCall = <Result>(
      operation: (connection: Connection) => Promise<Result>,
    ) =>
      callWithFreshChannelRetry({
        acquire: () => this.connection(credentials),
        refresh: (stale) => this.refreshConnection(credentials, stale),
        call: (connection) =>
          connection.withDeadline(Date.now() + 3_000, () =>
            operation(connection),
          ),
      });
    const deployment = await temporalCall((connection) =>
      connection.workflowService.describeWorkerDeployment({
        namespace: credentials.namespace,
        deploymentName: params.deploymentName,
      }),
    );
    const summaries = deployment.workerDeploymentInfo?.versionSummaries ?? [];
    const states = new Map<string, WorkerBuildState>();
    for (const summary of summaries) {
      const buildId = summary.deploymentVersion?.buildId;
      if (buildId) states.set(buildId, buildState(summary.status ?? 0));
    }

    const observations: QueueCapacityObservation[] = [];
    const fixedLegacyBuildIds = new Set<string>();
    const unclassifiedBuildIds = new Set<string>();
    const versionEntries = [...states.entries()].filter(
      ([, state]) => state !== "DRAINED",
    );
    const versionReads = await Promise.allSettled(
      versionEntries.map(async ([buildId, state]) => {
        const version = await temporalCall((connection) =>
          connection.workflowService.describeWorkerDeploymentVersion({
            namespace: credentials.namespace,
            deploymentVersion: {
              deploymentName: params.deploymentName,
              buildId,
            },
            reportTaskQueueStats: true,
          }),
        );
        const registeredQueues = [
          ...new Set(
            (version.versionTaskQueues ?? []).flatMap((item) => {
              const prefix = `${params.environment}-`;
              return item.name?.startsWith(prefix)
                ? [item.name.slice(prefix.length)]
                : [];
            }),
          ),
        ].sort();
        const expectedQueues = [...TEMPORAL_TASK_QUEUES].sort();
        if (
          registeredQueues.length === expectedQueues.length &&
          registeredQueues.every(
            (queue, index) => queue === expectedQueues[index],
          )
        ) {
          fixedLegacyBuildIds.add(buildId);
        }
        for (const item of version.versionTaskQueues ?? []) {
          const queue = queueSuffix(params.environment, item.name ?? "");
          const type = taskType(item.type ?? 0);
          if (!queue || !type || !item.stats) continue;
          observations.push({
            environment: params.environment,
            deploymentName: params.deploymentName,
            buildId,
            buildState: state,
            poolId: poolForQueue(queue),
            taskQueue: queue,
            taskType: type,
            backlogCount: Number(item.stats.approximateBacklogCount ?? 0),
            backlogAgeSeconds: durationSeconds(
              item.stats.approximateBacklogAge,
            ),
            tasksAddRate: item.stats.tasksAddRate ?? 0,
            tasksDispatchRate: item.stats.tasksDispatchRate ?? 0,
            activeSlots: 0,
            availableSlots: 0,
            processedTasks: 0,
            observedAt: Date.now(),
            fresh: true,
            workerTelemetryFresh: false,
          });
        }
      }),
    );
    for (const [index, result] of versionReads.entries()) {
      if (result.status === "rejected") {
        const buildId = versionEntries[index]?.[0];
        if (buildId) unclassifiedBuildIds.add(buildId);
        console.error(
          "Temporal capacity version snapshot failed; affected queues will be marked stale",
          result.reason,
        );
      }
    }

    let nextPageToken: Uint8Array | undefined;
    let workerListingComplete = true;
    const workers: Array<{
      workerInstanceKey: string;
      workerIdentity: string;
      taskQueue: string;
      buildId: string;
      status: number;
    }> = [];
    try {
      do {
        const response = await temporalCall((connection) =>
          connection.workflowService.listWorkers({
            namespace: credentials.namespace,
            pageSize: 100,
            nextPageToken,
            query: `DeploymentName = "${params.deploymentName}"`,
          }),
        );
        for (const worker of response.workers ?? []) {
          const buildId = worker.deploymentVersion?.buildId;
          if (
            worker.workerInstanceKey &&
            worker.workerIdentity &&
            worker.taskQueue &&
            buildId
          ) {
            workers.push({
              workerInstanceKey: worker.workerInstanceKey,
              workerIdentity: worker.workerIdentity,
              taskQueue: worker.taskQueue,
              buildId,
              status: worker.status ?? 0,
            });
          }
        }
        nextPageToken =
          response.nextPageToken && response.nextPageToken.length > 0
            ? response.nextPageToken
            : undefined;
      } while (nextPageToken);
    } catch (error) {
      workerListingComplete = false;
      console.error(
        "Temporal worker listing failed; retaining queue stats and marking worker telemetry stale",
        error,
      );
    }

    const heartbeatDetails = await mapWithConcurrency(
      workers,
      100,
      async (worker) => {
        try {
          const response = await temporalCall((connection) =>
            connection.workflowService.describeWorker({
              namespace: credentials.namespace,
              workerInstanceKey: worker.workerInstanceKey,
            }),
          );
          return { worker, heartbeat: response.workerInfo?.workerHeartbeat };
        } catch (error) {
          console.error(
            "Temporal worker heartbeat lookup failed; worker will be treated as stale",
            { workerInstanceKey: worker.workerInstanceKey, error },
          );
          return { worker, heartbeat: undefined };
        }
      },
    );
    const heartbeatListingComplete = heartbeatDetails.every(
      ({ heartbeat }) => heartbeat !== undefined,
    );
    const now = Date.now();
    const workerProcesses = new Map<string, WorkerProcessObservation>();
    for (const { worker, heartbeat } of heartbeatDetails) {
      if (!heartbeat) continue;
      if (
        worker.status !==
        proto.temporal.api.enums.v1.WorkerStatus.WORKER_STATUS_RUNNING
      ) {
        continue;
      }
      const queue = queueSuffix(params.environment, worker.taskQueue);
      if (!queue) continue;
      const heartbeatAt = timestampMillis(heartbeat.heartbeatTime);
      const fresh = now - heartbeatAt <= this.config.workerHeartbeatFreshnessMs;
      const pairs: Array<
        [TemporalTaskType, typeof heartbeat.activityTaskSlotsInfo]
      > = [
        ["activity", heartbeat.activityTaskSlotsInfo],
        ["workflow", heartbeat.workflowTaskSlotsInfo],
      ];
      for (const [type, slots] of pairs) {
        if (!slots) continue;
        if (
          worker.workerIdentity.startsWith("arn:aws:ecs:") &&
          worker.status ===
            proto.temporal.api.enums.v1.WorkerStatus.WORKER_STATUS_RUNNING
        ) {
          const poolId = poolForQueue(queue);
          const key = `${worker.workerIdentity}#${worker.buildId}#${poolId}`;
          const process = workerProcesses.get(key) ?? {
            environment: params.environment,
            deploymentName: params.deploymentName,
            buildId: worker.buildId,
            poolId,
            taskArn: worker.workerIdentity,
            workerInstanceKeys: [],
            taskQueues: [],
            activeActivitySlots: 0,
            activeWorkflowSlots: 0,
            heartbeatAt,
            fresh,
          };
          process.workerInstanceKeys.push(worker.workerInstanceKey);
          process.taskQueues.push(`${worker.taskQueue}#${type}`);
          process.heartbeatAt = Math.min(process.heartbeatAt, heartbeatAt);
          process.fresh = process.fresh && fresh;
          if (type === "activity") {
            process.activeActivitySlots += slots.currentUsedSlots ?? 0;
          } else {
            process.activeWorkflowSlots += slots.currentUsedSlots ?? 0;
          }
          workerProcesses.set(key, process);
        }
        const observation = observations.find(
          (item) =>
            item.buildId === worker.buildId &&
            item.taskQueue === queue &&
            item.taskType === type,
        );
        if (!observation) continue;
        if (!fresh) continue;
        observation.activeSlots += slots.currentUsedSlots ?? 0;
        observation.availableSlots += slots.currentAvailableSlots ?? 0;
        observation.processedTasks += slots.lastIntervalProcessedTasks ?? 0;
        observation.processedIntervalSeconds = 5;
        observation.workerTelemetryFresh = true;
      }
    }
    for (const observation of observations) {
      if (observation.processedTasks > 0 && observation.activeSlots > 0) {
        observation.serviceTimeSeconds =
          (observation.activeSlots * 5) / observation.processedTasks;
      }
    }
    if (!workerListingComplete) {
      for (const observation of observations) {
        observation.workerTelemetryFresh = false;
      }
    }
    return {
      observations,
      states,
      fixedLegacyBuildIds,
      unclassifiedBuildIds,
      workerTelemetryComplete:
        workerListingComplete && heartbeatListingComplete,
      workerProcesses: [...workerProcesses.values()],
    };
  }

  async read(services: ManagedTemporalService[]) {
    const environmentReads = await Promise.allSettled(
      this.config.clusters.map((cluster) =>
        this.readEnvironment({
          environment: cluster.environment,
          deploymentName: cluster.deploymentName,
          parameterPrefix: cluster.temporalParameterPrefix,
        }),
      ),
    );
    const environments: Array<
      Awaited<ReturnType<TemporalCapacityReader["readEnvironment"]>> & {
        clusterIndex: number;
      }
    > = [];
    let failedEnvironmentReads = 0;
    const incompleteWorkerDeployments = new Set<string>();
    for (const [clusterIndex, result] of environmentReads.entries()) {
      if (result.status === "fulfilled") {
        environments.push({ clusterIndex, ...result.value });
        if (!result.value.workerTelemetryComplete) {
          const deploymentName =
            this.config.clusters[clusterIndex]?.deploymentName;
          if (deploymentName) incompleteWorkerDeployments.add(deploymentName);
        }
        continue;
      }
      failedEnvironmentReads += 1;
      const deploymentName = this.config.clusters[clusterIndex]?.deploymentName;
      if (deploymentName) incompleteWorkerDeployments.add(deploymentName);
      console.error(
        "Temporal capacity environment snapshot failed; affected services will be marked stale",
        {
          environment: this.config.clusters[clusterIndex]?.environment,
          error: result.reason,
        },
      );
    }
    const observations = environments.flatMap((item) => item.observations);
    const workerProcesses = environments.flatMap(
      (item) => item.workerProcesses,
    );
    const states = new Map<string, WorkerBuildState>();
    const fixedLegacyBuilds = new Set<string>();
    const unclassifiedBuilds = new Set<string>();
    for (const environment of environments) {
      const deploymentName =
        this.config.clusters[environment.clusterIndex]?.deploymentName;
      if (!deploymentName) continue;
      for (const buildId of environment.fixedLegacyBuildIds) {
        fixedLegacyBuilds.add(`${deploymentName}#${buildId}`);
      }
      for (const buildId of environment.unclassifiedBuildIds) {
        unclassifiedBuilds.add(`${deploymentName}#${buildId}`);
      }
      for (const [buildId, state] of environment.states) {
        states.set(`${deploymentName}#${buildId}`, state);
      }
    }
    const activeTemporalBuilds = [...states.entries()].filter(
      ([, state]) =>
        state === "CURRENT" || state === "RAMPING" || state === "DRAINING",
    );
    const unclassifiedActiveBuilds = activeTemporalBuilds.filter(([build]) =>
      unclassifiedBuilds.has(build),
    );
    for (const [deploymentBuild, state] of activeTemporalBuilds) {
      const [deploymentName, buildId] = deploymentBuild.split("#");
      if (
        !services.some(
          (service) =>
            service.deploymentName === deploymentName &&
            service.buildId === buildId,
        ) &&
        !fixedLegacyBuilds.has(deploymentBuild) &&
        !unclassifiedBuilds.has(deploymentBuild)
      ) {
        throw new Error(
          `Temporal ${state} role-pure build ${deploymentBuild} has no capacity-managed ECS services`,
        );
      }
    }
    const servicesWithState = services.map((service) => ({
      ...service,
      buildState:
        states.get(`${service.deploymentName}#${service.buildId}`) ??
        service.buildState,
    }));
    const observedAt = Date.now();
    for (const service of servicesWithState) {
      if (service.buildState === "DRAINED") continue;
      for (const taskQueue of getTemporalPoolQueues(service.poolId)) {
        for (const taskType of ["activity", "workflow"] as const) {
          const present = observations.some(
            (observation) =>
              observation.environment === service.environment &&
              observation.deploymentName === service.deploymentName &&
              observation.buildId === service.buildId &&
              observation.poolId === service.poolId &&
              observation.taskQueue === taskQueue &&
              observation.taskType === taskType,
          );
          if (present) continue;
          observations.push({
            environment: service.environment,
            deploymentName: service.deploymentName,
            buildId: service.buildId,
            buildState: service.buildState,
            poolId: service.poolId,
            taskQueue,
            taskType,
            backlogCount: 0,
            backlogAgeSeconds: 0,
            tasksAddRate: 0,
            tasksDispatchRate: 0,
            activeSlots: 0,
            availableSlots: 0,
            processedTasks: 0,
            observedAt,
            fresh: false,
            workerTelemetryFresh: false,
          });
        }
      }
    }
    return {
      observations,
      services: servicesWithState,
      workerProcesses,
      staleReadCount: failedEnvironmentReads + unclassifiedActiveBuilds.length,
      incompleteWorkerDeployments: [...incompleteWorkerDeployments],
    };
  }
}
