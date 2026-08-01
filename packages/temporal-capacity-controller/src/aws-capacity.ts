import {
  CloudWatchClient,
  GetMetricDataCommand,
  PutMetricDataCommand,
  type MetricDatum,
} from "@aws-sdk/client-cloudwatch";
import {
  DescribeServicesCommand,
  DescribeTasksCommand,
  DescribeTaskDefinitionCommand,
  ECSClient,
  GetTaskProtectionCommand,
  ListClustersCommand,
  ListServicesCommand,
  ListTasksCommand,
  UpdateServiceCommand,
  UpdateTaskProtectionCommand,
  type Service,
} from "@aws-sdk/client-ecs";
import {
  GetServiceQuotaCommand,
  ServiceQuotasClient,
} from "@aws-sdk/client-service-quotas";

import {
  TEMPORAL_CAPACITY_MANIFEST_VERSION,
  TEMPORAL_STABLE_POOL_IDS,
  TEMPORAL_STABLE_POOLS,
  type TemporalStablePoolId,
} from "@capy/shared/temporal/capacity";

import { stableHash } from "./hash.js";
import { buildQueueBacklogMetricData } from "./queue-backlog.js";
import type {
  CapacityEnvironment,
  CapacitySnapshot,
  ControllerConfig,
  ControllerEnvironmentScope,
  ManagedTemporalService,
  ManagedTemporalTask,
  QueueCapacityObservation,
  WorkerBuildState,
} from "./types.js";

const chunks = <Value>(values: Value[], size: number) => {
  const result: Value[][] = [];
  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }
  return result;
};

const tagMap = (service: Service) =>
  Object.fromEntries(
    (service.tags ?? [])
      .filter((tag) => tag.key && tag.value)
      .map((tag) => [tag.key as string, tag.value as string]),
  );

const isPoolId = (value: string | undefined): value is TemporalStablePoolId =>
  Boolean(
    value && TEMPORAL_STABLE_POOL_IDS.includes(value as TemporalStablePoolId),
  );

const environmentForTag = (
  value: string | undefined,
): CapacityEnvironment | undefined => {
  if (
    value === "prod" ||
    value === "dev" ||
    value === "staging" ||
    value === "preview"
  ) {
    return value;
  }
  return undefined;
};

const FROZEN_LEGACY_SUBAGENT_POOL_MANIFEST_VERSION = "1";

export function isExactFrozenLegacySubagentPoolService(input: {
  serviceName: string | undefined;
  deploymentName: string;
  environment: ControllerEnvironmentScope;
  tags: Readonly<Record<string, string>>;
}): boolean {
  const buildId = input.tags["capy:worker-build-id"];
  return (
    input.tags["capy:temporal-worker"] === "true" &&
    input.tags["capy:capacity-managed"] === "true" &&
    input.tags["capy:worker-pool"] === "subagent" &&
    input.tags["capy:environment"] === input.environment &&
    input.tags["capy:capacity-manifest-version"] ===
      FROZEN_LEGACY_SUBAGENT_POOL_MANIFEST_VERSION &&
    buildId !== undefined &&
    buildId.length > 0 &&
    input.serviceName ===
      `${input.deploymentName}-subagent-pool-service-${buildId.slice(0, 12)}`
  );
}

export type AwsCapacityRead = {
  services: ManagedTemporalService[];
  tasks: ManagedTemporalTask[];
  snapshot: CapacitySnapshot;
  inventoryHash: string;
};

export class AwsCapacityReader {
  private readonly ecs: ECSClient;
  private readonly quotas: ServiceQuotasClient;
  private readonly cloudwatch: CloudWatchClient;
  private readonly taskCpu = new Map<string, number>();

  constructor(private readonly config: ControllerConfig) {
    // Reconcile cycles and deploy-time reserve/release calls each run a full
    // account-wide Fargate inventory; adaptive retry self-paces the client
    // instead of failing the cycle when ECS throttles concurrent scans.
    const retryConfig = { retryMode: "adaptive", maxAttempts: 10 };
    this.ecs = new ECSClient({ region: config.region, ...retryConfig });
    this.quotas = new ServiceQuotasClient({
      region: config.region,
      ...retryConfig,
    });
    this.cloudwatch = new CloudWatchClient({
      region: config.region,
      ...retryConfig,
    });
  }

  private async listServices(clusterName: string) {
    const arns: string[] = [];
    let nextToken: string | undefined;
    do {
      const response = await this.ecs.send(
        new ListServicesCommand({
          cluster: clusterName,
          nextToken,
          maxResults: 100,
        }),
      );
      arns.push(...(response.serviceArns ?? []));
      nextToken = response.nextToken;
    } while (nextToken);
    return arns;
  }

  private async describeServices(clusterName: string, arns: string[]) {
    const responses = await Promise.all(
      chunks(arns, 10).map((serviceArns) =>
        this.ecs.send(
          new DescribeServicesCommand({
            cluster: clusterName,
            services: serviceArns,
            include: ["TAGS"],
          }),
        ),
      ),
    );
    const failures = responses.flatMap((response) => response.failures ?? []);
    if (failures.length > 0) {
      throw new Error(
        `ECS DescribeServices returned failures: ${JSON.stringify(failures)}`,
      );
    }
    return responses.flatMap((response) => response.services ?? []);
  }

  private async taskDefinitionCpu(taskDefinitionArn: string) {
    const cached = this.taskCpu.get(taskDefinitionArn);
    if (cached) return cached;
    const response = await this.ecs.send(
      new DescribeTaskDefinitionCommand({ taskDefinition: taskDefinitionArn }),
    );
    const cpu = Number(response.taskDefinition?.cpu);
    if (!Number.isInteger(cpu) || cpu <= 0) {
      throw new Error(
        `Task definition ${taskDefinitionArn} has invalid CPU ${response.taskDefinition?.cpu}`,
      );
    }
    this.taskCpu.set(taskDefinitionArn, cpu);
    return cpu;
  }

  private async managedServices() {
    const managed: ManagedTemporalService[] = [];
    for (const cluster of this.config.clusters) {
      const arns = await this.listServices(cluster.clusterName);
      const services = await this.describeServices(cluster.clusterName, arns);
      for (const service of services) {
        const tags = tagMap(service);
        if (tags["capy:capacity-managed"] !== "true") continue;
        if (
          isExactFrozenLegacySubagentPoolService({
            serviceName: service.serviceName,
            deploymentName: cluster.deploymentName,
            environment: cluster.environment,
            tags,
          })
        ) {
          continue;
        }
        const poolId = tags["capy:worker-pool"];
        const environment = environmentForTag(tags["capy:environment"]);
        const serviceArn = service.serviceArn;
        const serviceName = service.serviceName;
        const taskDefinitionArn = service.taskDefinition;
        const buildId = tags["capy:worker-build-id"];
        if (poolId === "subagent") {
          throw new Error(
            `Capacity-managed ECS service ${serviceArn ?? serviceName ?? "unknown"} resembles the frozen legacy subagent pool but does not match its exact identity`,
          );
        }
        if (
          !isPoolId(poolId) ||
          !environment ||
          !serviceArn ||
          !serviceName ||
          !taskDefinitionArn ||
          !buildId
        ) {
          console.warn(
            JSON.stringify({
              event: "capacity.inventory.skipped_unidentified_service",
              serviceArn: serviceArn ?? serviceName ?? "unknown",
              poolTag: poolId ?? null,
              cluster: cluster.clusterName,
            }),
          );
          continue;
        }
        const expectedServiceName = `${cluster.deploymentName}-${TEMPORAL_STABLE_POOLS[poolId].serviceNameSegment}-service-${buildId.slice(0, 12)}`;
        if (
          serviceName !== expectedServiceName ||
          tags["capy:capacity-manifest-version"] !==
            String(TEMPORAL_CAPACITY_MANIFEST_VERSION)
        ) {
          console.warn(
            JSON.stringify({
              event: "capacity.inventory.skipped_manifest_mismatch",
              serviceName,
              poolId,
              buildId,
              manifestTag: tags["capy:capacity-manifest-version"] ?? null,
              expectedManifest: String(TEMPORAL_CAPACITY_MANIFEST_VERSION),
            }),
          );
          continue;
        }
        const taggedCpu = Number(tags["capy:task-vcpu"]) * 1024;
        const cpuUnits =
          Number.isInteger(taggedCpu) && taggedCpu > 0
            ? taggedCpu
            : await this.taskDefinitionCpu(taskDefinitionArn);
        managed.push({
          environment,
          deploymentName: cluster.deploymentName,
          buildId,
          buildState: "REGISTRATION" satisfies WorkerBuildState,
          poolId,
          clusterArn: service.clusterArn ?? cluster.clusterName,
          clusterName: cluster.clusterName,
          serviceArn,
          serviceName,
          taskDefinitionArn,
          cpuUnits,
          desiredCount: service.desiredCount ?? 0,
          runningCount: service.runningCount ?? 0,
          pendingCount: service.pendingCount ?? 0,
          deploymentInProgress:
            (service.deployments?.length ?? 0) > 1 ||
            service.deployments?.some(
              (deployment) =>
                deployment.status === "PRIMARY" &&
                deployment.rolloutState !== undefined &&
                deployment.rolloutState !== "COMPLETED",
            ) === true,
        });
      }
    }
    return managed;
  }

  private async managedTasks(services: ManagedTemporalService[]) {
    const byService = await Promise.all(
      services.map(async (service) => {
        const tasks: ManagedTemporalTask[] = [];
        const taskArns: string[] = [];
        let nextToken: string | undefined;
        do {
          const response = await this.ecs.send(
            new ListTasksCommand({
              cluster: service.clusterArn,
              serviceName: service.serviceName,
              desiredStatus: "RUNNING",
              nextToken,
              maxResults: 100,
            }),
          );
          taskArns.push(...(response.taskArns ?? []));
          nextToken = response.nextToken;
        } while (nextToken);
        if (taskArns.length === 0) return tasks;

        const protection = new Map<
          string,
          { protectionEnabled: boolean; expirationDate?: number }
        >();
        const unavailableProtection = new Set<string>();
        const protectionResponses = await Promise.all(
          chunks(taskArns, 10).map((taskChunk) =>
            this.ecs.send(
              new GetTaskProtectionCommand({
                cluster: service.clusterArn,
                tasks: taskChunk,
              }),
            ),
          ),
        );
        for (const response of protectionResponses) {
          const unexpectedFailures = (response.failures ?? []).filter(
            (failure) =>
              failure.reason !== "MISSING" && failure.reason !== "STOPPED",
          );
          if (unexpectedFailures.length > 0) {
            throw new Error(
              `ECS GetTaskProtection returned failures: ${JSON.stringify(unexpectedFailures)}`,
            );
          }
          for (const failure of response.failures ?? []) {
            if (failure.arn) unavailableProtection.add(failure.arn);
          }
          for (const item of response.protectedTasks ?? []) {
            if (!item.taskArn) continue;
            protection.set(item.taskArn, {
              protectionEnabled: item.protectionEnabled === true,
              ...(item.expirationDate
                ? { expirationDate: item.expirationDate.getTime() }
                : {}),
            });
          }
        }
        const availableTaskArns = taskArns.filter(
          (taskArn) => !unavailableProtection.has(taskArn),
        );
        const taskResponses = await Promise.all(
          chunks(availableTaskArns, 100).map((taskChunk) =>
            this.ecs.send(
              new DescribeTasksCommand({
                cluster: service.clusterArn,
                tasks: taskChunk,
              }),
            ),
          ),
        );
        for (const response of taskResponses) {
          const unexpectedFailures = (response.failures ?? []).filter(
            (failure) =>
              failure.reason !== "MISSING" && failure.reason !== "STOPPED",
          );
          if (unexpectedFailures.length > 0) {
            throw new Error(
              `ECS DescribeTasks returned failures: ${JSON.stringify(unexpectedFailures)}`,
            );
          }
          for (const task of response.tasks ?? []) {
            if (!task.taskArn) continue;
            if (unavailableProtection.has(task.taskArn)) continue;
            const taskProtection = protection.get(task.taskArn);
            tasks.push({
              taskArn: task.taskArn,
              clusterArn: service.clusterArn,
              serviceArn: service.serviceArn,
              serviceName: service.serviceName,
              poolId: service.poolId,
              buildId: service.buildId,
              lastStatus: task.lastStatus ?? "UNKNOWN",
              desiredStatus: task.desiredStatus ?? "UNKNOWN",
              ...(task.healthStatus ? { healthStatus: task.healthStatus } : {}),
              protectionEnabled: taskProtection?.protectionEnabled === true,
              ...(taskProtection?.expirationDate
                ? {
                    protectionExpirationDate: taskProtection.expirationDate,
                  }
                : {}),
            });
          }
        }
        return tasks;
      }),
    );
    return byService.flat();
  }

  private async quotaVcpu() {
    const response = await this.quotas.send(
      new GetServiceQuotaCommand({
        ServiceCode: "fargate",
        QuotaCode: this.config.serviceQuotaCode,
      }),
    );
    const value = response.Quota?.Value;
    if (!value || value <= 0) {
      throw new Error("Fargate On-Demand vCPU quota is unavailable");
    }
    return value;
  }

  private async accountUsageVcpu() {
    const endTime = new Date();
    const startTime = new Date(endTime.getTime() - 5 * 60_000);
    const response = await this.cloudwatch.send(
      new GetMetricDataCommand({
        StartTime: startTime,
        EndTime: endTime,
        ScanBy: "TimestampDescending",
        MetricDataQueries: [
          {
            Id: "fargate_on_demand_vcpu",
            ReturnData: true,
            MetricStat: {
              Period: 60,
              Stat: "Maximum",
              Metric: {
                Namespace: "AWS/Usage",
                MetricName: "ResourceCount",
                Dimensions: [
                  { Name: "Service", Value: "Fargate" },
                  { Name: "Type", Value: "Resource" },
                  { Name: "Resource", Value: "OnDemand" },
                  { Name: "Class", Value: "None" },
                ],
              },
            },
          },
        ],
      }),
    );
    const values = response.MetricDataResults?.[0]?.Values ?? [];
    return values.length > 0 ? Math.max(...values) : undefined;
  }

  private async directFargateInventoryVcpu() {
    const clusterArns: string[] = [];
    let clusterToken: string | undefined;
    do {
      const response = await this.ecs.send(
        new ListClustersCommand({ nextToken: clusterToken, maxResults: 100 }),
      );
      clusterArns.push(...(response.clusterArns ?? []));
      clusterToken = response.nextToken;
    } while (clusterToken);

    let totalVcpu = 0;
    for (const clusterArn of clusterArns) {
      const taskArns: string[] = [];
      for (const desiredStatus of ["RUNNING", "PENDING"] as const) {
        let taskToken: string | undefined;
        do {
          const response = await this.ecs.send(
            new ListTasksCommand({
              cluster: clusterArn,
              desiredStatus,
              launchType: "FARGATE",
              nextToken: taskToken,
              maxResults: 100,
            }),
          );
          taskArns.push(...(response.taskArns ?? []));
          taskToken = response.nextToken;
        } while (taskToken);
      }
      for (const taskChunk of chunks([...new Set(taskArns)], 100)) {
        const response = await this.ecs.send(
          new DescribeTasksCommand({
            cluster: clusterArn,
            tasks: taskChunk,
          }),
        );
        for (const task of response.tasks ?? []) {
          if (task.launchType !== "FARGATE" || !task.taskDefinitionArn)
            continue;
          totalVcpu +=
            (await this.taskDefinitionCpu(task.taskDefinitionArn)) / 1024;
        }
      }
    }
    return totalVcpu;
  }

  private managedVcpu(services: ManagedTemporalService[]) {
    const managedCommittedVcpu = services.reduce(
      (total, service) =>
        total +
        Math.max(
          service.desiredCount,
          service.runningCount + service.pendingCount,
        ) *
          (service.cpuUnits / 1024),
      0,
    );
    const managedActualVcpu = services.reduce(
      (total, service) =>
        total +
        (service.runningCount + service.pendingCount) *
          (service.cpuUnits / 1024),
      0,
    );
    return { managedCommittedVcpu, managedActualVcpu };
  }

  async read(activeReservationVcpu: number): Promise<AwsCapacityRead> {
    const capturedAt = Date.now();
    const [services, quotaVcpu, metricUsageVcpu, directUsageVcpu] =
      await Promise.all([
        this.managedServices(),
        this.quotaVcpu(),
        this.accountUsageVcpu(),
        this.directFargateInventoryVcpu(),
      ]);
    const tasks = await this.managedTasks(services);
    const accountUsageVcpu = Math.max(metricUsageVcpu ?? 0, directUsageVcpu);
    const { managedCommittedVcpu, managedActualVcpu } =
      this.managedVcpu(services);
    const unmanagedCommittedVcpu = Math.max(
      0,
      accountUsageVcpu - managedActualVcpu,
    );
    const snapshot: CapacitySnapshot = {
      quotaVcpu,
      accountUsageVcpu,
      managedCommittedVcpu,
      unmanagedCommittedVcpu,
      activeReservationVcpu,
      hardReserveVcpu: this.config.hardReserveVcpu,
      prodGuaranteedEnvelopeVcpu: this.config.prodGuaranteedEnvelopeVcpu,
      ...(this.config.environmentVcpuBudget !== undefined
        ? { environmentVcpuBudget: this.config.environmentVcpuBudget }
        : {}),
      capturedAt,
    };
    return {
      services,
      tasks,
      snapshot,
      inventoryHash: stableHash({
        services: services.map((service) => ({
          serviceArn: service.serviceArn,
          taskDefinitionArn: service.taskDefinitionArn,
          desiredCount: service.desiredCount,
          runningCount: service.runningCount,
          pendingCount: service.pendingCount,
        })),
        snapshot,
      }),
    };
  }

  // Optimized read for deploy-time reserve/release admission. These callers
  // only consume `services` and `snapshot` (never `tasks`/`inventoryHash`), so
  // we skip the two most expensive parts of `read()`: the per-task
  // GetTaskProtection/DescribeTasks fan-out (`managedTasks`) and the
  // full-account, task-by-task `directFargateInventoryVcpu` scan. Account
  // usage comes from the CloudWatch metric alone; callers may pass
  // `unmanagedFloorVcpu` (the last persisted unmanaged figure) so the metric's
  // lag cannot under-count committed capacity and over-admit a reservation.
  async readForReservation(
    activeReservationVcpu: number,
    options: { unmanagedFloorVcpu?: number } = {},
  ): Promise<{
    services: ManagedTemporalService[];
    snapshot: CapacitySnapshot;
  }> {
    const capturedAt = Date.now();
    const [services, quotaVcpu, metricUsageVcpu] = await Promise.all([
      this.managedServices(),
      this.quotaVcpu(),
      this.accountUsageVcpu(),
    ]);
    const accountUsageVcpu = metricUsageVcpu ?? 0;
    const { managedCommittedVcpu, managedActualVcpu } =
      this.managedVcpu(services);
    const unmanagedCommittedVcpu = Math.max(
      0,
      accountUsageVcpu - managedActualVcpu,
      options.unmanagedFloorVcpu ?? 0,
    );
    const snapshot: CapacitySnapshot = {
      quotaVcpu,
      accountUsageVcpu,
      managedCommittedVcpu,
      unmanagedCommittedVcpu,
      activeReservationVcpu,
      hardReserveVcpu: this.config.hardReserveVcpu,
      prodGuaranteedEnvelopeVcpu: this.config.prodGuaranteedEnvelopeVcpu,
      ...(this.config.environmentVcpuBudget !== undefined
        ? { environmentVcpuBudget: this.config.environmentVcpuBudget }
        : {}),
      capturedAt,
    };
    return { services, snapshot };
  }

  async updateDesiredCount(
    service: ManagedTemporalService,
    desiredCount: number,
  ) {
    if (desiredCount < service.desiredCount) {
      throw new Error(
        `Increase-only controller refused ${service.serviceName} decrease ${service.desiredCount} -> ${desiredCount}`,
      );
    }
    if (desiredCount === service.desiredCount) return undefined;
    const response = await this.ecs.send(
      new UpdateServiceCommand({
        cluster: service.clusterArn,
        service: service.serviceArn,
        desiredCount,
      }),
    );
    return response.$metadata.requestId;
  }

  async updateTaskProtection(params: {
    clusterArn: string;
    taskArn: string;
    protectionEnabled: boolean;
    expiresInMinutes?: number;
  }) {
    const readProtection = async () => {
      const response = await this.ecs.send(
        new GetTaskProtectionCommand({
          cluster: params.clusterArn,
          tasks: [params.taskArn],
        }),
      );
      const terminalFailure = response.failures?.some(
        (failure) =>
          failure.arn === params.taskArn &&
          (failure.reason === "MISSING" || failure.reason === "STOPPED"),
      );
      if (terminalFailure && !params.protectionEnabled) {
        return {
          taskArn: params.taskArn,
          protectionEnabled: false,
        };
      }
      const unexpectedFailures = (response.failures ?? []).filter(
        (failure) =>
          failure.reason !== "MISSING" && failure.reason !== "STOPPED",
      );
      if (unexpectedFailures.length > 0) {
        throw new Error(
          `ECS GetTaskProtection returned failures: ${JSON.stringify(unexpectedFailures)}`,
        );
      }
      return response.protectedTasks?.find(
        (item) => item.taskArn === params.taskArn,
      );
    };
    const response = await this.ecs.send(
      new UpdateTaskProtectionCommand({
        cluster: params.clusterArn,
        tasks: [params.taskArn],
        protectionEnabled: params.protectionEnabled,
        ...(params.protectionEnabled && params.expiresInMinutes
          ? { expiresInMinutes: params.expiresInMinutes }
          : {}),
      }),
    );
    const terminalFailure = response.failures?.some(
      (failure) =>
        failure.arn === params.taskArn &&
        (failure.reason === "MISSING" || failure.reason === "STOPPED"),
    );
    const unexpectedFailures = (response.failures ?? []).filter(
      (failure) => failure.reason !== "MISSING" && failure.reason !== "STOPPED",
    );
    if (unexpectedFailures.length > 0) {
      throw new Error(
        `ECS UpdateTaskProtection returned failures: ${JSON.stringify(unexpectedFailures)}`,
      );
    }
    if (terminalFailure && !params.protectionEnabled) {
      return {
        taskArn: params.taskArn,
        protectionEnabled: false,
      };
    }
    const task = response.protectedTasks?.find(
      (item) => item.taskArn === params.taskArn,
    );
    const resolved =
      task?.protectionEnabled === params.protectionEnabled
        ? task
        : await readProtection();
    if (!resolved || resolved.protectionEnabled !== params.protectionEnabled) {
      throw new Error(
        `ECS task protection did not reach ${params.protectionEnabled} for ${params.taskArn}`,
      );
    }
    return resolved;
  }

  async readTaskTerminalState(params: { clusterArn: string; taskArn: string }) {
    const response = await this.ecs.send(
      new DescribeTasksCommand({
        cluster: params.clusterArn,
        tasks: [params.taskArn],
      }),
    );
    const task = response.tasks?.find(
      (candidate) => candidate.taskArn === params.taskArn,
    );
    if (task) {
      return {
        terminal: task.lastStatus === "STOPPED",
        lastStatus: task.lastStatus ?? "UNKNOWN",
        desiredStatus: task.desiredStatus ?? "UNKNOWN",
      };
    }
    const missing = response.failures?.some(
      (failure) =>
        failure.arn === params.taskArn &&
        (failure.reason === "MISSING" || failure.reason === "STOPPED"),
    );
    if (!missing) {
      throw new Error(
        `ECS could not determine terminal state for ${params.taskArn}: ${JSON.stringify(response.failures)}`,
      );
    }
    return {
      terminal: true,
      lastStatus: "PURGED",
      desiredStatus: "STOPPED",
    };
  }

  async decreaseDesiredCount(
    service: ManagedTemporalService,
    desiredCount: number,
  ) {
    if (desiredCount !== service.desiredCount - 1) {
      throw new Error(
        `Protected scale-in only permits one-task decrements: ${service.desiredCount} -> ${desiredCount}`,
      );
    }
    const current = await this.ecs.send(
      new DescribeServicesCommand({
        cluster: service.clusterArn,
        services: [service.serviceArn],
      }),
    );
    const liveDesired = current.services?.[0]?.desiredCount;
    if (liveDesired !== service.desiredCount) {
      throw new Error(
        `Protected scale-in desired count changed before write: expected ${service.desiredCount}, observed ${liveDesired ?? "missing"}`,
      );
    }
    const response = await this.ecs.send(
      new UpdateServiceCommand({
        cluster: service.clusterArn,
        service: service.serviceArn,
        desiredCount,
      }),
    );
    return response.$metadata.requestId;
  }

  // Batch-retirement zeroing for a DRAINED build's service: unlike protected
  // scale-in this writes desiredCount=0 in one shot, revalidated by an O(1)
  // point DescribeServices against the cycle's expected desired count (the
  // same pattern decreaseDesiredCount uses — never an account re-scan).
  async zeroDesiredCount(service: ManagedTemporalService) {
    const current = await this.ecs.send(
      new DescribeServicesCommand({
        cluster: service.clusterArn,
        services: [service.serviceArn],
      }),
    );
    const liveDesired = current.services?.[0]?.desiredCount;
    if (liveDesired !== service.desiredCount) {
      throw new Error(
        `Retirement desired count changed before write: expected ${service.desiredCount}, observed ${liveDesired ?? "missing"}`,
      );
    }
    const response = await this.ecs.send(
      new UpdateServiceCommand({
        cluster: service.clusterArn,
        service: service.serviceArn,
        desiredCount: 0,
      }),
    );
    return response.$metadata.requestId;
  }

  async updateManagedService(params: {
    clusterArn: string;
    serviceArn: string;
    desiredCount?: number;
    taskDefinitionArn?: string;
    forceNewDeployment?: boolean;
  }) {
    const cluster = this.config.clusters.find(
      (item) =>
        params.clusterArn === item.clusterName ||
        params.clusterArn.endsWith(`/${item.clusterName}`),
    );
    if (!cluster) {
      throw new Error(`Cluster ${params.clusterArn} is not controller-managed`);
    }
    const described = await this.ecs.send(
      new DescribeServicesCommand({
        cluster: params.clusterArn,
        services: [params.serviceArn],
        include: ["TAGS"],
      }),
    );
    const service = described.services?.[0];
    if (!service || service.status !== "ACTIVE") {
      throw new Error(`Managed service ${params.serviceArn} is not active`);
    }
    const tags = tagMap(service);
    const expectedPrefix = `arn:aws:ecs:${this.config.region}:${this.config.accountId}:service/${cluster.clusterName}/capy-temporal-worker-${cluster.environment}-`;
    if (!params.serviceArn.startsWith(expectedPrefix)) {
      throw new Error(
        `Service ${params.serviceArn} is outside the managed set`,
      );
    }
    if (
      params.desiredCount !== undefined &&
      tags["capy:capacity-managed"] !== "true"
    ) {
      throw new Error(
        `Service ${params.serviceArn} is not capacity-managed and cannot change desired count`,
      );
    }
    if (params.taskDefinitionArn) {
      const taskDefinition = await this.ecs.send(
        new DescribeTaskDefinitionCommand({
          taskDefinition: params.taskDefinitionArn,
        }),
      );
      const describedTaskDefinition = taskDefinition.taskDefinition;
      if (
        !describedTaskDefinition ||
        describedTaskDefinition.family !== service.serviceName
      ) {
        throw new Error(
          `Task definition ${params.taskDefinitionArn} does not belong to service ${service.serviceName}`,
        );
      }
      if (
        describedTaskDefinition.runtimePlatform?.cpuArchitecture !== "ARM64"
      ) {
        throw new Error(
          `Task definition ${params.taskDefinitionArn} is not ARM64`,
        );
      }
    }
    if (
      params.desiredCount !== undefined &&
      params.desiredCount < (service.desiredCount ?? 0)
    ) {
      throw new Error(
        `Increase-only maintenance update refused ${params.serviceArn} decrease`,
      );
    }
    if (params.desiredCount !== undefined) {
      const poolId = tags["capy:worker-pool"];
      if (!isPoolId(poolId)) {
        throw new Error(`Service ${params.serviceArn} has no valid pool tag`);
      }
      const hardMax = TEMPORAL_STABLE_POOLS[poolId].hardMax;
      if (params.desiredCount > hardMax) {
        throw new Error(
          `Desired count ${params.desiredCount} exceeds ${poolId} hard max ${hardMax}`,
        );
      }
    }
    const response = await this.ecs.send(
      new (await import("@aws-sdk/client-ecs")).UpdateServiceCommand({
        cluster: params.clusterArn,
        service: params.serviceArn,
        ...(params.desiredCount === undefined
          ? {}
          : { desiredCount: params.desiredCount }),
        ...(params.taskDefinitionArn
          ? { taskDefinition: params.taskDefinitionArn }
          : {}),
        forceNewDeployment: params.forceNewDeployment,
        deploymentConfiguration: {
          minimumHealthyPercent: 100,
          maximumPercent: 200,
          deploymentCircuitBreaker: {
            enable: true,
            rollback: true,
          },
        },
      }),
    );
    return { requestId: response.$metadata.requestId };
  }

  // Controller-health metrics gain a Controller dimension when this instance
  // is environment-scoped, so each per-env stack alarms on its own
  // controller's heartbeat and a live sibling cannot mask a dead one.
  // CloudWatch keys metrics by their full dimension set, so the legacy
  // (unscoped) controller keeps emitting dimensionless metrics that its
  // existing alarms continue to match. Queue-backlog metrics stay keyed by
  // Environment only (their consumers already partition on it).
  private controllerHealthDimensions() {
    return this.config.environmentScope
      ? [{ Name: "Controller", Value: this.config.environmentScope }]
      : [];
  }

  // CycleResult-only emission for cycles that exit before any capacity read
  // exists: the quiet "not my turn" exits (another invoke holds the cycle
  // lock, or this execution's chain heartbeat was superseded). These used to
  // surface as raw Lambda invoke errors; the dimension value here is what
  // distinguishes benign contention from a genuinely broken controller.
  async emitCycleResultMetric(params: {
    result: "LOCK_CONTENDED" | "SUPERSEDED";
  }) {
    await this.cloudwatch.send(
      new PutMetricDataCommand({
        Namespace: "Capy/TemporalCapacity",
        MetricData: [
          {
            MetricName: "CycleResult",
            Value: 1,
            Unit: "Count",
            StorageResolution: 1,
            Dimensions: [
              { Name: "Result", Value: params.result },
              ...this.controllerHealthDimensions(),
            ],
          },
        ],
      }),
    );
  }

  async emitCycleMetrics(params: {
    result: string;
    managedCommittedVcpu: number;
    accountUsageVcpu: number;
    quotaVcpu: number;
    ungrantedProdReplicas: number;
    cycleDurationMs: number;
    staleTemporalInputs: number;
    staleWorkerHeartbeats: number;
    pendingTasks: number;
    desiredNotReadyReplicas: number;
    drainDeadlineExpired: number;
    drainedAwaitingRetirement: number;
    retirementVerifyTimeouts: number;
  }) {
    const metricData: MetricDatum[] = [
      {
        MetricName: "CycleCompleted",
        Value: 1,
        Unit: "Count",
        StorageResolution: 1,
      },
      {
        MetricName: "CycleResult",
        Value: 1,
        Unit: "Count",
        StorageResolution: 1,
        Dimensions: [{ Name: "Result", Value: params.result }],
      },
      {
        MetricName: "CycleDuration",
        Value: params.cycleDurationMs,
        Unit: "Milliseconds",
        StorageResolution: 1,
      },
      {
        MetricName: "ManagedCommittedVcpu",
        Value: params.managedCommittedVcpu,
        Unit: "Count",
      },
      {
        MetricName: "AccountUsageVcpu",
        Value: params.accountUsageVcpu,
        Unit: "Count",
      },
      {
        MetricName: "FargateQuotaVcpu",
        Value: params.quotaVcpu,
        Unit: "Count",
      },
      {
        MetricName: "FargateQuotaUtilization",
        Value:
          params.quotaVcpu > 0
            ? (params.accountUsageVcpu / params.quotaVcpu) * 100
            : 100,
        Unit: "Percent",
      },
      {
        MetricName: "UngrantedProdReplicas",
        Value: params.ungrantedProdReplicas,
        Unit: "Count",
        StorageResolution: 1,
      },
      {
        MetricName: "StaleTemporalInputs",
        Value: params.staleTemporalInputs,
        Unit: "Count",
        StorageResolution: 1,
      },
      {
        MetricName: "StaleWorkerHeartbeats",
        Value: params.staleWorkerHeartbeats,
        Unit: "Count",
        StorageResolution: 1,
      },
      {
        MetricName: "PendingTasks",
        Value: params.pendingTasks,
        Unit: "Count",
        StorageResolution: 1,
      },
      {
        MetricName: "DesiredNotReadyReplicas",
        Value: params.desiredNotReadyReplicas,
        Unit: "Count",
        StorageResolution: 1,
      },
      // Emitted every cycle, zero included, so the >=1 alarm can tell a
      // healthy quiet controller from a dead emitter.
      {
        MetricName: "DrainDeadlineExpired",
        Value: params.drainDeadlineExpired,
        Unit: "Count",
        StorageResolution: 1,
      },
      // Builds fully zeroed by the retirement lane but whose ECS services the
      // iac retire verb has not yet deleted. Emitted every cycle (zero
      // included) so a wedged reaper is visible as a sustained nonzero count.
      {
        MetricName: "DrainedAwaitingRetirement",
        Value: params.drainedAwaitingRetirement,
        Unit: "Count",
        StorageResolution: 1,
      },
      // Retirements that hit their verify deadline (typed FAILED closes,
      // including zeroed-but-still-running builds, which otherwise appear in
      // no signal). Zero-inclusive like DrainDeadlineExpired so an alarm can
      // tell quiet from dead.
      {
        MetricName: "RetirementVerifyTimeout",
        Value: params.retirementVerifyTimeouts,
        Unit: "Count",
        StorageResolution: 1,
      },
      // Only environment-scoped controllers carry a budget; the legacy shared
      // controller must not emit a utilization series nothing alarms on.
      ...(this.config.environmentVcpuBudget !== undefined
        ? [
            {
              MetricName: "EnvironmentBudgetUtilization",
              Value:
                this.config.environmentVcpuBudget > 0
                  ? (params.managedCommittedVcpu /
                      this.config.environmentVcpuBudget) *
                    100
                  : 100,
              Unit: "Percent",
            } satisfies MetricDatum,
          ]
        : []),
    ];
    const controllerDimensions = this.controllerHealthDimensions();
    await this.cloudwatch.send(
      new PutMetricDataCommand({
        Namespace: "Capy/TemporalCapacity",
        MetricData: metricData.map((datum) => ({
          ...datum,
          Dimensions: [...(datum.Dimensions ?? []), ...controllerDimensions],
        })),
      }),
    );
  }

  async emitQueueBacklogMetrics(params: {
    observations: QueueCapacityObservation[];
  }) {
    const metricData = buildQueueBacklogMetricData(params.observations);
    if (metricData.length === 0) return;
    await this.cloudwatch.send(
      new PutMetricDataCommand({
        Namespace: "Capy/TemporalCapacity",
        MetricData: metricData,
      }),
    );
  }

  async emitLoadGateMetrics(params: { allowed: boolean }) {
    await this.cloudwatch.send(
      new PutMetricDataCommand({
        Namespace: "Capy/TemporalCapacity",
        MetricData: [
          {
            MetricName: "LoadGateDenied",
            Value: params.allowed ? 0 : 1,
            Unit: "Count",
            StorageResolution: 1,
            Dimensions: this.controllerHealthDimensions(),
          },
        ],
      }),
    );
  }
}
