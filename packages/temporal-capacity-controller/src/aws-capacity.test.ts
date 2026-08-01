import type { Service } from "@aws-sdk/client-ecs";
import { describe, expect, it } from "vitest";

import {
  AwsCapacityReader,
  isExactFrozenLegacySubagentPoolService,
} from "./aws-capacity.js";
import type { ControllerConfig } from "./types.js";

const taskProtectionConfig: ControllerConfig = {
  accountId: "123456789012",
  region: "us-west-2",
  tableName: "control",
  drainTableName: "drains",
  controlPartitionKey: "CONTROL#123456789012#us-west-2",
  clusters: [],
  hardReserveVcpu: 96,
  prodGuaranteedEnvelopeVcpu: 303,
  serviceQuotaCode: "L-3032A538",
  workerHeartbeatFreshnessMs: 90_000,
  cycleLockMs: 70_000,
  cycleStaleAfterMs: 90_000,
  reservationSnapshotMaxAgeMs: 90_000,
  auditTtlSeconds: 14 * 24 * 60 * 60,
  chainRotationCycles: 2_000,
};

type AwsCommand = { constructor: { name: string } };

const readerWithResponses = (responses: Record<string, unknown[]>) => {
  const reader = new AwsCapacityReader(taskProtectionConfig);
  Object.defineProperty(reader, "ecs", {
    value: {
      async send(command: AwsCommand) {
        const queue = responses[command.constructor.name];
        const response = queue?.shift();
        if (!response) {
          throw new Error(`Unexpected ${command.constructor.name}`);
        }
        return response;
      },
    },
  });
  return reader;
};

describe("AwsCapacityReader", () => {
  it("confirms disabled task protection with a readback when update omits the task", async () => {
    const reader = readerWithResponses({
      UpdateTaskProtectionCommand: [{ protectedTasks: [], failures: [] }],
      GetTaskProtectionCommand: [
        {
          protectedTasks: [
            {
              taskArn: "task-1",
              protectionEnabled: false,
            },
          ],
          failures: [],
        },
      ],
    });

    await expect(
      reader.updateTaskProtection({
        clusterArn: "cluster",
        taskArn: "task-1",
        protectionEnabled: false,
      }),
    ).resolves.toMatchObject({
      taskArn: "task-1",
      protectionEnabled: false,
    });
  });

  it("treats a stopped task as unprotected when disabling protection", async () => {
    const reader = readerWithResponses({
      UpdateTaskProtectionCommand: [{ protectedTasks: [], failures: [] }],
      GetTaskProtectionCommand: [
        {
          protectedTasks: [],
          failures: [{ arn: "task-1", reason: "STOPPED" }],
        },
      ],
    });

    await expect(
      reader.updateTaskProtection({
        clusterArn: "cluster",
        taskArn: "task-1",
        protectionEnabled: false,
      }),
    ).resolves.toMatchObject({
      taskArn: "task-1",
      protectionEnabled: false,
    });
  });

  it("confirms enabled task protection with a readback when update omits the task", async () => {
    const reader = readerWithResponses({
      UpdateTaskProtectionCommand: [{ protectedTasks: [], failures: [] }],
      GetTaskProtectionCommand: [
        {
          protectedTasks: [
            {
              taskArn: "task-1",
              protectionEnabled: true,
            },
          ],
          failures: [],
        },
      ],
    });

    await expect(
      reader.updateTaskProtection({
        clusterArn: "cluster",
        taskArn: "task-1",
        protectionEnabled: true,
        expiresInMinutes: 60,
      }),
    ).resolves.toMatchObject({
      taskArn: "task-1",
      protectionEnabled: true,
    });
  });
});

const buildId = "111111111111aaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const deploymentName = "capy-temporal-worker-prod";
const serviceName = `${deploymentName}-subagent-pool-service-${buildId.slice(0, 12)}`;

const tags = {
  "capy:temporal-worker": "true",
  "capy:capacity-managed": "true",
  "capy:worker-pool": "subagent",
  "capy:environment": "prod",
  "capy:worker-build-id": buildId,
  "capy:capacity-manifest-version": "1",
  "capy:task-vcpu": "2",
};

const service = (overrides: Partial<Service> = {}): Service => ({
  serviceArn: `arn:aws:ecs:us-west-2:123456789012:service/prod/${serviceName}`,
  serviceName,
  clusterArn: "arn:aws:ecs:us-west-2:123456789012:cluster/prod",
  taskDefinition:
    "arn:aws:ecs:us-west-2:123456789012:task-definition/legacy-subagent:1",
  desiredCount: 1,
  runningCount: 1,
  pendingCount: 0,
  tags: Object.entries(tags).map(([key, value]) => ({ key, value })),
  deployments: [{ status: "PRIMARY", rolloutState: "COMPLETED" }],
  ...overrides,
});

const frozenPoolConfig: ControllerConfig = {
  ...taskProtectionConfig,
  clusters: [
    {
      environment: "prod",
      clusterName: "prod",
      deploymentName,
      temporalParameterPrefix: "/capy/prod/env",
    },
  ],
  hardReserveVcpu: 10,
  prodGuaranteedEnvelopeVcpu: 20,
  workerHeartbeatFreshnessMs: 15_000,
  cycleLockMs: 8_000,
  auditTtlSeconds: 86_400,
  chainRotationCycles: 4_000,
};

type TestReader = {
  listServices(clusterName: string): Promise<string[]>;
  describeServices(clusterName: string, arns: string[]): Promise<Service[]>;
  quotaVcpu(): Promise<number>;
  accountUsageVcpu(): Promise<number | undefined>;
};

function readerFor(candidate: Service): AwsCapacityReader {
  const reader = new AwsCapacityReader(frozenPoolConfig);
  const testReader = reader as unknown as TestReader;
  testReader.listServices = async () => [candidate.serviceArn!];
  testReader.describeServices = async () => [candidate];
  testReader.quotaVcpu = async () => 100;
  testReader.accountUsageVcpu = async () => 2;
  return reader;
}

describe("frozen legacy subagent capacity discovery", () => {
  it("recognizes and excludes only the exact historical pool", async () => {
    expect(
      isExactFrozenLegacySubagentPoolService({
        serviceName,
        deploymentName,
        environment: "prod",
        tags,
      }),
    ).toBe(true);

    await expect(readerFor(service()).readForReservation(0)).resolves.toEqual(
      expect.objectContaining({
        services: [],
        snapshot: expect.objectContaining({
          managedCommittedVcpu: 0,
          unmanagedCommittedVcpu: 2,
        }),
      }),
    );
  });

  it.each([
    {
      label: "wrong service name",
      mutate: (candidate: Service) => ({
        ...candidate,
        serviceName: `${deploymentName}-subagent-service-${buildId.slice(0, 12)}`,
      }),
    },
    {
      label: "wrong environment",
      mutate: (candidate: Service) => ({
        ...candidate,
        tags: candidate.tags?.map((tag) =>
          tag.key === "capy:environment" ? { ...tag, value: "dev" } : tag,
        ),
      }),
    },
    {
      label: "missing build tag",
      mutate: (candidate: Service) => ({
        ...candidate,
        tags: candidate.tags?.filter(
          (tag) => tag.key !== "capy:worker-build-id",
        ),
      }),
    },
    {
      label: "wrong manifest",
      mutate: (candidate: Service) => ({
        ...candidate,
        tags: candidate.tags?.map((tag) =>
          tag.key === "capy:capacity-manifest-version"
            ? { ...tag, value: "2" }
            : tag,
        ),
      }),
    },
  ])("fails closed for a $label near-match", async ({ mutate }) => {
    await expect(
      readerFor(mutate(service())).readForReservation(0),
    ).rejects.toThrow(
      "resembles the frozen legacy subagent pool but does not match its exact identity",
    );
  });
});

describe("managed service inventory tolerance", () => {
  it("accounts for a foreign pool taxonomy as unmanaged capacity", async () => {
    const candidate = service({
      serviceName: `${deploymentName}-future-pool-service-${buildId.slice(0, 12)}`,
      tags: service().tags?.map((tag) =>
        tag.key === "capy:worker-pool" ? { ...tag, value: "future-pool" } : tag,
      ),
    });

    await expect(readerFor(candidate).readForReservation(0)).resolves.toEqual(
      expect.objectContaining({
        services: [],
        snapshot: expect.objectContaining({
          managedCommittedVcpu: 0,
          unmanagedCommittedVcpu: 2,
        }),
      }),
    );
  });

  it("accounts for a foreign manifest version as unmanaged capacity", async () => {
    const candidate = service({
      serviceName: `${deploymentName}-parent-service-${buildId.slice(0, 12)}`,
      tags: service().tags?.map((tag) => {
        if (tag.key === "capy:worker-pool") {
          return { ...tag, value: "parent" };
        }
        if (tag.key === "capy:capacity-manifest-version") {
          return { ...tag, value: "0" };
        }
        return tag;
      }),
    });

    await expect(readerFor(candidate).readForReservation(0)).resolves.toEqual(
      expect.objectContaining({
        services: [],
        snapshot: expect.objectContaining({
          managedCommittedVcpu: 0,
          unmanagedCommittedVcpu: 2,
        }),
      }),
    );
  });
});

type PublishedDatum = {
  MetricName: string;
  Value: number;
  Dimensions?: Array<{ Name: string; Value: string }>;
};

const readerWithMetricSink = (config: ControllerConfig) => {
  const published: PublishedDatum[] = [];
  const reader = new AwsCapacityReader(config);
  Object.defineProperty(reader, "cloudwatch", {
    value: {
      async send(command: { input: { MetricData?: PublishedDatum[] } }) {
        published.push(...(command.input.MetricData ?? []));
        return {};
      },
    },
  });
  return { reader, published };
};

const cycleMetricsParams = {
  result: "APPLIED",
  managedCommittedVcpu: 45,
  accountUsageVcpu: 50,
  quotaVcpu: 100,
  ungrantedProdReplicas: 0,
  cycleDurationMs: 1_000,
  staleTemporalInputs: 0,
  staleWorkerHeartbeats: 0,
  pendingTasks: 0,
  desiredNotReadyReplicas: 0,
  drainDeadlineExpired: 0,
  drainedAwaitingRetirement: 0,
  retirementVerifyTimeouts: 0,
};

const scopedConfig: ControllerConfig = {
  ...taskProtectionConfig,
  environmentScope: "prod",
  environmentVcpuBudget: 50,
};

describe("cycle metric emission", () => {
  it("emits DrainDeadlineExpired with the Controller dimension", async () => {
    const { reader, published } = readerWithMetricSink(scopedConfig);
    await reader.emitCycleMetrics({
      ...cycleMetricsParams,
      drainDeadlineExpired: 2,
    });
    expect(
      published.find((datum) => datum.MetricName === "DrainDeadlineExpired"),
    ).toMatchObject({
      Value: 2,
      Dimensions: [{ Name: "Controller", Value: "prod" }],
    });
  });

  it("emits a DrainDeadlineExpired zero every cycle so the alarm sees healthy data", async () => {
    const { reader, published } = readerWithMetricSink(scopedConfig);
    await reader.emitCycleMetrics(cycleMetricsParams);
    expect(
      published.find((datum) => datum.MetricName === "DrainDeadlineExpired"),
    ).toMatchObject({ Value: 0 });
  });

  it("emits DrainedAwaitingRetirement every cycle, zero included, so a wedged reaper is visible", async () => {
    const { reader, published } = readerWithMetricSink(scopedConfig);
    await reader.emitCycleMetrics(cycleMetricsParams);
    expect(
      published.find(
        (datum) => datum.MetricName === "DrainedAwaitingRetirement",
      ),
    ).toMatchObject({ Value: 0 });
    await reader.emitCycleMetrics({
      ...cycleMetricsParams,
      drainedAwaitingRetirement: 3,
    });
    expect(
      published.filter(
        (datum) => datum.MetricName === "DrainedAwaitingRetirement",
      ),
    ).toHaveLength(2);
  });

  it("emits EnvironmentBudgetUtilization as the managed share of the environment budget", async () => {
    const { reader, published } = readerWithMetricSink(scopedConfig);
    await reader.emitCycleMetrics(cycleMetricsParams);
    expect(
      published.find(
        (datum) => datum.MetricName === "EnvironmentBudgetUtilization",
      ),
    ).toMatchObject({
      // 45 managed vCPU against a 50 vCPU budget.
      Value: 90,
      Dimensions: [{ Name: "Controller", Value: "prod" }],
    });
  });

  it("omits EnvironmentBudgetUtilization for the unscoped legacy controller", async () => {
    const { reader, published } = readerWithMetricSink(taskProtectionConfig);
    await reader.emitCycleMetrics(cycleMetricsParams);
    expect(
      published.find(
        (datum) => datum.MetricName === "EnvironmentBudgetUtilization",
      ),
    ).toBeUndefined();
    // The legacy controller still emits the drain metric, dimensionless.
    expect(
      published.find((datum) => datum.MetricName === "DrainDeadlineExpired"),
    ).toMatchObject({ Value: 0, Dimensions: [] });
  });
});
