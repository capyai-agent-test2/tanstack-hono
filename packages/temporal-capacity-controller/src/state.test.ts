import {
  ConditionalCheckFailedException,
  TransactGetItemsCommand,
  TransactWriteItemsCommand,
  TransactionCanceledException,
  TransactionConflictException,
  UpdateItemCommand,
  type DynamoDBClient,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { randomBytes } from "node:crypto";
import { describe, expect, it, vi } from "vitest";

import {
  CapacityStateStore,
  MAX_PACKED_CYCLE_AUDIT_BYTES,
  MAX_PACKED_RECONCILER_MAPS_BYTES,
  isConditionalCheckContention,
  isRetriableDynamoContention,
  packCycleAudit,
  packReconcilerMaps,
  unpackCycleAudit,
  unpackReconcilerMaps,
} from "./state.js";
import type { TemporalRetirementRecord } from "@capy/shared/temporal/capacity";
import type {
  CapacityReservation,
  ControllerConfig,
  ReconcilerState,
  TemporalDrainRecord,
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

describe("capacity reservation transaction", () => {
  it("retries initialization when a bootstrap write overlaps a transaction", async () => {
    let ledgerAttempts = 0;
    const client = {
      async send(command: unknown) {
        if (
          command instanceof UpdateItemCommand &&
          unmarshall(command.input.Key ?? {}).SK === "CAPACITY_LEDGER"
        ) {
          ledgerAttempts += 1;
          if (ledgerAttempts === 1) {
            throw new TransactionConflictException({
              $metadata: {},
              message: "Transaction is ongoing for the item",
            });
          }
        }
        return {};
      },
    } as DynamoDBClient;

    await new CapacityStateStore(config, client).initialize();

    expect(ledgerAttempts).toBe(2);
  });

  it("preserves non-conflict failures from concurrent initialization writes", async () => {
    const permanentFailure = new Error("access denied");
    const client = {
      async send(command: unknown) {
        if (!(command instanceof UpdateItemCommand)) return {};
        const sortKey = unmarshall(command.input.Key ?? {}).SK;
        if (sortKey === "CAPACITY_LEDGER") {
          throw new TransactionConflictException({
            $metadata: {},
            message: "Transaction is ongoing for the item",
          });
        }
        if (sortKey === "CHAIN") throw permanentFailure;
        return {};
      },
    } as DynamoDBClient;

    await expect(
      new CapacityStateStore(config, client).initialize(),
    ).rejects.toBe(permanentFailure);
  });

  it("retries initialization when the reconciler bootstrap write conflicts", async () => {
    let reconcilerAttempts = 0;
    const client = {
      async send(command: unknown) {
        if (
          command instanceof UpdateItemCommand &&
          unmarshall(command.input.Key ?? {}).SK === "RECONCILER"
        ) {
          reconcilerAttempts += 1;
          if (reconcilerAttempts === 1) {
            throw new TransactionConflictException({
              $metadata: {},
              message: "Transaction is ongoing for the item",
            });
          }
        }
        return {};
      },
    } as DynamoDBClient;

    await new CapacityStateStore(config, client).initialize();

    expect(reconcilerAttempts).toBe(2);
  });

  it("atomically claims ledger generation and inserts one reservation", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    await store.admitReservation({
      reservation: {
        reservationId: "deploy-1",
        ownerToken: "owner-1",
        environment: "prod",
        buildId: "build-1",
        pools: ["parent"],
        requestedVcpu: 2,
        expiresAt: Date.now() + 60_000,
      },
      expectedLedgerGeneration: 4,
      expectedActiveReservationVcpu: 6,
      now: Date.now(),
    });

    expect(commands).toHaveLength(1);
    const command = commands[0];
    expect(command).toBeInstanceOf(TransactWriteItemsCommand);
    const input = (command as TransactWriteItemsCommand).input;
    expect(input.ClientRequestToken).toMatch(/^[a-f0-9]{36}$/);
    expect(input.TransactItems).toHaveLength(2);
    expect(input.TransactItems?.[0]?.Update?.ConditionExpression).toContain(
      "generation = :generation",
    );
    expect(input.TransactItems?.[0]?.Update?.ConditionExpression).toContain(
      "activeReservationVcpu = :expectedActiveReservationVcpu",
    );
    expect(input.TransactItems?.[1]?.Put?.ConditionExpression).toBe(
      "attribute_not_exists(PK)",
    );
  });

  it("uses a new idempotency token when retrying a conditional ledger race", async () => {
    const commands: TransactWriteItemsCommand[] = [];
    const client = {
      async send(command: TransactWriteItemsCommand) {
        commands.push(command);
        if (commands.length === 1) {
          throw new TransactionCanceledException({
            $metadata: {},
            message: "concurrent reservation",
          });
        }
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();
    const reservation = {
      reservationId: "deploy-race",
      ownerToken: "owner-1",
      environment: "prod" as const,
      buildId: "build-1",
      pools: ["parent" as const],
      requestedVcpu: 2,
      expiresAt: now + 60_000,
    };

    await expect(
      store.admitReservation({
        reservation,
        expectedLedgerGeneration: 4,
        expectedActiveReservationVcpu: 6,
        now,
      }),
    ).rejects.toBeInstanceOf(TransactionCanceledException);
    await expect(
      store.admitReservation({
        reservation,
        expectedLedgerGeneration: 5,
        expectedActiveReservationVcpu: 8,
        now,
      }),
    ).resolves.toBe(6);
    await store.admitReservation({
      reservation,
      expectedLedgerGeneration: 5,
      expectedActiveReservationVcpu: 8,
      now,
    });

    expect(commands).toHaveLength(3);
    expect(commands[1]?.input.ClientRequestToken).not.toBe(
      commands[0]?.input.ClientRequestToken,
    );
    expect(commands[2]?.input.ClientRequestToken).toBe(
      commands[1]?.input.ClientRequestToken,
    );
  });

  it.each([
    [
      "conditional transaction cancellation",
      new TransactionCanceledException({
        $metadata: {},
        message: "concurrent expiry",
      }),
    ],
    [
      "in-flight transaction conflict",
      new TransactionConflictException({
        $metadata: {},
        message: "Transaction is ongoing for the item",
      }),
    ],
  ])("retries expiry after a %s", async (_name, conflict) => {
    const now = Date.now();
    const reservation: CapacityReservation = {
      reservationId: "expired-1",
      ownerToken: "owner-1",
      environment: "dev" as const,
      buildId: "build-1",
      pools: ["parent"],
      requestedVcpu: 2,
      state: "ACTIVE" as const,
      expiresAt: now - 1,
      ledgerGeneration: 4,
      serviceArns: [],
      consumedVcpu: 0,
    };
    const store = new CapacityStateStore(config, {
      async send() {
        return {};
      },
    } as unknown as DynamoDBClient);
    vi.spyOn(store, "listOpenReservations").mockResolvedValue([reservation]);
    vi.spyOn(store, "readReservation").mockResolvedValue(reservation);
    vi.spyOn(store, "readControlSnapshot")
      .mockResolvedValueOnce({
        authority: {
          generation: 1,
          writerKind: "STEP_FUNCTIONS_LAMBDA",
          transitionId: "test",
          effectiveAt: now,
          checksum: "test",
        },
        ledger: {
          generation: 4,
          managedCommittedVcpu: 0,
          activeReservationVcpu: 2,
          allocations: {},
          inventoryHash: "inventory",
          updatedAt: now,
        },
        reconciler: { capability: "PROTECTED_SCALE_IN" },
      })
      .mockResolvedValueOnce({
        authority: {
          generation: 1,
          writerKind: "STEP_FUNCTIONS_LAMBDA",
          transitionId: "test",
          effectiveAt: now,
          checksum: "test",
        },
        ledger: {
          generation: 5,
          managedCommittedVcpu: 0,
          activeReservationVcpu: 2,
          allocations: {},
          inventoryHash: "inventory",
          updatedAt: now,
        },
        reconciler: { capability: "PROTECTED_SCALE_IN" },
      });
    const release = vi
      .spyOn(store, "releaseReservation")
      .mockRejectedValueOnce(conflict)
      .mockResolvedValueOnce(6);

    await store.expireReservations(now);

    expect(release).toHaveBeenCalledTimes(2);
    expect(release.mock.calls[1]?.[0].expectedLedgerGeneration).toBe(5);
  });

  it("fences reservation consumption without unsupported condition arithmetic", async () => {
    const commands: unknown[] = [];
    const reservation: CapacityReservation = {
      reservationId: "deploy-1",
      ownerToken: "owner-1",
      environment: "prod",
      buildId: "build-1",
      pools: ["parent"],
      requestedVcpu: 4,
      state: "ACTIVE",
      expiresAt: Date.now() + 60_000,
      ledgerGeneration: 4,
      serviceArns: [],
      consumedVcpu: 1,
    };
    const client = {
      async send(command: unknown) {
        commands.push(command);
        if (commands.length === 1) {
          return { Item: marshall(reservation) };
        }
        return { Attributes: marshall({ ...reservation, consumedVcpu: 3 }) };
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);

    await store.consumeReservation({
      reservationId: reservation.reservationId,
      ownerToken: reservation.ownerToken,
      serviceArn: "arn:service/parent",
      additionalVcpu: 2,
      now: Date.now(),
    });

    const update = commands[1];
    expect(update).toBeInstanceOf(UpdateItemCommand);
    expect((update as UpdateItemCommand).input.ConditionExpression).toContain(
      "consumedVcpu <= :maxPriorConsumedVcpu",
    );
    expect(
      (update as UpdateItemCommand).input.ConditionExpression,
    ).not.toContain("consumedVcpu +");
  });

  it("claims a decrease only with the exact ready drain and write fences", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn: "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1",
      clusterArn: "cluster",
      serviceArn: "service",
      poolId: "parent",
      buildId: "build-1",
      intentId: "intent-1",
      cycleId: "cycle-1",
      authorityGeneration: 5,
      ledgerGeneration: 6,
      priorDesiredCount: 3,
      targetDesiredCount: 2,
      state: "READY",
      createdAt: now,
      deadline: now + 60_000,
    };
    await store.claimProtectedDecrease({
      drain,
      cycleId: "cycle-1",
      authorityGeneration: 7,
      ledger: {
        generation: 9,
        managedCommittedVcpu: 6,
        activeReservationVcpu: 0,
        allocations: { service: 6 },
        inventoryHash: "inventory",
        updatedAt: now,
      },
      serviceArn: "service",
      taskVcpu: 2,
      targetAllocationVcpu: 4,
      now,
    });

    const input = (commands[0] as TransactWriteItemsCommand).input;
    expect(input.TransactItems).toHaveLength(4);
    expect(input.TransactItems?.[3]?.Update?.ConditionExpression).toContain(
      "intentId = :intentId AND #state = :ready AND authorityGeneration = :authorityGeneration AND ledgerGeneration = :ledgerGeneration",
    );
  });

  it("fences the retirement intent on authority, ledger, and cycle lock before the record write", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();
    const record: TemporalRetirementRecord = {
      kind: "RETIREMENT",
      serviceArn:
        "arn:aws:ecs:us-west-2:123456789012:service/cluster/drained-service",
      clusterArn: "cluster",
      poolId: "parent",
      buildId: "build-1",
      intentId: "retire-1",
      cycleId: "cycle-1",
      authorityGeneration: 7,
      ledgerGeneration: 9,
      priorDesiredCount: 3,
      state: "ZEROING",
      createdAt: now,
      verifyDeadline: now + 10 * 60_000,
    };

    await store.putRetirementIntent({ record, now });

    const input = (commands[0] as TransactWriteItemsCommand).input;
    expect(input.TransactItems).toHaveLength(4);
    const authorityCheck = input.TransactItems?.[0]?.ConditionCheck;
    const ledgerCheck = input.TransactItems?.[1]?.ConditionCheck;
    const reconcilerCheck = input.TransactItems?.[2]?.ConditionCheck;
    const recordPut = input.TransactItems?.[3]?.Put;
    expect(authorityCheck?.ConditionExpression).toBe(
      "generation = :generation AND writerKind = :writer",
    );
    expect(unmarshall(authorityCheck?.ExpressionAttributeValues ?? {})).toEqual(
      { ":generation": 7, ":writer": "STEP_FUNCTIONS_LAMBDA" },
    );
    expect(ledgerCheck?.ConditionExpression).toBe("generation = :generation");
    expect(unmarshall(ledgerCheck?.ExpressionAttributeValues ?? {})).toEqual({
      ":generation": 9,
    });
    expect(reconcilerCheck?.ConditionExpression).toBe(
      "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
    );
    expect(recordPut?.TableName).toBe(config.drainTableName);
    const item = unmarshall(recordPut?.Item ?? {});
    expect(item.SK).toBe("RETIREMENT#drained-service");
    expect(item.state).toBe("ZEROING");
    expect(item.priorDesiredCount).toBe(3);
    // Terminal records are overwritable so a re-selected service can mint a
    // fresh intent; an active ZEROING record is not.
    expect(recordPut?.ConditionExpression).toBe(
      "attribute_not_exists(PK) OR #state IN (:applied, :failed)",
    );
  });

  it("releases a retired service's whole allocation in one clamped ledger transaction", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();
    const record: TemporalRetirementRecord = {
      kind: "RETIREMENT",
      serviceArn:
        "arn:aws:ecs:us-west-2:123456789012:service/cluster/drained-service",
      clusterArn: "cluster",
      poolId: "parent",
      buildId: "build-1",
      intentId: "retire-1",
      cycleId: "cycle-1",
      authorityGeneration: 7,
      ledgerGeneration: 9,
      priorDesiredCount: 3,
      state: "ZEROING",
      createdAt: now,
      verifyDeadline: now + 10 * 60_000,
    };

    const released = await store.claimRetirementRelease({
      record,
      cycleId: "cycle-2",
      authorityGeneration: 7,
      ledger: {
        generation: 9,
        managedCommittedVcpu: 4,
        activeReservationVcpu: 0,
        allocations: { [record.serviceArn]: 6, other: 2 },
        inventoryHash: "inventory",
        updatedAt: now,
      },
      // More vCPU than the ledger still carries: the committed write must
      // clamp at zero instead of failing an arithmetic condition forever.
      releasedVcpu: 6,
      now,
    });

    const input = (commands[0] as TransactWriteItemsCommand).input;
    expect(input.TransactItems).toHaveLength(4);
    const ledgerUpdate = input.TransactItems?.[1]?.Update;
    expect(ledgerUpdate?.ConditionExpression).toBe("generation = :generation");
    const ledgerValues = unmarshall(
      ledgerUpdate?.ExpressionAttributeValues ?? {},
    );
    expect(ledgerValues[":nextGeneration"]).toBe(10);
    expect(ledgerValues[":managedCommittedVcpu"]).toBe(0);
    expect(ledgerValues[":allocations"]).toEqual({
      [record.serviceArn]: 0,
      other: 2,
    });
    const recordUpdate = input.TransactItems?.[3]?.Update;
    expect(recordUpdate?.TableName).toBe(config.drainTableName);
    expect(recordUpdate?.ConditionExpression).toBe(
      "intentId = :intentId AND #state = :zeroing",
    );
    expect(recordUpdate?.UpdateExpression).toContain(
      "releasedLedgerGeneration = :nextGeneration",
    );
    expect(released.ledger).toMatchObject({
      generation: 10,
      managedCommittedVcpu: 0,
      allocations: { [record.serviceArn]: 0, other: 2 },
    });
  });

  it("completes a retirement only from ZEROING and records the typed reason", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();
    const record: TemporalRetirementRecord = {
      kind: "RETIREMENT",
      serviceArn:
        "arn:aws:ecs:us-west-2:123456789012:service/cluster/drained-service",
      clusterArn: "cluster",
      poolId: "parent",
      buildId: "build-1",
      intentId: "retire-1",
      cycleId: "cycle-1",
      authorityGeneration: 7,
      ledgerGeneration: 9,
      priorDesiredCount: 3,
      state: "ZEROING",
      createdAt: now,
      verifyDeadline: now - 1_000,
    };

    await store.completeRetirement({
      record,
      terminalState: "FAILED",
      reason: "RETIREMENT_RUNNING_NOT_STOPPED",
      now,
    });

    const command = commands[0] as UpdateItemCommand;
    expect(command).toBeInstanceOf(UpdateItemCommand);
    expect(command.input.TableName).toBe(config.drainTableName);
    expect(unmarshall(command.input.Key ?? {}).SK).toBe(
      "RETIREMENT#drained-service",
    );
    expect(command.input.ConditionExpression).toBe(
      "intentId = :intentId AND #state = :zeroing",
    );
    expect(
      unmarshall(command.input.ExpressionAttributeValues ?? {})[":reason"],
    ).toBe("RETIREMENT_RUNNING_NOT_STOPPED");
  });

  it("asserts the cycle lock alongside the ledger generation on the ready-drain fence", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();
    const drain: TemporalDrainRecord = {
      kind: "SCALE_IN",
      taskArn: "arn:aws:ecs:us-west-2:123456789012:task/cluster/task-1",
      clusterArn: "cluster",
      serviceArn: "service",
      poolId: "parent",
      buildId: "build-1",
      intentId: "intent-1",
      cycleId: "cycle-1",
      authorityGeneration: 5,
      ledgerGeneration: 6,
      priorDesiredCount: 3,
      targetDesiredCount: 2,
      state: "READY",
      createdAt: now,
      deadline: now + 60_000,
    };

    await store.refreshReadyDrainFence({
      drain,
      cycleId: "cycle-2",
      authorityGeneration: 7,
      ledgerGeneration: 9,
      now,
    });

    const input = (commands[0] as TransactWriteItemsCommand).input;
    const items = input.TransactItems ?? [];
    const bySortKey = (sortKey: string) =>
      items.find(
        (item) =>
          item.ConditionCheck &&
          unmarshall(item.ConditionCheck.Key ?? {}).SK === sortKey,
      )?.ConditionCheck;
    // The ledger-generation guard is still asserted...
    const ledgerCheck = bySortKey("CAPACITY_LEDGER");
    expect(ledgerCheck?.ConditionExpression).toBe("generation = :generation");
    expect(unmarshall(ledgerCheck?.ExpressionAttributeValues ?? {})).toEqual({
      ":generation": 9,
    });
    // ...and, like claimProtectedDecrease / claimMaintenanceDrain, the fence now
    // also asserts this cycle still holds the reconciler lock so a lock-lost
    // cycle fails the transaction cleanly instead of fencing a stale drain.
    const reconcilerCheck = bySortKey("RECONCILER");
    expect(reconcilerCheck?.ConditionExpression).toBe(
      "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
    );
    expect(
      unmarshall(reconcilerCheck?.ExpressionAttributeValues ?? {}),
    ).toEqual({
      ":cycleId": "cycle-2",
      ":authorityGeneration": 7,
      ":now": now,
    });
  });
});

describe("chain record forward-claim", () => {
  it("fences the advance on the record's exact identity and never regresses the generation", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();

    const generation = await store.advanceChainPastStaleRecord({
      expectedGeneration: 5,
      expectedExecutionArn: "arn:execution/capacity-5",
      executionArn: "arn:execution/capacity-4",
      now,
    });

    expect(generation).toBe(6);
    const command = commands[0] as UpdateItemCommand;
    expect(command).toBeInstanceOf(UpdateItemCommand);
    expect(unmarshall(command.input.Key ?? {}).SK).toBe("CHAIN");
    expect(command.input.ConditionExpression).toBe(
      "generation = :expectedGeneration AND executionArn = :expectedExecutionArn",
    );
    expect(unmarshall(command.input.ExpressionAttributeValues ?? {})).toEqual({
      ":expectedGeneration": 5,
      ":expectedExecutionArn": "arn:execution/capacity-5",
      ":generation": 6,
      ":executionArn": "arn:execution/capacity-4",
      ":now": now,
    });
  });
});

describe("dynamo failure classification", () => {
  const cancellation = (...codes: string[]) =>
    new TransactionCanceledException({
      $metadata: {},
      message: codes.join(","),
      CancellationReasons: codes.map((Code) => ({ Code })),
    });

  it("treats a pure ConditionalCheckFailed cancellation as benign contention", () => {
    expect(
      isConditionalCheckContention(
        cancellation("None", "ConditionalCheckFailed", "None", "None"),
      ),
    ).toBe(true);
    expect(
      isConditionalCheckContention(
        new ConditionalCheckFailedException({
          $metadata: {},
          message: "condition failed",
        }),
      ),
    ).toBe(true);
  });

  it("rejects a cancellation that mixes ConditionalCheckFailed with a genuine failure code", () => {
    expect(
      isConditionalCheckContention(
        cancellation("None", "ConditionalCheckFailed", "ValidationError"),
      ),
    ).toBe(false);
  });

  it("rejects a cancellation with no ConditionalCheckFailed reason and non-Dynamo errors", () => {
    expect(
      isConditionalCheckContention(cancellation("None", "ThrottlingError")),
    ).toBe(false);
    expect(isConditionalCheckContention(cancellation())).toBe(false);
    expect(isConditionalCheckContention(new Error("boom"))).toBe(false);
    expect(isConditionalCheckContention(undefined)).toBe(false);
  });

  it("treats conditional, conflict, and throttle-only failures as retriable contention", () => {
    expect(
      isRetriableDynamoContention(
        cancellation("None", "ConditionalCheckFailed"),
      ),
    ).toBe(true);
    expect(
      isRetriableDynamoContention(cancellation("None", "TransactionConflict")),
    ).toBe(true);
    expect(
      isRetriableDynamoContention(
        new TransactionConflictException({
          $metadata: {},
          message: "Transaction is ongoing for the item",
        }),
      ),
    ).toBe(true);
  });

  it("does not treat validation or systemic failures as retriable contention", () => {
    expect(
      isRetriableDynamoContention(cancellation("None", "ValidationError")),
    ).toBe(false);
    expect(
      isRetriableDynamoContention(
        cancellation("ConditionalCheckFailed", "ValidationError"),
      ),
    ).toBe(false);
    expect(isRetriableDynamoContention(new Error("table not found"))).toBe(
      false,
    );
  });
});

describe("packed reconciler maps", () => {
  const serviceTimes: NonNullable<ReconcilerState["serviceTimes"]> = {
    "dev#capy-temporal-worker-dev#build-1#jam-run#activity": {
      ewmaSeconds: 1.5,
      sampleCount: 4,
      activeSlots: 2,
      updatedAt: 7_000,
    },
  };
  const scaleIn: NonNullable<ReconcilerState["scaleIn"]> = {
    "arn:service/parent": { eligibleSince: 3_000, waveDecrements: 1 },
  };

  it("round-trips the cycle maps through the packed encoding", () => {
    const packed = packReconcilerMaps({ serviceTimes, scaleIn });
    expect(packed.byteLength).toBeLessThanOrEqual(
      MAX_PACKED_RECONCILER_MAPS_BYTES,
    );
    expect(unpackReconcilerMaps(packed)).toEqual({ serviceTimes, scaleIn });
  });

  it("sheds serviceTimes history at the byte cap but keeps scale-in state", () => {
    // Incompressible keys so gzip cannot rescue the oversized map.
    const oversized: NonNullable<ReconcilerState["serviceTimes"]> = {};
    for (let index = 0; index < 6_000; index += 1) {
      oversized[`${randomBytes(48).toString("hex")}#${index}`] = {
        ewmaSeconds: index * 0.5,
        sampleCount: index,
        activeSlots: 1,
        updatedAt: index,
      };
    }
    const packed = packReconcilerMaps({ serviceTimes: oversized, scaleIn });
    expect(packed.byteLength).toBeLessThanOrEqual(
      MAX_PACKED_RECONCILER_MAPS_BYTES,
    );
    expect(unpackReconcilerMaps(packed)).toEqual({ serviceTimes: {}, scaleIn });
  });

  it("persists cycle completion maps as one packed attribute and clears the legacy maps", async () => {
    let captured: TransactWriteItemsCommand | undefined;
    const client = {
      async send(command: unknown) {
        if (command instanceof TransactWriteItemsCommand) {
          captured = command;
        }
        return {};
      },
    } as DynamoDBClient;

    const audit = { cycleId: "cycle-1", demands: [{ replicas: 2 }] };
    await new CapacityStateStore(config, client).completeCycle({
      cycleId: "cycle-1",
      authorityGeneration: 1,
      result: "APPLIED",
      inputHash: "hash",
      audit,
      serviceTimes,
      scaleIn,
      ungrantedProdReplicas: 0,
      staleTemporalInputs: 0,
      now: 1_000,
    });

    const update = captured?.input.TransactItems?.[0]?.Update;
    expect(update?.UpdateExpression).toContain("packedMaps = :packedMaps");
    expect(update?.UpdateExpression).toContain(
      "REMOVE lockExpiresAt, serviceTimes, scaleIn",
    );
    const values = unmarshall(update?.ExpressionAttributeValues ?? {}) as {
      ":packedMaps": Uint8Array;
    };
    expect(unpackReconcilerMaps(values[":packedMaps"])).toEqual({
      serviceTimes,
      scaleIn,
    });
    const journal = unmarshall(
      captured?.input.TransactItems?.[1]?.Put?.Item ?? {},
    ) as { audit?: unknown; packedAudit: Uint8Array };
    expect(journal.audit).toBeUndefined();
    expect(unpackCycleAudit(journal.packedAudit)).toEqual(audit);
  });

  it("truncates an audit journal that would breach the item cap on its own", () => {
    const audit: Record<string, unknown> = {
      cycleId: "cycle-1",
      error: "boom",
      observations: Array.from({ length: 9_000 }, () =>
        randomBytes(48).toString("hex"),
      ),
    };
    const packed = packCycleAudit(audit);
    expect(packed.byteLength).toBeLessThanOrEqual(MAX_PACKED_CYCLE_AUDIT_BYTES);
    expect(unpackCycleAudit(packed)).toEqual({
      truncated: true,
      auditKeys: ["cycleId", "error", "observations"],
      cycleId: "cycle-1",
      error: "boom",
    });
  });

  it("hydrates reconciler maps from the packed attribute", async () => {
    const client = {
      async send(command: unknown) {
        if (!(command instanceof TransactGetItemsCommand)) return {};
        return {
          Responses: [
            {
              Item: marshall({
                generation: 1,
                writerKind: "STEP_FUNCTIONS_LAMBDA",
                transitionId: "test",
                effectiveAt: 1,
                checksum: "test",
              }),
            },
            {
              Item: marshall({
                generation: 1,
                managedCommittedVcpu: 0,
                activeReservationVcpu: 0,
                allocations: {},
                inventoryHash: "inventory",
                updatedAt: 1,
              }),
            },
            {
              Item: marshall({
                capability: "PROTECTED_SCALE_IN",
                packedMaps: packReconcilerMaps({ serviceTimes, scaleIn }),
              }),
            },
          ],
        };
      },
    } as DynamoDBClient;

    const snapshot = await new CapacityStateStore(
      config,
      client,
    ).readControlSnapshot();
    expect(snapshot.reconciler.serviceTimes).toEqual(serviceTimes);
    expect(snapshot.reconciler.scaleIn).toEqual(scaleIn);
    expect(snapshot.reconciler).not.toHaveProperty("packedMaps");
  });

  it("hydrates legacy reconciler items that still carry plain maps", async () => {
    const client = {
      async send(command: unknown) {
        if (!(command instanceof TransactGetItemsCommand)) return {};
        return {
          Responses: [
            {
              Item: marshall({
                generation: 1,
                writerKind: "STEP_FUNCTIONS_LAMBDA",
                transitionId: "test",
                effectiveAt: 1,
                checksum: "test",
              }),
            },
            {
              Item: marshall({
                generation: 1,
                managedCommittedVcpu: 0,
                activeReservationVcpu: 0,
                allocations: {},
                inventoryHash: "inventory",
                updatedAt: 1,
              }),
            },
            {
              Item: marshall({
                capability: "PROTECTED_SCALE_IN",
                serviceTimes,
                scaleIn,
              }),
            },
          ],
        };
      },
    } as DynamoDBClient;

    const snapshot = await new CapacityStateStore(
      config,
      client,
    ).readControlSnapshot();
    expect(snapshot.reconciler.serviceTimes).toEqual(serviceTimes);
    expect(snapshot.reconciler.scaleIn).toEqual(scaleIn);
  });
});
