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
        retiringBuilds: { builds: {} },
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
        retiringBuilds: { builds: {} },
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

  // ─── Retirement v2 marker op fences (design doc 2026-07-18 §4.2) ───

  const markerArn =
    "arn:aws:ecs:us-west-2:123456789012:service/cluster/capy-temporal-worker-dev-parent-service-aaaaaaaaaaaa";

  it("begins a marker under the authority fence, admitting only absent/terminal/expired/own entries", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();

    await store.beginRetirement({
      authorityGeneration: 7,
      buildId: "build-1",
      entry: {
        intentId: "run-1",
        deploymentName: "capy-temporal-worker-dev",
        environment: "dev",
        services: { [markerArn]: { priorDesired: 3 } },
        expiresAt: now + 45 * 60_000,
      },
      pruneBuildIds: ["build-terminal"],
      now,
    });

    const input = (commands[0] as TransactWriteItemsCommand).input;
    expect(input.TransactItems).toHaveLength(2);
    const authorityCheck = input.TransactItems?.[0]?.ConditionCheck;
    expect(authorityCheck?.ConditionExpression).toBe(
      "generation = :generation AND writerKind = :writer",
    );
    expect(unmarshall(authorityCheck?.ExpressionAttributeValues ?? {})).toEqual(
      { ":generation": 7, ":writer": "STEP_FUNCTIONS_LAMBDA" },
    );
    const markerUpdate = input.TransactItems?.[1]?.Update;
    expect(unmarshall(markerUpdate?.Key ?? {}).SK).toBe("RETIRING_BUILDS");
    // Begin guard: absent | terminal | expired | same intentId — a FOREIGN
    // live intentId fails and the caller defers the build (overlap fence).
    expect(markerUpdate?.ConditionExpression).toContain(
      "attribute_not_exists(builds.#build) OR builds.#build.#state IN (:buried, :aborted) OR builds.#build.expiresAt < :now OR builds.#build.intentId = :mine",
    );
    // Terminal-sibling prune is guarded: the pruned entry must still be
    // non-OPEN in the same transaction (no clobbering a concurrent re-begin).
    expect(markerUpdate?.ConditionExpression).toContain(
      "attribute_not_exists(builds.#prune0) OR builds.#prune0.#state <> :open",
    );
    expect(markerUpdate?.UpdateExpression).toContain("REMOVE builds.#prune0");
    expect(markerUpdate?.ExpressionAttributeNames?.["#prune0"]).toBe(
      "build-terminal",
    );
    const values = unmarshall(markerUpdate?.ExpressionAttributeValues ?? {});
    expect(values[":entry"]).toMatchObject({
      intentId: "run-1",
      state: "OPEN",
      createdAt: now,
      services: { [markerArn]: { priorDesired: 3 } },
    });
  });

  it("never prunes the build being begun and omits :open when nothing is pruned", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();

    await store.beginRetirement({
      authorityGeneration: 7,
      buildId: "build-1",
      entry: {
        intentId: "run-1",
        deploymentName: "capy-temporal-worker-dev",
        environment: "dev",
        services: { [markerArn]: { priorDesired: 3 } },
        expiresAt: now + 45 * 60_000,
      },
      // The build's own (terminal) entry must not be REMOVEd while being SET.
      pruneBuildIds: ["build-1"],
      now,
    });

    const markerUpdate = (commands[0] as TransactWriteItemsCommand).input
      .TransactItems?.[1]?.Update;
    expect(markerUpdate?.UpdateExpression).not.toContain("REMOVE");
    // DynamoDB rejects unused ExpressionAttributeValues.
    expect(
      unmarshall(markerUpdate?.ExpressionAttributeValues ?? {})[":open"],
    ).toBeUndefined();
  });

  it("releases the ledger, deletes the grant, and stamps the marker in one generation-fenced transaction", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();

    const nextGeneration = await store.releaseRetirementLedger({
      authorityGeneration: 7,
      ledger: {
        generation: 9,
        managedCommittedVcpu: 4,
        activeReservationVcpu: 0,
        allocations: { [markerArn]: 6, other: 2 },
        grants: {
          [markerArn]: { vcpu: 6, expiresAt: now + 60_000 },
          other: { vcpu: 2, expiresAt: now + 60_000 },
        },
        inventoryHash: "inventory",
        updatedAt: now,
      },
      buildId: "build-1",
      intentId: "run-1",
      serviceArn: markerArn,
      // More vCPU than the ledger still carries: the committed write must
      // clamp at zero client-side instead of failing an arithmetic
      // condition forever (the R1 no-brick posture, unchanged).
      releasedVcpu: 6,
      now,
    });

    expect(nextGeneration).toBe(10);
    const input = (commands[0] as TransactWriteItemsCommand).input;
    expect(input.TransactItems).toHaveLength(3);
    const authorityCheck = input.TransactItems?.[0]?.ConditionCheck;
    expect(authorityCheck?.ConditionExpression).toBe(
      "generation = :generation AND writerKind = :writer",
    );
    const ledgerUpdate = input.TransactItems?.[1]?.Update;
    expect(ledgerUpdate?.ConditionExpression).toBe("generation = :generation");
    const ledgerValues = unmarshall(
      ledgerUpdate?.ExpressionAttributeValues ?? {},
    );
    expect(ledgerValues[":nextGeneration"]).toBe(10);
    expect(ledgerValues[":managedCommittedVcpu"]).toBe(0);
    // Phase A dual-book (§3.2): allocations clamp AND grant delete land in
    // the same generation-fenced write — both books conserved throughout.
    expect(ledgerValues[":allocations"]).toEqual({
      [markerArn]: 0,
      other: 2,
    });
    expect(ledgerValues[":grants"]).toEqual({
      other: { vcpu: 2, expiresAt: now + 60_000 },
    });
    // NO RECONCILER cycle fence: data-plane ops write outside cycles
    // (admitReservation precedent).
    const sortKeys = (input.TransactItems ?? []).map(
      (item) =>
        unmarshall(
          item.ConditionCheck?.Key ?? item.Update?.Key ?? item.Put?.Item ?? {},
        ).SK,
    );
    expect(sortKeys).not.toContain("RECONCILER");
    const markerUpdate = input.TransactItems?.[2]?.Update;
    expect(unmarshall(markerUpdate?.Key ?? {}).SK).toBe("RETIRING_BUILDS");
    expect(markerUpdate?.ConditionExpression).toBe(
      "builds.#build.#state = :open AND builds.#build.intentId = :mine AND attribute_not_exists(builds.#build.services.#arn.releasedLedgerGeneration)",
    );
    expect(markerUpdate?.UpdateExpression).toContain(
      "builds.#build.services.#arn.releasedLedgerGeneration = :nextGeneration",
    );
    expect(markerUpdate?.ExpressionAttributeNames?.["#arn"]).toBe(markerArn);
  });

  it("aborts only an OPEN entry it owns, recording the typed reason", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();

    await store.abortRetirement({
      authorityGeneration: 7,
      buildId: "build-1",
      intentId: "run-1",
      reason: "BUILD_STATE_REGRESSED",
      now,
    });

    const input = (commands[0] as TransactWriteItemsCommand).input;
    expect(input.TransactItems).toHaveLength(2);
    const markerUpdate = input.TransactItems?.[1]?.Update;
    expect(markerUpdate?.ConditionExpression).toBe(
      "builds.#build.#state = :open AND builds.#build.intentId = :mine",
    );
    const values = unmarshall(markerUpdate?.ExpressionAttributeValues ?? {});
    expect(values[":aborted"]).toBe("ABORTED");
    // I5: ABORTED without a typed reason is unrepresentable at the write.
    expect(values[":reason"]).toBe("BUILD_STATE_REGRESSED");
  });

  it("closes only an OPEN entry it owns and prunes terminal siblings under guard", async () => {
    const commands: unknown[] = [];
    const client = {
      async send(command: unknown) {
        commands.push(command);
        return {};
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);
    const now = Date.now();

    await store.closeRetirement({
      authorityGeneration: 7,
      buildId: "build-1",
      intentId: "run-1",
      pruneBuildIds: ["build-old"],
      now,
    });

    const input = (commands[0] as TransactWriteItemsCommand).input;
    const markerUpdate = input.TransactItems?.[1]?.Update;
    expect(markerUpdate?.ConditionExpression).toContain(
      "builds.#build.#state = :open AND builds.#build.intentId = :mine",
    );
    expect(markerUpdate?.ConditionExpression).toContain(
      "attribute_not_exists(builds.#prune0) OR builds.#prune0.#state <> :open",
    );
    expect(markerUpdate?.UpdateExpression).toContain(
      "builds.#build.#state = :buried",
    );
    expect(markerUpdate?.UpdateExpression).toContain("REMOVE builds.#prune0");
  });

  it("reads retiring-builds markers atomically with the control snapshot, tolerating absence", async () => {
    const authorityItem = marshall({
      generation: 3,
      writerKind: "STEP_FUNCTIONS_LAMBDA",
      transitionId: "t",
      effectiveAt: 1,
      checksum: "c",
    });
    const ledgerItem = marshall({
      generation: 5,
      managedCommittedVcpu: 0,
      activeReservationVcpu: 0,
      allocations: {},
      inventoryHash: "i",
      updatedAt: 1,
    });
    const reconcilerItem = marshall({ capability: "PROTECTED_SCALE_IN" });
    let requestedKeys: string[] = [];
    const client = {
      async send(command: unknown) {
        const transactGet = command as TransactGetItemsCommand;
        requestedKeys = (transactGet.input.TransactItems ?? []).map(
          (item) => unmarshall(item.Get?.Key ?? {}).SK as string,
        );
        return {
          Responses: [
            { Item: authorityItem },
            { Item: ledgerItem },
            { Item: reconcilerItem },
            // RETIRING_BUILDS absent (pre-marker environment): must not fail.
            {},
          ],
        };
      },
    } as DynamoDBClient;
    const store = new CapacityStateStore(config, client);

    const snapshot = await store.readControlSnapshot();

    expect(requestedKeys).toEqual([
      "WRITER_AUTHORITY",
      "CAPACITY_LEDGER",
      "RECONCILER",
      "RETIRING_BUILDS",
    ]);
    expect(snapshot.retiringBuilds).toEqual({ builds: {} });
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
