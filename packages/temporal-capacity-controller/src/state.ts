import {
  ConditionalCheckFailedException,
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
  TransactGetItemsCommand,
  TransactWriteItemsCommand,
  TransactionCanceledException,
  TransactionConflictException,
  UpdateItemCommand,
  type AttributeValue,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { createHash } from "node:crypto";
import { gunzipSync, gzipSync } from "node:zlib";
import {
  getTemporalDrainSortKey,
  getTemporalMaintenanceSortKey,
  getTemporalRetirementSortKey,
  type TemporalRetirementRecord,
} from "@capy/shared/temporal/capacity";

import type {
  CapacityLedger,
  CapacityReservation,
  ControllerConfig,
  MaintenanceRedeploy,
  ReconcilerState,
  ReservationCapacityState,
  ReservationState,
  TemporalDrainRecord,
  WriterAuthority,
} from "./types.js";

type ControlSnapshot = {
  authority: WriterAuthority;
  ledger: CapacityLedger;
  reconciler: ReconcilerState;
};

const encode = (value: Record<string, unknown>) =>
  marshall(value, { removeUndefinedValues: true });

const itemKey = (config: ControllerConfig, sortKey: string) =>
  encode({ PK: config.controlPartitionKey, SK: sortKey });

const decodeItem = <Value>(item: Record<string, AttributeValue> | undefined) =>
  item ? (unmarshall(item) as Value) : undefined;

// The serviceTimes/scaleIn maps are the only RECONCILER payload that grows
// with deploy churn: serviceTimes keys embed buildIds (dev redeploys mint new
// ones all day) and scaleIn keys are per-deploy ECS service ARNs. Stored as
// plain maps they pushed the item past DynamoDB's 400KB cap and failed every
// completeCycle write (2026-07-14 incident: 278KB scaleIn + 136KB
// serviceTimes). They are private to this store — nothing queries individual
// map keys — so persist them as one gzipped Binary attribute instead. The JSON
// is dominated by long repeated key prefixes and compresses ~10x, and the cap
// below hard-bounds the attribute: rather than ever failing the write again,
// packing sheds the advisory serviceTimes history first (EWMA smoothing
// rebuilds within ~3 cycles) and the scale-in wave state only as a last
// resort (scale-in eligibility timers restart, which merely delays scale-in).
export const RECONCILER_PACKED_MAPS_ATTRIBUTE = "packedMaps";
export const MAX_PACKED_RECONCILER_MAPS_BYTES = 128 * 1024;

type ReconcilerMaps = {
  serviceTimes: NonNullable<ReconcilerState["serviceTimes"]>;
  scaleIn: NonNullable<ReconcilerState["scaleIn"]>;
};

export const packReconcilerMaps = (maps: ReconcilerMaps): Uint8Array => {
  const candidates: ReconcilerMaps[] = [
    maps,
    { serviceTimes: {}, scaleIn: maps.scaleIn },
    { serviceTimes: {}, scaleIn: {} },
  ];
  for (const candidate of candidates) {
    const packed = gzipSync(JSON.stringify(candidate));
    if (packed.byteLength <= MAX_PACKED_RECONCILER_MAPS_BYTES) return packed;
  }
  // Unreachable: the final candidate packs two empty maps.
  throw new Error("Reconciler maps cannot be packed within the byte cap");
};

export const unpackReconcilerMaps = (packed: Uint8Array): ReconcilerMaps =>
  JSON.parse(gunzipSync(packed).toString("utf8")) as ReconcilerMaps;

// The per-cycle CYCLE# journal embeds the full demand/allocation/observation
// picture, which scales with fleet size — at ~270 live worker services the
// plain-map item intermittently breached the 400KB cap on its own and failed
// the same completeCycle transaction (the second failure mode of the
// 2026-07-14 incident). Nothing reads these journal items programmatically
// (they are TTL'd operator forensics), so pack them the same way, and degrade
// to a truncation stub rather than ever failing the cycle over a journal
// entry. To inspect one by hand: base64-decode the Binary and gunzip.
export const MAX_PACKED_CYCLE_AUDIT_BYTES = 256 * 1024;

export const packCycleAudit = (audit: Record<string, unknown>): Uint8Array => {
  const packed = gzipSync(JSON.stringify(audit));
  if (packed.byteLength <= MAX_PACKED_CYCLE_AUDIT_BYTES) return packed;
  return gzipSync(
    JSON.stringify({
      truncated: true,
      auditKeys: Object.keys(audit),
      cycleId: audit.cycleId,
      error: audit.error,
    }),
  );
};

export const unpackCycleAudit = (packed: Uint8Array): Record<string, unknown> =>
  JSON.parse(gunzipSync(packed).toString("utf8")) as Record<string, unknown>;

// Items written before the packed attribute existed carry the maps as plain
// top-level attributes; hydrate either shape so a deploy (or rollback) across
// the format change never loses state it can still read.
const decodeReconciler = (
  item: Record<string, AttributeValue> | undefined,
): ReconcilerState | undefined => {
  const raw = decodeItem<ReconcilerState & { packedMaps?: Uint8Array }>(item);
  if (!raw) return undefined;
  const { packedMaps, ...reconciler } = raw;
  if (packedMaps === undefined) return reconciler;
  const maps = unpackReconcilerMaps(packedMaps);
  return {
    ...reconciler,
    serviceTimes: maps.serviceTimes,
    scaleIn: maps.scaleIn,
  };
};

// Precisely classifies a DynamoDB write failure as *benign optimistic-
// concurrency contention* — a condition we asserted no longer held — as
// distinct from a genuine systemic failure (malformed request, missing table,
// throttling, denied credentials) that must never be swallowed.
//
// For a single-item conditional write this surfaces as
// ConditionalCheckFailedException. For a TransactWriteItems (how the fence and
// its sibling claims write) a failed condition instead arrives as
// TransactionCanceledException carrying a CancellationReasons array, one entry
// per transaction item, whose Code is "ConditionalCheckFailed" for the item(s)
// whose condition failed and "None" for the items that were fine. Any other
// reason code (ValidationError, ThrottlingError, ResourceNotFound,
// ProvisionedThroughputExceeded, TransactionConflict, …) means the transaction
// failed for a reason we must not treat as a benign lost race — so a mixed
// cancellation that contains even one non-CCF, non-None code is rejected here.
export const isConditionalCheckContention = (error: unknown): boolean => {
  if (error instanceof ConditionalCheckFailedException) return true;
  if (error instanceof TransactionCanceledException) {
    const reasons = error.CancellationReasons;
    if (!reasons || reasons.length === 0) return false;
    let sawConditionalCheckFailed = false;
    for (const reason of reasons) {
      const code = reason.Code;
      if (code === "ConditionalCheckFailed") {
        sawConditionalCheckFailed = true;
        continue;
      }
      // "None" flags an item that did not cause the cancellation; anything else
      // is a genuine failure code that must propagate.
      if (code && code !== "None") return false;
    }
    return sawConditionalCheckFailed;
  }
  return false;
};

// Broader allowlist for the reconcile-level per-drain isolation backstop: a
// single drain's *transient* DynamoDB failure (a lost optimistic-concurrency
// race, a concurrent transaction on the same item, or capacity throttling)
// must defer to the next cycle rather than abort the global dev+prod cycle.
// Everything outside this allowlist — validation errors, a missing table,
// denied credentials, or any non-DynamoDB bug — still fails loudly.
export const isRetriableDynamoContention = (error: unknown): boolean => {
  if (isConditionalCheckContention(error)) return true;
  // A concurrent transaction touched the same item(s); resolves on retry.
  if (error instanceof TransactionConflictException) return true;
  if (error instanceof TransactionCanceledException) {
    const reasons = error.CancellationReasons ?? [];
    // Tolerate a cancellation whose non-"None" reasons are exclusively
    // transient contention/capacity codes — never a validation/auth/resource
    // code, which isConditionalCheckContention already rejected above.
    return (
      reasons.some((reason) => reason.Code && reason.Code !== "None") &&
      reasons.every(
        (reason) =>
          !reason.Code ||
          reason.Code === "None" ||
          reason.Code === "ConditionalCheckFailed" ||
          reason.Code === "TransactionConflict" ||
          reason.Code === "ThrottlingError" ||
          reason.Code === "ProvisionedThroughputExceeded",
      )
    );
  }
  // Bare throttling / capacity exceptions raised outside a transaction.
  const name = error instanceof Error ? error.name : "";
  return (
    name === "ProvisionedThroughputExceededException" ||
    name === "ThrottlingException" ||
    name === "RequestLimitExceeded"
  );
};

export class CapacityStateStore {
  constructor(
    private readonly config: ControllerConfig,
    private readonly client = new DynamoDBClient({ region: config.region }),
  ) {}

  private async retryTransactionConflicts<Result>(
    operation: () => Promise<Result>,
  ): Promise<Result> {
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      try {
        return await operation();
      } catch (error) {
        if (!(error instanceof TransactionConflictException) || attempt === 5) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, attempt * 100));
      }
    }
    throw new Error("Transaction conflict retries exhausted");
  }

  async initialize() {
    const now = Date.now();
    await this.retryTransactionConflicts(async () => {
      const results = await Promise.allSettled([
        this.client.send(
          new UpdateItemCommand({
            TableName: this.config.tableName,
            Key: itemKey(this.config, "WRITER_AUTHORITY"),
            UpdateExpression:
              "SET generation = if_not_exists(generation, :one), writerKind = if_not_exists(writerKind, :writer), transitionId = if_not_exists(transitionId, :transition), effectiveAt = if_not_exists(effectiveAt, :now), checksum = if_not_exists(checksum, :checksum)",
            ExpressionAttributeValues: encode({
              ":one": 1,
              ":writer": "APPLICATION_AUTO_SCALING",
              ":transition": "bootstrap",
              ":now": now,
              ":checksum": "bootstrap",
            }),
          }),
        ),
        this.client.send(
          new UpdateItemCommand({
            TableName: this.config.tableName,
            Key: itemKey(this.config, "CAPACITY_LEDGER"),
            UpdateExpression:
              "SET generation = if_not_exists(generation, :one), managedCommittedVcpu = if_not_exists(managedCommittedVcpu, :zero), activeReservationVcpu = if_not_exists(activeReservationVcpu, :zero), allocations = if_not_exists(allocations, :allocations), inventoryHash = if_not_exists(inventoryHash, :inventory), updatedAt = if_not_exists(updatedAt, :now)",
            ExpressionAttributeValues: encode({
              ":one": 1,
              ":zero": 0,
              ":allocations": {},
              ":inventory": "bootstrap",
              ":now": now,
            }),
          }),
        ),
        this.client.send(
          new UpdateItemCommand({
            TableName: this.config.tableName,
            Key: itemKey(this.config, "CHAIN"),
            UpdateExpression:
              "SET generation = if_not_exists(generation, :zero), executionArn = if_not_exists(executionArn, :empty), updatedAt = if_not_exists(updatedAt, :now)",
            ExpressionAttributeValues: encode({
              ":zero": 0,
              ":empty": "",
              ":now": now,
            }),
          }),
        ),
      ]);
      const failures = results.flatMap((result) =>
        result.status === "rejected" ? [result.reason] : [],
      );
      const nonConflictFailure = failures.find(
        (error) => !(error instanceof TransactionConflictException),
      );
      if (nonConflictFailure) throw nonConflictFailure;
      if (failures[0]) throw failures[0];
    });
    try {
      await this.retryTransactionConflicts(() =>
        this.client.send(
          new UpdateItemCommand({
            TableName: this.config.tableName,
            Key: itemKey(this.config, "RECONCILER"),
            ConditionExpression:
              "attribute_not_exists(capability) OR capability = :priorCapability",
            UpdateExpression: "SET capability = :capability",
            ExpressionAttributeValues: encode({
              ":priorCapability": "INCREASE_ONLY",
              ":capability": "PROTECTED_SCALE_IN",
            }),
          }),
        ),
      );
    } catch (error) {
      if (!(error instanceof ConditionalCheckFailedException)) throw error;
    }
  }

  async readControlSnapshot(): Promise<ControlSnapshot> {
    const response = await this.client.send(
      new TransactGetItemsCommand({
        TransactItems: [
          {
            Get: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
            },
          },
          {
            Get: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
            },
          },
          {
            Get: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
            },
          },
        ],
      }),
    );
    const authority = decodeItem<WriterAuthority>(
      response.Responses?.[0]?.Item,
    );
    const ledger = decodeItem<CapacityLedger>(response.Responses?.[1]?.Item);
    const reconciler = decodeReconciler(response.Responses?.[2]?.Item);
    if (!authority || !ledger || !reconciler) {
      throw new Error("Capacity control state is incomplete");
    }
    return { authority, ledger, reconciler };
  }

  // Persisted by the reconcile loop each cycle so deploy-time reserve/release
  // can read the slow-moving capacity picture instead of scanning the full
  // account-wide Fargate inventory themselves. Latest-wins single item; no
  // conditional guard because the reconcile cycle lock already serializes
  // writers and a slightly older snapshot is harmless (callers re-read the
  // ledger transactionally and fall back to a live read when this is stale).
  async writeReservationSnapshot(params: ReservationCapacityState) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, "RESERVATION_SNAPSHOT"),
        // Alias attribute names: "snapshot"/"services"/"capturedAt" may collide
        // with DynamoDB reserved words, so reference them indirectly.
        UpdateExpression:
          "SET #snapshot = :snapshot, #services = :services, #capturedAt = :capturedAt",
        ExpressionAttributeNames: {
          "#snapshot": "snapshot",
          "#services": "services",
          "#capturedAt": "capturedAt",
        },
        ExpressionAttributeValues: encode({
          ":snapshot": params.snapshot,
          ":services": params.services,
          ":capturedAt": params.capturedAt,
        }),
      }),
    );
  }

  async readReservationSnapshot(): Promise<
    ReservationCapacityState | undefined
  > {
    const response = await this.client.send(
      new GetItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, "RESERVATION_SNAPSHOT"),
        ConsistentRead: true,
      }),
    );
    return decodeItem<ReservationCapacityState>(response.Item);
  }

  async claimCycle(params: {
    cycleId: string;
    authorityGeneration: number;
    ledgerGeneration: number;
    now: number;
  }) {
    const lockExpiresAt = params.now + this.config.cycleLockMs;
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression: "generation = :generation",
              ExpressionAttributeValues: encode({
                ":generation": params.authorityGeneration,
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression: "generation = :generation",
              ExpressionAttributeValues: encode({
                ":generation": params.ledgerGeneration,
              }),
            },
          },
          {
            Update: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "attribute_not_exists(lockExpiresAt) OR lockExpiresAt < :now OR cycleId = :cycleId",
              UpdateExpression:
                "SET cycleId = :cycleId, authorityGeneration = :authorityGeneration, capability = :capability, lockExpiresAt = :lockExpiresAt, lastStartedAt = :now",
              ExpressionAttributeValues: encode({
                ":cycleId": params.cycleId,
                ":authorityGeneration": params.authorityGeneration,
                ":capability": "PROTECTED_SCALE_IN",
                ":lockExpiresAt": lockExpiresAt,
                ":now": params.now,
              }),
            },
          },
        ],
      }),
    );
  }

  async claimCapacityPlan(params: {
    cycleId: string;
    authorityGeneration: number;
    ledger: CapacityLedger;
    additionalManagedVcpu: number;
    observedManagedVcpu: number;
    pendingLedgerVcpu: number;
    allocations: Record<string, number>;
    inventoryHash: string;
    now: number;
  }) {
    const nextGeneration = params.ledger.generation + 1;
    const nextManagedCommittedVcpu =
      params.observedManagedVcpu +
      params.pendingLedgerVcpu +
      params.additionalManagedVcpu;
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression:
                "generation = :generation AND writerKind = :writer",
              ExpressionAttributeValues: encode({
                ":generation": params.authorityGeneration,
                ":writer": "STEP_FUNCTIONS_LAMBDA",
              }),
            },
          },
          {
            Update: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression: "generation = :generation",
              UpdateExpression:
                "SET generation = :nextGeneration, managedCommittedVcpu = :managedCommittedVcpu, allocations = :allocations, inventoryHash = :inventoryHash, updatedAt = :now, pendingCycleId = :cycleId",
              ExpressionAttributeValues: encode({
                ":generation": params.ledger.generation,
                ":nextGeneration": nextGeneration,
                ":managedCommittedVcpu": nextManagedCommittedVcpu,
                ":allocations": params.allocations,
                ":inventoryHash": params.inventoryHash,
                ":now": params.now,
                ":cycleId": params.cycleId,
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
              ExpressionAttributeValues: encode({
                ":cycleId": params.cycleId,
                ":authorityGeneration": params.authorityGeneration,
                ":now": params.now,
              }),
            },
          },
        ],
      }),
    );
    return nextGeneration;
  }

  async verifyWriteFence(params: {
    cycleId: string;
    authorityGeneration: number;
    ledgerGeneration: number;
    now: number;
  }) {
    const snapshot = await this.readControlSnapshot();
    return (
      snapshot.authority.writerKind === "STEP_FUNCTIONS_LAMBDA" &&
      snapshot.authority.generation === params.authorityGeneration &&
      snapshot.ledger.generation === params.ledgerGeneration &&
      snapshot.reconciler.cycleId === params.cycleId &&
      (snapshot.reconciler.lockExpiresAt ?? 0) >= params.now
    );
  }

  async completeCycle(params: {
    cycleId: string;
    authorityGeneration: number;
    result: ReconcilerState["lastResult"];
    inputHash: string;
    audit: Record<string, unknown>;
    serviceTimes?: ReconcilerState["serviceTimes"];
    scaleIn?: ReconcilerState["scaleIn"];
    ungrantedProdReplicas: number;
    staleTemporalInputs: number;
    now: number;
  }) {
    const ttl = Math.floor(params.now / 1_000) + this.config.auditTtlSeconds;
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "cycleId = :cycleId AND authorityGeneration = :authorityGeneration",
              // REMOVE of the legacy plain-map attributes migrates a
              // pre-packed item in place on its first completed cycle; on an
              // already-migrated item the REMOVE is a no-op.
              UpdateExpression:
                "SET lastCompletedAt = :now, lastInputHash = :inputHash, lastResult = :result, lastUngrantedProdReplicas = :ungrantedProdReplicas, lastStaleTemporalInputs = :staleTemporalInputs, packedMaps = :packedMaps REMOVE lockExpiresAt, serviceTimes, scaleIn",
              ExpressionAttributeValues: encode({
                ":cycleId": params.cycleId,
                ":authorityGeneration": params.authorityGeneration,
                ":now": params.now,
                ":inputHash": params.inputHash,
                ":result": params.result,
                ":ungrantedProdReplicas": params.ungrantedProdReplicas,
                ":staleTemporalInputs": params.staleTemporalInputs,
                ":packedMaps": packReconcilerMaps({
                  serviceTimes: params.serviceTimes ?? {},
                  scaleIn: params.scaleIn ?? {},
                }),
              }),
            },
          },
          {
            Put: {
              TableName: this.config.tableName,
              Item: encode({
                PK: this.config.controlPartitionKey,
                SK: `CYCLE#${params.now}#${params.cycleId}`,
                cycleId: params.cycleId,
                authorityGeneration: params.authorityGeneration,
                result: params.result,
                inputHash: params.inputHash,
                packedAudit: packCycleAudit(params.audit),
                createdAt: params.now,
                ttl,
              }),
              ConditionExpression: "attribute_not_exists(PK)",
            },
          },
        ],
      }),
    );
  }

  async readChain() {
    const response = await this.client.send(
      new GetItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, "CHAIN"),
        ConsistentRead: true,
      }),
    );
    return decodeItem<{
      generation: number;
      executionArn: string;
      updatedAt: number;
    }>(response.Item);
  }

  async claimNextChain(params: {
    priorGeneration: number;
    executionArn: string;
    now: number;
  }) {
    const generation = params.priorGeneration + 1;
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, "CHAIN"),
        ConditionExpression: "generation = :priorGeneration",
        UpdateExpression:
          "SET generation = :generation, executionArn = :executionArn, updatedAt = :now",
        ExpressionAttributeValues: encode({
          ":priorGeneration": params.priorGeneration,
          ":generation": generation,
          ":executionArn": params.executionArn,
          ":now": params.now,
        }),
      }),
    );
    return generation;
  }

  async adoptRunningChain(params: {
    generation: number;
    executionArn: string;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, "CHAIN"),
        ConditionExpression:
          "generation <= :generation OR executionArn = :executionArn",
        UpdateExpression:
          "SET generation = :generation, executionArn = :executionArn, updatedAt = :now",
        ExpressionAttributeValues: encode({
          ":generation": params.generation,
          ":executionArn": params.executionArn,
          ":now": params.now,
        }),
      }),
    );
  }

  // Recovery for a chain record that outruns every running execution: its
  // recorded arn is dead and its generation EXCEEDS the live ones (deploy
  // transition with old-code stragglers), so adoptRunningChain's monotonic
  // condition (generation <= :generation) can never pass and ensure would
  // CCF-loop every minute forever. Re-point the record at the given
  // execution while advancing the generation PAST the stale value — never
  // regress — fenced on the record's exact current identity so a concurrent
  // ensure cannot double-advance.
  async advanceChainPastStaleRecord(params: {
    expectedGeneration: number;
    expectedExecutionArn: string;
    executionArn: string;
    now: number;
  }) {
    const generation = params.expectedGeneration + 1;
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, "CHAIN"),
        ConditionExpression:
          "generation = :expectedGeneration AND executionArn = :expectedExecutionArn",
        UpdateExpression:
          "SET generation = :generation, executionArn = :executionArn, updatedAt = :now",
        ExpressionAttributeValues: encode({
          ":expectedGeneration": params.expectedGeneration,
          ":expectedExecutionArn": params.expectedExecutionArn,
          ":generation": generation,
          ":executionArn": params.executionArn,
          ":now": params.now,
        }),
      }),
    );
    return generation;
  }

  async touchChain(params: {
    generation: number;
    executionArn: string;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, "CHAIN"),
        ConditionExpression:
          "generation = :generation AND executionArn = :executionArn",
        UpdateExpression: "SET updatedAt = :now",
        ExpressionAttributeValues: encode({
          ":generation": params.generation,
          ":executionArn": params.executionArn,
          ":now": params.now,
        }),
      }),
    );
  }

  async admitReservation(params: {
    reservation: Omit<
      CapacityReservation,
      "state" | "ledgerGeneration" | "serviceArns" | "consumedVcpu"
    >;
    expectedLedgerGeneration: number;
    expectedActiveReservationVcpu: number;
    now: number;
  }) {
    if (params.reservation.reservationId.length < 1) {
      throw new Error("Reservation reservationId must not be empty");
    }
    const nextGeneration = params.expectedLedgerGeneration + 1;
    const transactItems = [
      {
        Update: {
          TableName: this.config.tableName,
          Key: itemKey(this.config, "CAPACITY_LEDGER"),
          ConditionExpression:
            "generation = :generation AND activeReservationVcpu = :expectedActiveReservationVcpu",
          UpdateExpression:
            "SET generation = :nextGeneration, activeReservationVcpu = activeReservationVcpu + :requestedVcpu, updatedAt = :now",
          ExpressionAttributeValues: encode({
            ":generation": params.expectedLedgerGeneration,
            ":nextGeneration": nextGeneration,
            ":requestedVcpu": params.reservation.requestedVcpu,
            ":expectedActiveReservationVcpu":
              params.expectedActiveReservationVcpu,
            ":now": params.now,
          }),
        },
      },
      {
        Put: {
          TableName: this.config.tableName,
          Item: encode({
            PK: this.config.controlPartitionKey,
            SK: `RESERVATION#${params.reservation.reservationId}`,
            ...params.reservation,
            state: "ACTIVE" satisfies ReservationState,
            ledgerGeneration: nextGeneration,
            serviceArns: [],
            consumedVcpu: 0,
            createdAt: params.now,
            ttl:
              Math.floor(params.reservation.expiresAt / 1_000) +
              this.config.auditTtlSeconds,
          }),
          ConditionExpression: "attribute_not_exists(PK)",
        },
      },
    ];
    const clientRequestToken = createHash("sha256")
      .update(JSON.stringify(transactItems))
      .digest("hex")
      .slice(0, 36);
    await this.client.send(
      new TransactWriteItemsCommand({
        ClientRequestToken: clientRequestToken,
        TransactItems: transactItems,
      }),
    );
    return nextGeneration;
  }

  async readReservation(reservationId: string) {
    const response = await this.client.send(
      new GetItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, `RESERVATION#${reservationId}`),
        ConsistentRead: true,
      }),
    );
    return decodeItem<CapacityReservation>(response.Item);
  }

  async consumeReservation(params: {
    reservationId: string;
    ownerToken: string;
    serviceArn: string;
    additionalVcpu: number;
    now: number;
  }) {
    const reservation = await this.readReservation(params.reservationId);
    if (!reservation) {
      throw new Error(`Reservation ${params.reservationId} was not found`);
    }
    if (reservation.serviceArns.includes(params.serviceArn)) return reservation;
    const maxPriorConsumedVcpu =
      reservation.requestedVcpu - params.additionalVcpu;
    const response = await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, `RESERVATION#${params.reservationId}`),
        ConditionExpression:
          "ownerToken = :ownerToken AND #state = :active AND expiresAt > :now AND NOT contains(serviceArns, :serviceArn) AND consumedVcpu <= :maxPriorConsumedVcpu",
        UpdateExpression:
          "SET consumedVcpu = consumedVcpu + :additionalVcpu, serviceArns = list_append(serviceArns, :serviceArns), updatedAt = :now",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":ownerToken": params.ownerToken,
          ":active": "ACTIVE",
          ":now": params.now,
          ":serviceArn": params.serviceArn,
          ":serviceArns": [params.serviceArn],
          ":additionalVcpu": params.additionalVcpu,
          ":maxPriorConsumedVcpu": maxPriorConsumedVcpu,
        }),
        ReturnValues: "ALL_NEW",
      }),
    );
    const updated = decodeItem<CapacityReservation>(response.Attributes);
    if (!updated)
      throw new Error("Reservation consumption update returned no state");
    return updated;
  }

  async listOpenReservations() {
    const reservations: CapacityReservation[] = [];
    let exclusiveStartKey: Record<string, AttributeValue> | undefined;
    do {
      const response = await this.client.send(
        new QueryCommand({
          TableName: this.config.tableName,
          KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
          ExpressionAttributeValues: encode({
            ":pk": this.config.controlPartitionKey,
            ":prefix": "RESERVATION#",
          }),
          ExclusiveStartKey: exclusiveStartKey,
          ConsistentRead: true,
        }),
      );
      reservations.push(
        ...(response.Items ?? [])
          .map((item) => decodeItem<CapacityReservation>(item))
          .filter((item): item is CapacityReservation => Boolean(item))
          .filter(
            (item) => item.state === "ACTIVE" || item.state === "CONSUMING",
          ),
      );
      exclusiveStartKey = response.LastEvaluatedKey;
    } while (exclusiveStartKey);
    return reservations;
  }

  async expireReservations(now: number) {
    const reservations = await this.listOpenReservations();
    for (const reservation of reservations) {
      if (reservation.expiresAt > now) continue;
      for (let attempt = 1; attempt <= 5; attempt += 1) {
        const current = await this.readReservation(reservation.reservationId);
        if (
          !current ||
          current.state === "RELEASED" ||
          current.state === "EXPIRED"
        ) {
          break;
        }
        if (current.expiresAt > now) break;

        const snapshot = await this.readControlSnapshot();
        try {
          await this.releaseReservation({
            reservation: current,
            expectedLedgerGeneration: snapshot.ledger.generation,
            terminalState: "EXPIRED",
            now,
          });
          break;
        } catch (error) {
          if (!this.isConditionalFailure(error) || attempt === 5) {
            throw error;
          }
        }
      }
    }
  }

  async releaseReservation(params: {
    reservation: CapacityReservation;
    expectedLedgerGeneration: number;
    terminalState: Extract<ReservationState, "RELEASED" | "EXPIRED">;
    now: number;
  }) {
    const nextGeneration = params.expectedLedgerGeneration + 1;
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression:
                "generation = :generation AND activeReservationVcpu >= :requestedVcpu",
              UpdateExpression:
                "SET generation = :nextGeneration, activeReservationVcpu = activeReservationVcpu - :requestedVcpu, updatedAt = :now",
              ExpressionAttributeValues: encode({
                ":generation": params.expectedLedgerGeneration,
                ":nextGeneration": nextGeneration,
                ":requestedVcpu": params.reservation.requestedVcpu,
                ":now": params.now,
              }),
            },
          },
          {
            Update: {
              TableName: this.config.tableName,
              Key: itemKey(
                this.config,
                `RESERVATION#${params.reservation.reservationId}`,
              ),
              ConditionExpression:
                "ownerToken = :ownerToken AND #state IN (:active, :consuming)",
              UpdateExpression:
                "SET #state = :terminalState, ledgerGeneration = :nextGeneration, updatedAt = :now",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":ownerToken": params.reservation.ownerToken,
                ":active": "ACTIVE",
                ":consuming": "CONSUMING",
                ":terminalState": params.terminalState,
                ":nextGeneration": nextGeneration,
                ":now": params.now,
              }),
            },
          },
        ],
      }),
    );
    return nextGeneration;
  }

  async putDrainIntent(params: { drain: TemporalDrainRecord; now: number }) {
    const ttl =
      Math.floor(params.drain.deadline / 1_000) + this.config.auditTtlSeconds;
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression:
                "generation = :generation AND writerKind = :writer",
              ExpressionAttributeValues: encode({
                ":generation": params.drain.authorityGeneration,
                ":writer": "STEP_FUNCTIONS_LAMBDA",
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression: "generation = :generation",
              ExpressionAttributeValues: encode({
                ":generation": params.drain.ledgerGeneration,
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
              ExpressionAttributeValues: encode({
                ":cycleId": params.drain.cycleId,
                ":authorityGeneration": params.drain.authorityGeneration,
                ":now": params.now,
              }),
            },
          },
          {
            Put: {
              TableName: this.config.drainTableName,
              Item: encode({
                PK: this.config.controlPartitionKey,
                SK: getTemporalDrainSortKey(params.drain.taskArn),
                ...params.drain,
                ttl,
              }),
              ConditionExpression:
                "attribute_not_exists(PK) OR #state IN (:cancelled, :applied, :failed)",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":cancelled": "CANCELLED",
                ":applied": "APPLIED",
                ":failed": "FAILED",
              }),
            },
          },
        ],
      }),
    );
  }

  async readDrain(taskArn: string) {
    const response = await this.client.send(
      new GetItemCommand({
        TableName: this.config.drainTableName,
        Key: itemKey(this.config, getTemporalDrainSortKey(taskArn)),
        ConsistentRead: true,
      }),
    );
    return decodeItem<TemporalDrainRecord>(response.Item);
  }

  // One RETIREMENT record per SERVICE of a DRAINED build, fenced by the same
  // authority/ledger/cycle ConditionChecks as putDrainIntent: the intent is
  // observable in the drains table before any actuation happens. Terminal
  // records (APPLIED/FAILED) are overwritable so a service that resurfaces
  // (e.g. a failed verify followed by re-selection) mints a fresh intent.
  async putRetirementIntent(params: {
    record: TemporalRetirementRecord;
    now: number;
  }) {
    const ttl =
      Math.floor(params.record.verifyDeadline / 1_000) +
      this.config.auditTtlSeconds;
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression:
                "generation = :generation AND writerKind = :writer",
              ExpressionAttributeValues: encode({
                ":generation": params.record.authorityGeneration,
                ":writer": "STEP_FUNCTIONS_LAMBDA",
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression: "generation = :generation",
              ExpressionAttributeValues: encode({
                ":generation": params.record.ledgerGeneration,
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
              ExpressionAttributeValues: encode({
                ":cycleId": params.record.cycleId,
                ":authorityGeneration": params.record.authorityGeneration,
                ":now": params.now,
              }),
            },
          },
          {
            Put: {
              TableName: this.config.drainTableName,
              Item: encode({
                PK: this.config.controlPartitionKey,
                SK: getTemporalRetirementSortKey(params.record.serviceArn),
                ...params.record,
                ttl,
              }),
              ConditionExpression:
                "attribute_not_exists(PK) OR #state IN (:applied, :failed)",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":applied": "APPLIED",
                ":failed": "FAILED",
              }),
            },
          },
        ],
      }),
    );
  }

  async listActiveRetirements() {
    const retirements: TemporalRetirementRecord[] = [];
    let exclusiveStartKey: Record<string, AttributeValue> | undefined;
    do {
      const response = await this.client.send(
        new QueryCommand({
          TableName: this.config.drainTableName,
          KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
          ExpressionAttributeValues: encode({
            ":pk": this.config.controlPartitionKey,
            ":prefix": "RETIREMENT#",
          }),
          ExclusiveStartKey: exclusiveStartKey,
          ConsistentRead: true,
        }),
      );
      retirements.push(
        ...(response.Items ?? [])
          .map((item) => decodeItem<TemporalRetirementRecord>(item))
          .filter((item): item is TemporalRetirementRecord => Boolean(item))
          .filter((item) => item.state === "ZEROING"),
      );
      exclusiveStartKey = response.LastEvaluatedKey;
    } while (exclusiveStartKey);
    return retirements;
  }

  // The batch analogue of claimProtectedDecrease: ONE ledger transaction per
  // retired service (not one per task) releasing its entire allocation. The
  // committed value is computed client-side (clamped at zero) and written
  // under the ledger-generation condition, exactly like claimCapacityPlan —
  // no arithmetic ConditionExpression that could brick on drift.
  async claimRetirementRelease(params: {
    record: TemporalRetirementRecord;
    cycleId: string;
    authorityGeneration: number;
    ledger: CapacityLedger;
    releasedVcpu: number;
    now: number;
  }) {
    const nextGeneration = params.ledger.generation + 1;
    const allocations = {
      ...params.ledger.allocations,
      [params.record.serviceArn]: 0,
    };
    const nextManagedCommittedVcpu = Math.max(
      0,
      params.ledger.managedCommittedVcpu - params.releasedVcpu,
    );
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression:
                "generation = :generation AND writerKind = :writer",
              ExpressionAttributeValues: encode({
                ":generation": params.authorityGeneration,
                ":writer": "STEP_FUNCTIONS_LAMBDA",
              }),
            },
          },
          {
            Update: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression: "generation = :generation",
              UpdateExpression:
                "SET generation = :nextGeneration, managedCommittedVcpu = :managedCommittedVcpu, allocations = :allocations, updatedAt = :now, pendingCycleId = :cycleId",
              ExpressionAttributeValues: encode({
                ":generation": params.ledger.generation,
                ":nextGeneration": nextGeneration,
                ":managedCommittedVcpu": nextManagedCommittedVcpu,
                ":allocations": allocations,
                ":now": params.now,
                ":cycleId": params.cycleId,
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
              ExpressionAttributeValues: encode({
                ":cycleId": params.cycleId,
                ":authorityGeneration": params.authorityGeneration,
                ":now": params.now,
              }),
            },
          },
          {
            Update: {
              TableName: this.config.drainTableName,
              Key: itemKey(
                this.config,
                getTemporalRetirementSortKey(params.record.serviceArn),
              ),
              ConditionExpression: "intentId = :intentId AND #state = :zeroing",
              UpdateExpression:
                "SET releasedLedgerGeneration = :nextGeneration, updatedAt = :now",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":intentId": params.record.intentId,
                ":zeroing": "ZEROING",
                ":nextGeneration": nextGeneration,
                ":now": params.now,
              }),
            },
          },
        ],
      }),
    );
    return {
      ledger: {
        ...params.ledger,
        generation: nextGeneration,
        managedCommittedVcpu: nextManagedCommittedVcpu,
        allocations,
      } satisfies CapacityLedger,
    };
  }

  async completeRetirement(params: {
    record: TemporalRetirementRecord;
    terminalState: Extract<
      TemporalRetirementRecord["state"],
      "APPLIED" | "FAILED"
    >;
    reason?: string;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.drainTableName,
        Key: itemKey(
          this.config,
          getTemporalRetirementSortKey(params.record.serviceArn),
        ),
        ConditionExpression: "intentId = :intentId AND #state = :zeroing",
        UpdateExpression:
          "SET #state = :terminalState, terminalReason = :reason, appliedAt = :now",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":intentId": params.record.intentId,
          ":zeroing": "ZEROING",
          ":terminalState": params.terminalState,
          ":reason": params.reason ?? params.terminalState,
          ":now": params.now,
        }),
      }),
    );
  }

  async listActiveDrains() {
    const drains: TemporalDrainRecord[] = [];
    let exclusiveStartKey: Record<string, AttributeValue> | undefined;
    do {
      const response = await this.client.send(
        new QueryCommand({
          TableName: this.config.drainTableName,
          KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
          ExpressionAttributeValues: encode({
            ":pk": this.config.controlPartitionKey,
            ":prefix": "DRAIN#",
          }),
          ExclusiveStartKey: exclusiveStartKey,
          ConsistentRead: true,
        }),
      );
      drains.push(
        ...(response.Items ?? [])
          .map((item) => decodeItem<TemporalDrainRecord>(item))
          .filter((item): item is TemporalDrainRecord => Boolean(item))
          .filter(
            (item) =>
              item.state === "INTENT" ||
              item.state === "READY" ||
              item.state === "APPLYING" ||
              item.state === "VERIFYING",
          ),
      );
      exclusiveStartKey = response.LastEvaluatedKey;
    } while (exclusiveStartKey);
    return drains;
  }

  async putMaintenanceRedeploy(params: {
    maintenance: MaintenanceRedeploy;
    now: number;
  }) {
    const ttl =
      Math.floor(params.maintenance.deadline / 1_000) +
      this.config.auditTtlSeconds;
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression: "writerKind = :writer",
              ExpressionAttributeValues: encode({
                ":writer": "STEP_FUNCTIONS_LAMBDA",
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "attribute_not_exists(lockExpiresAt) OR lockExpiresAt < :now",
              ExpressionAttributeValues: encode({
                ":now": params.now,
              }),
            },
          },
          {
            Put: {
              TableName: this.config.tableName,
              Item: encode({
                PK: this.config.controlPartitionKey,
                SK: getTemporalMaintenanceSortKey(
                  params.maintenance.serviceArn,
                ),
                ...params.maintenance,
                ttl,
              }),
              ConditionExpression:
                "attribute_not_exists(PK) OR #state IN (:complete, :failed)",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":complete": "COMPLETE",
                ":failed": "FAILED",
              }),
            },
          },
        ],
      }),
    );
  }

  async listActiveMaintenanceRedeploys() {
    const records: MaintenanceRedeploy[] = [];
    let exclusiveStartKey: Record<string, AttributeValue> | undefined;
    do {
      const response = await this.client.send(
        new QueryCommand({
          TableName: this.config.tableName,
          KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
          ExpressionAttributeValues: encode({
            ":pk": this.config.controlPartitionKey,
            ":prefix": "MAINTENANCE#",
          }),
          ExclusiveStartKey: exclusiveStartKey,
          ConsistentRead: true,
        }),
      );
      records.push(
        ...(response.Items ?? [])
          .map((item) => decodeItem<MaintenanceRedeploy>(item))
          .filter((item): item is MaintenanceRedeploy => Boolean(item))
          .filter(
            (item) =>
              item.state === "REQUESTED" ||
              item.state === "DEPLOYING" ||
              item.state === "DRAINING",
          ),
      );
      exclusiveStartKey = response.LastEvaluatedKey;
    } while (exclusiveStartKey);
    return records;
  }

  async readMaintenanceRedeploy(serviceArn: string) {
    const response = await this.client.send(
      new GetItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(this.config, getTemporalMaintenanceSortKey(serviceArn)),
        ConsistentRead: true,
      }),
    );
    return decodeItem<MaintenanceRedeploy>(response.Item);
  }

  async requestMaintenanceRerun(params: {
    maintenance: MaintenanceRedeploy;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(
          this.config,
          getTemporalMaintenanceSortKey(params.maintenance.serviceArn),
        ),
        ConditionExpression:
          "maintenanceId = :maintenanceId AND #state IN (:requested, :deploying, :draining)",
        UpdateExpression: "SET rerunRequested = :true, updatedAt = :now",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":maintenanceId": params.maintenance.maintenanceId,
          ":requested": "REQUESTED",
          ":deploying": "DEPLOYING",
          ":draining": "DRAINING",
          ":true": true,
          ":now": params.now,
        }),
      }),
    );
  }

  async markMaintenanceLaunchAttempt(params: {
    maintenance: MaintenanceRedeploy;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(
          this.config,
          getTemporalMaintenanceSortKey(params.maintenance.serviceArn),
        ),
        ConditionExpression:
          "maintenanceId = :maintenanceId AND #state = :requested",
        UpdateExpression: "SET launchAttemptedAt = :now, updatedAt = :now",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":maintenanceId": params.maintenance.maintenanceId,
          ":requested": "REQUESTED",
          ":now": params.now,
        }),
      }),
    );
  }

  async prepareMaintenanceRerun(params: {
    maintenance: MaintenanceRedeploy;
    oldTaskArns: string[];
    reservationId: string;
    reservationOwnerToken: string;
    deadline: number;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(
          this.config,
          getTemporalMaintenanceSortKey(params.maintenance.serviceArn),
        ),
        ConditionExpression:
          "maintenanceId = :maintenanceId AND rerunRequested = :true",
        UpdateExpression:
          "SET #state = :requested, oldTaskArns = :oldTaskArns, rerunRequested = :false, reservationId = :reservationId, reservationOwnerToken = :reservationOwnerToken, deadline = :deadline, updatedAt = :now REMOVE launchAttemptedAt, terminalReason",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":maintenanceId": params.maintenance.maintenanceId,
          ":requested": "REQUESTED",
          ":oldTaskArns": params.oldTaskArns,
          ":reservationId": params.reservationId,
          ":reservationOwnerToken": params.reservationOwnerToken,
          ":true": true,
          ":false": false,
          ":deadline": params.deadline,
          ":now": params.now,
        }),
      }),
    );
  }

  async updateMaintenanceRedeploy(params: {
    maintenance: MaintenanceRedeploy;
    state: MaintenanceRedeploy["state"];
    reason?: string;
    deadline?: number;
    deadlineExtensionCount?: number;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.tableName,
        Key: itemKey(
          this.config,
          getTemporalMaintenanceSortKey(params.maintenance.serviceArn),
        ),
        ConditionExpression: "maintenanceId = :maintenanceId",
        UpdateExpression:
          params.deadline && params.deadlineExtensionCount !== undefined
            ? "SET #state = :state, terminalReason = :reason, deadline = :deadline, deadlineExtensionCount = :deadlineExtensionCount, updatedAt = :now"
            : "SET #state = :state, terminalReason = :reason, updatedAt = :now",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":maintenanceId": params.maintenance.maintenanceId,
          ":state": params.state,
          ":reason": params.reason ?? params.state,
          ...(params.deadline ? { ":deadline": params.deadline } : {}),
          ...(params.deadlineExtensionCount !== undefined
            ? {
                ":deadlineExtensionCount": params.deadlineExtensionCount,
              }
            : {}),
          ":now": params.now,
        }),
      }),
    );
  }

  async claimMaintenanceDrain(params: {
    drain: TemporalDrainRecord;
    cycleId: string;
    authorityGeneration: number;
    ledgerGeneration: number;
    now: number;
  }) {
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression:
                "generation = :generation AND writerKind = :writer",
              ExpressionAttributeValues: encode({
                ":generation": params.authorityGeneration,
                ":writer": "STEP_FUNCTIONS_LAMBDA",
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression: "generation = :generation",
              ExpressionAttributeValues: encode({
                ":generation": params.ledgerGeneration,
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
              ExpressionAttributeValues: encode({
                ":cycleId": params.cycleId,
                ":authorityGeneration": params.authorityGeneration,
                ":now": params.now,
              }),
            },
          },
          {
            Update: {
              TableName: this.config.drainTableName,
              Key: itemKey(
                this.config,
                getTemporalDrainSortKey(params.drain.taskArn),
              ),
              ConditionExpression:
                "intentId = :intentId AND kind = :kind AND #state = :ready AND deadline >= :now",
              UpdateExpression:
                "SET #state = :applying, applyCycleId = :cycleId, updatedAt = :now",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":intentId": params.drain.intentId,
                ":kind": "MAINTENANCE",
                ":ready": "READY",
                ":applying": "APPLYING",
                ":cycleId": params.cycleId,
                ":now": params.now,
              }),
            },
          },
        ],
      }),
    );
  }

  async cancelDrain(params: {
    drain: TemporalDrainRecord;
    reason: string;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.drainTableName,
        Key: itemKey(
          this.config,
          getTemporalDrainSortKey(params.drain.taskArn),
        ),
        ConditionExpression:
          "intentId = :intentId AND #state IN (:intent, :ready)",
        UpdateExpression:
          "SET #state = :cancelled, terminalReason = :reason, updatedAt = :now",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":intentId": params.drain.intentId,
          ":intent": "INTENT",
          ":ready": "READY",
          ":cancelled": "CANCELLED",
          ":reason": params.reason,
          ":now": params.now,
        }),
      }),
    );
  }

  async claimProtectedDecrease(params: {
    drain: TemporalDrainRecord;
    cycleId: string;
    authorityGeneration: number;
    ledger: CapacityLedger;
    serviceArn: string;
    taskVcpu: number;
    targetAllocationVcpu: number;
    now: number;
  }) {
    const nextGeneration = params.ledger.generation + 1;
    const allocations = {
      ...params.ledger.allocations,
      [params.serviceArn]: params.targetAllocationVcpu,
    };
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression:
                "generation = :generation AND writerKind = :writer",
              ExpressionAttributeValues: encode({
                ":generation": params.authorityGeneration,
                ":writer": "STEP_FUNCTIONS_LAMBDA",
              }),
            },
          },
          {
            Update: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression:
                "generation = :generation AND managedCommittedVcpu >= :taskVcpu",
              UpdateExpression:
                "SET generation = :nextGeneration, managedCommittedVcpu = managedCommittedVcpu - :taskVcpu, allocations = :allocations, updatedAt = :now, pendingCycleId = :cycleId",
              ExpressionAttributeValues: encode({
                ":generation": params.ledger.generation,
                ":nextGeneration": nextGeneration,
                ":taskVcpu": params.taskVcpu,
                ":allocations": allocations,
                ":now": params.now,
                ":cycleId": params.cycleId,
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
              ExpressionAttributeValues: encode({
                ":cycleId": params.cycleId,
                ":authorityGeneration": params.authorityGeneration,
                ":now": params.now,
              }),
            },
          },
          {
            Update: {
              TableName: this.config.drainTableName,
              Key: itemKey(
                this.config,
                getTemporalDrainSortKey(params.drain.taskArn),
              ),
              ConditionExpression:
                "intentId = :intentId AND #state = :ready AND authorityGeneration = :authorityGeneration AND ledgerGeneration = :ledgerGeneration AND deadline >= :now",
              UpdateExpression:
                "SET #state = :applying, applyCycleId = :cycleId, applyLedgerGeneration = :nextGeneration, updatedAt = :now",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":intentId": params.drain.intentId,
                ":ready": "READY",
                ":applying": "APPLYING",
                ":authorityGeneration": params.authorityGeneration,
                ":ledgerGeneration": params.drain.ledgerGeneration,
                ":cycleId": params.cycleId,
                ":nextGeneration": nextGeneration,
                ":now": params.now,
              }),
            },
          },
        ],
      }),
    );
    return nextGeneration;
  }

  async refreshReadyDrainFence(params: {
    drain: TemporalDrainRecord;
    cycleId: string;
    authorityGeneration: number;
    ledgerGeneration: number;
    now: number;
  }): Promise<TemporalDrainRecord> {
    const response = await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "WRITER_AUTHORITY"),
              ConditionExpression:
                "generation = :generation AND writerKind = :writer",
              ExpressionAttributeValues: encode({
                ":generation": params.authorityGeneration,
                ":writer": "STEP_FUNCTIONS_LAMBDA",
              }),
            },
          },
          {
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression: "generation = :generation",
              ExpressionAttributeValues: encode({
                ":generation": params.ledgerGeneration,
              }),
            },
          },
          {
            // Assert we still hold the cycle lock, exactly as
            // claimProtectedDecrease and claimMaintenanceDrain do. Without this,
            // the fence relied on the ledger-generation check alone; a cycle
            // that lost its lock could still fence a drain as long as no
            // out-of-cycle reserve/release had bumped the ledger generation.
            ConditionCheck: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "RECONCILER"),
              ConditionExpression:
                "cycleId = :cycleId AND authorityGeneration = :authorityGeneration AND lockExpiresAt >= :now",
              ExpressionAttributeValues: encode({
                ":cycleId": params.cycleId,
                ":authorityGeneration": params.authorityGeneration,
                ":now": params.now,
              }),
            },
          },
          {
            Update: {
              TableName: this.config.drainTableName,
              Key: itemKey(
                this.config,
                getTemporalDrainSortKey(params.drain.taskArn),
              ),
              ConditionExpression:
                "intentId = :intentId AND #state = :ready AND deadline >= :now",
              UpdateExpression:
                "SET cycleId = :cycleId, authorityGeneration = :authorityGeneration, ledgerGeneration = :ledgerGeneration, updatedAt = :now",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":intentId": params.drain.intentId,
                ":ready": "READY",
                ":cycleId": params.cycleId,
                ":authorityGeneration": params.authorityGeneration,
                ":ledgerGeneration": params.ledgerGeneration,
                ":now": params.now,
              }),
            },
          },
        ],
      }),
    );
    void response;
    return {
      ...params.drain,
      cycleId: params.cycleId,
      authorityGeneration: params.authorityGeneration,
      ledgerGeneration: params.ledgerGeneration,
    };
  }

  async completeDrain(params: {
    drain: TemporalDrainRecord;
    terminalState: Extract<TemporalDrainRecord["state"], "APPLIED" | "FAILED">;
    reason?: string;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.drainTableName,
        Key: itemKey(
          this.config,
          getTemporalDrainSortKey(params.drain.taskArn),
        ),
        ConditionExpression:
          "intentId = :intentId AND (#state IN (:applying, :verifying) OR (#state = :ready AND :terminalState = :failed))",
        UpdateExpression:
          "SET #state = :terminalState, terminalReason = :reason, appliedAt = :now",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":intentId": params.drain.intentId,
          ":applying": "APPLYING",
          ":verifying": "VERIFYING",
          ":ready": "READY",
          ":failed": "FAILED",
          ":terminalState": params.terminalState,
          ":reason": params.reason ?? params.terminalState,
          ":now": params.now,
        }),
      }),
    );
  }

  async markDrainVerifying(params: {
    drain: TemporalDrainRecord;
    now: number;
  }) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.config.drainTableName,
        Key: itemKey(
          this.config,
          getTemporalDrainSortKey(params.drain.taskArn),
        ),
        ConditionExpression: "intentId = :intentId AND #state = :applying",
        UpdateExpression:
          "SET #state = :verifying, verifyingAt = :now, updatedAt = :now",
        ExpressionAttributeNames: { "#state": "state" },
        ExpressionAttributeValues: encode({
          ":intentId": params.drain.intentId,
          ":applying": "APPLYING",
          ":verifying": "VERIFYING",
          ":now": params.now,
        }),
      }),
    );
  }

  async rollbackProtectedDecrease(params: {
    drain: TemporalDrainRecord;
    ledgerGeneration: number;
    serviceArn: string;
    taskVcpu: number;
    priorAllocationVcpu: number;
    reason: string;
    now: number;
  }) {
    const nextGeneration = params.ledgerGeneration + 1;
    const snapshot = await this.readControlSnapshot();
    const allocations = {
      ...snapshot.ledger.allocations,
      [params.serviceArn]: params.priorAllocationVcpu,
    };
    await this.client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: this.config.tableName,
              Key: itemKey(this.config, "CAPACITY_LEDGER"),
              ConditionExpression: "generation = :generation",
              UpdateExpression:
                "SET generation = :nextGeneration, managedCommittedVcpu = managedCommittedVcpu + :taskVcpu, allocations = :allocations, updatedAt = :now",
              ExpressionAttributeValues: encode({
                ":generation": params.ledgerGeneration,
                ":nextGeneration": nextGeneration,
                ":taskVcpu": params.taskVcpu,
                ":allocations": allocations,
                ":now": params.now,
              }),
            },
          },
          {
            Update: {
              TableName: this.config.drainTableName,
              Key: itemKey(
                this.config,
                getTemporalDrainSortKey(params.drain.taskArn),
              ),
              ConditionExpression:
                "intentId = :intentId AND #state = :applying",
              UpdateExpression:
                "SET #state = :failed, terminalReason = :reason, updatedAt = :now",
              ExpressionAttributeNames: { "#state": "state" },
              ExpressionAttributeValues: encode({
                ":intentId": params.drain.intentId,
                ":applying": "APPLYING",
                ":failed": "FAILED",
                ":reason": params.reason,
                ":now": params.now,
              }),
            },
          },
        ],
      }),
    );
    return nextGeneration;
  }

  async recoverApplyingDecrease(params: {
    drain: TemporalDrainRecord;
    serviceArn: string;
    priorAllocationVcpu: number;
    now: number;
  }) {
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const snapshot = await this.readControlSnapshot();
      const current = await this.readDrain(params.drain.taskArn);
      if (!current || current.intentId !== params.drain.intentId) {
        throw new Error(
          `Drain ${params.drain.intentId} changed while recovering APPLYING`,
        );
      }
      if (current.state === "FAILED") return snapshot.ledger.generation;
      if (current.state !== "APPLYING" && current.state !== "VERIFYING") {
        throw new Error(
          `Drain ${params.drain.intentId} is ${current.state}, not recoverable`,
        );
      }
      const nextGeneration = snapshot.ledger.generation + 1;
      const currentAllocation =
        snapshot.ledger.allocations[params.serviceArn] ?? 0;
      const alreadyRestored = currentAllocation >= params.priorAllocationVcpu;
      const restoreVcpu = Math.max(
        0,
        params.priorAllocationVcpu - currentAllocation,
      );
      const allocations = {
        ...snapshot.ledger.allocations,
        [params.serviceArn]: params.priorAllocationVcpu,
      };
      try {
        await this.client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Update: {
                  TableName: this.config.tableName,
                  Key: itemKey(this.config, "CAPACITY_LEDGER"),
                  ConditionExpression: "generation = :generation",
                  UpdateExpression: alreadyRestored
                    ? "SET generation = :nextGeneration, allocations = :allocations, updatedAt = :now"
                    : "SET generation = :nextGeneration, managedCommittedVcpu = managedCommittedVcpu + :restoreVcpu, allocations = :allocations, updatedAt = :now",
                  ExpressionAttributeValues: encode({
                    ":generation": snapshot.ledger.generation,
                    ":nextGeneration": nextGeneration,
                    ...(!alreadyRestored
                      ? { ":restoreVcpu": restoreVcpu }
                      : {}),
                    ":allocations": allocations,
                    ":now": params.now,
                  }),
                },
              },
              {
                Update: {
                  TableName: this.config.drainTableName,
                  Key: itemKey(
                    this.config,
                    getTemporalDrainSortKey(params.drain.taskArn),
                  ),
                  ConditionExpression:
                    "intentId = :intentId AND #state IN (:applying, :verifying)",
                  UpdateExpression:
                    "SET #state = :failed, terminalReason = :reason, updatedAt = :now",
                  ExpressionAttributeNames: { "#state": "state" },
                  ExpressionAttributeValues: encode({
                    ":intentId": params.drain.intentId,
                    ":applying": "APPLYING",
                    ":verifying": "VERIFYING",
                    ":failed": "FAILED",
                    ":reason": "APPLYING_RECOVERY",
                    ":now": params.now,
                  }),
                },
              },
            ],
          }),
        );
        return nextGeneration;
      } catch (error) {
        if (
          !(
            error instanceof TransactionCanceledException ||
            error instanceof TransactionConflictException
          ) ||
          attempt === 5
        ) {
          throw error;
        }
      }
    }
    throw new Error(`Unable to recover drain ${params.drain.intentId}`);
  }

  isConditionalFailure(error: unknown) {
    return (
      error instanceof ConditionalCheckFailedException ||
      error instanceof TransactionCanceledException ||
      error instanceof TransactionConflictException
    );
  }
}
