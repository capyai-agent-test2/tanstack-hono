import { TEMPORAL_STABLE_POOLS } from "@capy/shared/temporal/capacity";

import type {
  AllocationPlan,
  CapacitySnapshot,
  PoolDemand,
  PoolGrant,
} from "./types.js";

const taskVcpu = (demand: PoolDemand) => demand.service.cpuUnits / 1024;
const committedDesired = (demand: PoolDemand) =>
  demand.service.committedDesiredCount ?? demand.service.desiredCount;

const priority = (demand: PoolDemand) => {
  const { environment, buildState, poolId } = demand.service;
  const backgroundPenalty = poolId === "background-batch" ? 100 : 0;
  if (environment === "prod" && buildState === "CURRENT")
    return 0 + backgroundPenalty;
  if (environment === "prod" && buildState === "RAMPING")
    return 1 + backgroundPenalty;
  if (environment === "prod" && buildState === "DRAINING")
    return 2 + backgroundPenalty;
  if (environment === "prod") return 3 + backgroundPenalty;
  if (environment === "dev" && buildState === "CURRENT")
    return 10 + backgroundPenalty;
  if (environment === "dev" && buildState === "DRAINING")
    return 11 + backgroundPenalty;
  if (environment === "dev") return 12 + backgroundPenalty;
  // Staging only ever shares a controller's view with itself (scoped
  // instance), so its tier relative to prod/dev is moot today; it still needs
  // explicit tiers so its demands sort deterministically ahead of preview.
  if (environment === "staging" && buildState === "CURRENT")
    return 14 + backgroundPenalty;
  if (environment === "staging" && buildState === "DRAINING")
    return 15 + backgroundPenalty;
  if (environment === "staging") return 16 + backgroundPenalty;
  return 20 + backgroundPenalty;
};

const compareDemand = (left: PoolDemand, right: PoolDemand) =>
  priority(left) - priority(right) ||
  right.floor - left.floor ||
  TEMPORAL_STABLE_POOLS[right.service.poolId].allocationWeight -
    TEMPORAL_STABLE_POOLS[left.service.poolId].allocationWeight ||
  left.service.serviceArn.localeCompare(right.service.serviceArn);

export function allocateGlobalCapacity(
  demands: PoolDemand[],
  snapshot: CapacitySnapshot,
): AllocationPlan {
  // Account-quota headroom is always a ceiling. When the controller is
  // environment-scoped, the static per-environment budget is a second,
  // usually tighter ceiling: everything this controller manages (committed +
  // reserved — both env-scoped by construction in scoped mode) must fit
  // inside its partition, regardless of account headroom.
  const quotaHeadroomVcpu = Math.max(
    0,
    snapshot.quotaVcpu -
      snapshot.hardReserveVcpu -
      snapshot.unmanagedCommittedVcpu -
      snapshot.managedCommittedVcpu -
      snapshot.activeReservationVcpu,
  );
  const budgetHeadroomVcpu =
    snapshot.environmentVcpuBudget === undefined
      ? Number.POSITIVE_INFINITY
      : Math.max(
          0,
          snapshot.environmentVcpuBudget -
            snapshot.managedCommittedVcpu -
            snapshot.activeReservationVcpu,
        );
  const allocatableAdditionalVcpu = Math.min(
    quotaHeadroomVcpu,
    budgetHeadroomVcpu,
  );
  const prodCommitted = demands
    .filter((demand) => demand.service.environment === "prod")
    .reduce(
      (total, demand) => total + committedDesired(demand) * taskVcpu(demand),
      0,
    );
  const protectedProdHeadroomVcpu = Math.max(
    0,
    snapshot.prodGuaranteedEnvelopeVcpu - prodCommitted,
  );
  let remainingGlobalVcpu = allocatableAdditionalVcpu;
  let remainingNonProdVcpu = Math.max(
    0,
    allocatableAdditionalVcpu - protectedProdHeadroomVcpu,
  );

  const grantsByService = new Map<string, PoolGrant>();
  for (const demand of demands) {
    const priorDesired = committedDesired(demand);
    const wanted = Math.max(priorDesired, demand.boundedWanted);
    grantsByService.set(demand.service.serviceArn, {
      service: demand.service,
      wanted,
      granted: priorDesired,
      priorDesired,
      additionalVcpu: 0,
      ungranted: Math.max(0, wanted - priorDesired) + demand.hardMaxShortfall,
      ...(demand.hardMaxShortfall > 0
        ? { ungrantedReason: "HARD_MAX" as const }
        : {}),
      ...(demand.staleInput && wanted > demand.service.desiredCount
        ? { ungrantedReason: "STALE_INPUT" as const }
        : {}),
    });
  }

  const eligible = demands
    .filter((demand) => !demand.staleInput)
    .sort(compareDemand);
  const availableFor = (demand: PoolDemand) =>
    demand.service.environment === "prod"
      ? remainingGlobalVcpu
      : Math.min(remainingGlobalVcpu, remainingNonProdVcpu);
  const consume = (demand: PoolDemand, grant: PoolGrant) => {
    const vcpu = taskVcpu(demand);
    grant.granted += 1;
    grant.additionalVcpu += vcpu;
    grant.ungranted = Math.max(demand.hardMaxShortfall, grant.ungranted - 1);
    remainingGlobalVcpu -= vcpu;
    if (demand.service.environment !== "prod") {
      remainingNonProdVcpu -= vcpu;
    }
  };
  const allocateFloors = (stageDemands: PoolDemand[]) => {
    while (true) {
      let progressed = false;
      for (const demand of stageDemands) {
        const grant = grantsByService.get(demand.service.serviceArn);
        if (!grant || grant.granted >= Math.min(demand.floor, grant.wanted)) {
          continue;
        }
        const vcpu = taskVcpu(demand);
        if (availableFor(demand) + Number.EPSILON < vcpu) continue;
        consume(demand, grant);
        progressed = true;
      }
      if (!progressed) break;
    }
  };
  const allocateExcess = (stageDemands: PoolDemand[]) => {
    const priorities = [...new Set(stageDemands.map(priority))].sort(
      (left, right) => left - right,
    );
    for (const tier of priorities) {
      const tierDemands = stageDemands.filter(
        (demand) => priority(demand) === tier,
      );
      while (true) {
        const candidates = tierDemands
          .map((demand) => ({
            demand,
            grant: grantsByService.get(demand.service.serviceArn),
          }))
          .filter(
            (
              item,
            ): item is {
              demand: PoolDemand;
              grant: PoolGrant;
            } => Boolean(item.grant && item.grant.granted < item.grant.wanted),
          )
          .filter(
            ({ demand }) =>
              availableFor(demand) + Number.EPSILON >= taskVcpu(demand),
          )
          .sort((left, right) => {
            const leftWeight =
              TEMPORAL_STABLE_POOLS[left.demand.service.poolId]
                .allocationWeight;
            const rightWeight =
              TEMPORAL_STABLE_POOLS[right.demand.service.poolId]
                .allocationWeight;
            const leftScore =
              left.grant.additionalVcpu / Math.max(leftWeight, Number.EPSILON);
            const rightScore =
              right.grant.additionalVcpu /
              Math.max(rightWeight, Number.EPSILON);
            return (
              leftScore - rightScore || compareDemand(left.demand, right.demand)
            );
          });
        const selected = candidates[0];
        if (!selected) break;
        consume(selected.demand, selected.grant);
      }
    }
  };

  const isPinnedProd = (demand: PoolDemand) =>
    demand.service.environment === "prod" &&
    demand.service.poolId !== "background-batch" &&
    (demand.service.buildState === "CURRENT" ||
      demand.service.buildState === "RAMPING" ||
      demand.service.buildState === "DRAINING");
  const stages = [
    eligible.filter(isPinnedProd),
    eligible.filter(
      (demand) =>
        demand.service.environment === "prod" &&
        demand.service.poolId !== "background-batch" &&
        !isPinnedProd(demand),
    ),
    eligible.filter(
      (demand) =>
        demand.service.environment === "dev" &&
        demand.service.poolId !== "background-batch",
    ),
    // Gate-2 F1: without an explicit stage, staging demands matched no stage
    // filter at all — floors and excess were never allocated, so a
    // staging-scoped controller could scale its fleet in but never out of
    // idle. Every environment name must appear here (or in the
    // background-batch catch-all) to be scalable.
    eligible.filter(
      (demand) =>
        demand.service.environment === "staging" &&
        demand.service.poolId !== "background-batch",
    ),
    eligible.filter(
      (demand) =>
        demand.service.environment === "preview" &&
        demand.service.poolId !== "background-batch",
    ),
    eligible.filter((demand) => demand.service.poolId === "background-batch"),
  ].map((stage) => stage.sort(compareDemand));
  for (const stage of stages) {
    allocateFloors(stage);
    allocateExcess(stage);
  }

  const grants = [...grantsByService.values()].map((grant) => {
    if (grant.ungranted > 0 && !grant.ungrantedReason) {
      grant.ungrantedReason =
        grant.wanted > TEMPORAL_STABLE_POOLS[grant.service.poolId].hardMax
          ? "HARD_MAX"
          : "GLOBAL_CAPACITY";
    }
    return grant;
  });
  return {
    grants,
    allocatableAdditionalVcpu,
    protectedProdHeadroomVcpu,
    ungrantedProdReplicas: grants
      .filter((grant) => grant.service.environment === "prod")
      .reduce((total, grant) => total + grant.ungranted, 0),
  };
}
