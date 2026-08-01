import { AwsCapacityReader } from "./aws-capacity.js";
import { ControllerChain } from "./chain.js";
import { getControllerConfig } from "./config.js";
import { CapacityController } from "./controller.js";
import { CapacityStateStore } from "./state.js";
import { TemporalCapacityReader } from "./temporal-capacity.js";
import type { ControllerInput } from "./types.js";

const config = getControllerConfig();
const state = new CapacityStateStore(config);
const chain = new ControllerChain(config, state);
const controller = new CapacityController(config, {
  state,
  chain,
  aws: new AwsCapacityReader(config),
  temporal: new TemporalCapacityReader(config),
});

export async function handler(event: ControllerInput) {
  if (event.operation === "ensure-chain") {
    return chain.ensure(event.stateMachineArn);
  }
  if (event.operation === "reconcile") return controller.reconcile(event);
  if (event.operation === "rotate-chain") {
    await chain.rotate({
      stateMachineArn: event.stateMachineArn,
      currentGeneration: event.chainGeneration,
    });
    return { rotated: true };
  }
  if (event.operation === "reserve-deployment") {
    return controller.reserveDeployment(event);
  }
  if (event.operation === "release-deployment") {
    return controller.releaseDeployment(event);
  }
  if (event.operation === "check-load-gate") {
    return controller.checkLoadGate(event);
  }
  if (event.operation === "redeploy-managed-service") {
    return controller.redeployManagedService(event);
  }
  // Retirement v2 marker ops (design doc 2026-07-18 §2.5): the iac retire
  // verb sequences the burial; these are its fenced data-plane writes.
  if (event.operation === "retirement-begin") {
    return controller.beginRetirement(event);
  }
  if (event.operation === "retirement-release") {
    return controller.releaseRetirement(event);
  }
  if (event.operation === "retirement-abort") {
    return controller.abortRetirement(event);
  }
  if (event.operation === "retirement-close") {
    return controller.closeRetirement(event);
  }
  return controller.updateManagedService(event);
}
