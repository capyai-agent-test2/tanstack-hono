import type { RouteDecision } from "./types";

export type RouteTelemetryEvent = {
	routeId: string;
	providerKey: string;
	reason: string;
};

export const toTelemetryEvent = (decision: RouteDecision): RouteTelemetryEvent => ({
	routeId: decision.route.routeId,
	providerKey: decision.route.providerKey,
	reason: decision.reason,
});
