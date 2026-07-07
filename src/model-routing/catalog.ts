import type { ModelRoute } from "./types";

export const modelRoutes = [
	{ routeId: "route-alpha", providerKey: "openai", lane: "stable", weight: 70 },
	{ routeId: "route-beta", providerKey: "anthropic", lane: "preview", weight: 30 },
	{ routeId: "route-gamma", providerKey: "local", lane: "preview", weight: 10 },
] as const satisfies readonly ModelRoute[];

export const getRouteById = (routeId: string) =>
	modelRoutes.find((route) => route.routeId === routeId) ?? modelRoutes[0];
