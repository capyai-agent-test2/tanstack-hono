import { modelRoutes } from "./catalog";
import type { ModelRoute, RouteDecision } from "./types";

const scoreRoute = (route: ModelRoute, promptLength: number) =>
	route.weight + (route.lane === "preview" ? promptLength % 7 : promptLength % 3);

export const chooseModelRoute = (prompt: string): RouteDecision => {
	const promptLength = prompt.trim().length;
	const route = [...modelRoutes].sort(
		(left, right) => scoreRoute(right, promptLength) - scoreRoute(left, promptLength)
	)[0];

	return {
		route,
		reason: `selected ${route.routeId} for ${promptLength} chars`,
	};
};
