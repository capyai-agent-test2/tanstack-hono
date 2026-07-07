export type ReviewLane = "stable" | "preview";

export type ModelRoute = {
	routeId: string;
	providerKey: string;
	lane: ReviewLane;
	weight: number;
};

export type RouteDecision = {
	route: ModelRoute;
	reason: string;
};
