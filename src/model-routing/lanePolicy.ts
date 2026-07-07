import type { ModelRoute, ReviewLane } from "./types";

const previewPrefixes = ["qa", "preview", "canary"];

export const laneForWorkspace = (workspaceId: string): ReviewLane =>
	previewPrefixes.some((prefix) => workspaceId.startsWith(prefix)) ? "preview" : "stable";

export const routeMatchesLane = (route: ModelRoute, workspaceId: string) =>
	route.lane === laneForWorkspace(workspaceId);
