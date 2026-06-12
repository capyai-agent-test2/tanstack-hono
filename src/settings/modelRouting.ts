import type { ReviewLane } from "../model-routing";

export type ModelRoutingSettings = {
	defaultLane: ReviewLane;
	enablePreviewLane: boolean;
	maxRouteFanout: number;
};

export const modelRoutingSettings: ModelRoutingSettings = {
	defaultLane: "stable",
	enablePreviewLane: true,
	maxRouteFanout: 3,
};
