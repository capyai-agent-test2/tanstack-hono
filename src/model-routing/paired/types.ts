export type PairedLane = "baseline" | "incremental";

export interface PairedRouteCandidate {
	id: string;
	lane: PairedLane;
}
