import type { PairedRouteCandidate } from "./types";

export function scorePairedCandidate(candidate: PairedRouteCandidate) {
	return candidate.id.length + candidate.lane.length;
}
