import type { PairedRouteCandidate } from "./types";

export function selectPairedCandidate(candidates: PairedRouteCandidate[]) {
	return candidates[0] ?? null;
}
