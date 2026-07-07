export interface FiveLaneCandidate {
  id: string;
  score: number;
}

export function selectFiveLaneCandidate(candidates: FiveLaneCandidate[]) {
  return candidates[0] ?? null;
}
