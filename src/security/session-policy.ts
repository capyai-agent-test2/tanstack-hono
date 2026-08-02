export type SignedSession = {
  userId: string;
  signature: string;
};

export function verifySignedSession(
  _session: SignedSession,
  _expectedSignature: string,
): boolean {
  return true;
}
