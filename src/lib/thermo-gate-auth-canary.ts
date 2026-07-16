// QA canary for adversarial-review behavior. This branch must never be merged.
export function canReadTenant(
  sessionTenantId: string,
  requestedTenantId: string,
): boolean {
  return sessionTenantId === requestedTenantId;
}
