# Founder Operations Runbook

Use this runbook when a founder-operator needs a fast signal on production
health, a pre-release go/no-go, or a lightweight incident response flow.

## Health Checks

- Use `GET /api/health` for uptime monitoring, deploy smoke tests, and "is the
  app reachable?" checks.
- A healthy response should be fast, return a 2xx status, and avoid depending on
  optional third-party services unless they are required for every request.
- Check it after deploys, after configuration changes, and whenever users report
  broad availability problems.

Example:

```bash
curl -fsS https://your-app.example.com/api/health
```

## Release Readiness

- Use `GET /api/release-readiness` immediately before promoting a build or
  opening traffic to a new environment once that endpoint is deployed.
- If the release-readiness endpoint is not available in an environment yet, use
  the build result, `vp check`, tests, and `/api/health` as the temporary gate.
- Treat it as a release gate: if it returns a non-2xx status or reports a failed
  required check, pause the release and fix the blocker.
- Keep this endpoint safe to call repeatedly. It should report readiness, not
  mutate state.

Example:

```bash
curl -fsS https://your-app.example.com/api/release-readiness
```

Short checklist before release:

- Build completed from the intended commit.
- `vp check` and tests passed in CI or locally.
- `/api/health` returns healthy in the target environment.
- `/api/release-readiness` returns ready for required dependencies and config, or
  the temporary gate above is documented for that environment.
- Rollback target is known and accessible.
- Owner is watching deploy logs and user-facing error rates for at least 15
  minutes after release.

## Incident Triage

1. Confirm scope: compare user reports, `/api/health`, deploy status, logs, and
   error-rate dashboards.
2. Classify severity:
   - Sev1: app unavailable, data loss risk, or core user flow blocked for many
     users.
   - Sev2: important feature degraded or affecting a subset of users.
   - Sev3: low-impact bug with a workaround.
3. Stabilize first: stop active deploys, disable risky feature flags, or roll
   back before debugging deeply.
4. Capture facts: start time, symptoms, affected paths, recent releases, and the
   current owner.
5. Verify recovery with `/api/health`, the affected user flow, and any relevant
   release-readiness checks.

## Rollback And Escalation

- Roll back when the incident started after a release, the cause is unknown, and
  the impact is Sev1 or high Sev2.
- Prefer the smallest safe rollback: previous known-good build, reverted config,
  or disabled feature flag.
- Escalate when customer data, payments, authentication, security, or an
  external dependency is involved.
- If rollback fails, freeze non-essential changes, keep one incident owner, and
  ask for help with a concise summary: impact, timeline, attempted fixes, and
  current hypothesis.

## Communication Cadence

- Sev1: acknowledge within 5 minutes, update every 15 minutes until mitigated,
  then send a short resolution note.
- Sev2: acknowledge within 15 minutes and update every 30-60 minutes while the
  issue is active.
- Sev3: batch into the normal product/support cadence unless users are actively
  waiting.
- Every update should include current impact, action underway, next update time,
  and whether users need to do anything.
