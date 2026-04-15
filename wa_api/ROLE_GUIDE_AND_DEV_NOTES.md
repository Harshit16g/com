# wa_api Role Guide and Developer Notes

This guide explains how each role should use `wa_api` safely and consistently.

## Roles and Responsibilities

### Platform Integrator (Leaex backend caller)
- Use partner endpoints with `x-api-key` and `x-tenant-id`.
- Call `POST /message/send` for CRM 1:1 sends.
- Call `POST /campaign/start` only for campaign-eligible tenants.
- Never call admin endpoints from partner-facing services.
- Treat `job_id` as async tracking handle; delivery is not immediate.

### Partner Operations User
- Use message sends for transactional CRM use cases.
- Use campaigns only through approved workflows (pool numbers, no personal number blasting).
- Monitor campaign progression via `GET /campaign/status/:id`.
- Respect opt-out behavior; blocked sends are expected compliance behavior.

### Admin / SRE Operator
- Use admin routes with `x-admin-key`.
- Manage tenant limits and instance states through admin endpoints.
- Monitor:
  - queue depth (`jobs:ready:*`, `jobs:scheduled:*`, `jobs:dlq:*`)
  - instance health (`instance_health:*`)
  - campaign pool availability (`pool:available`)
- Rotate compromised keys immediately (`PAUTH_API_KEY`, `ADMIN_API_KEY`, `WEBHOOK_SHARED_SECRET`, `EVO_API_KEY`).

### Security Reviewer / Auditor
- Verify webhook auth is enforced (`x-webhook-secret` required).
- Verify API/admin auth uses constant-time comparison path.
- Verify no plaintext phone leakage in partner responses where masking is expected.
- Verify campaign routing always uses pool instances, not partner personal instances.
- Review `.env*` for placeholders only; no production secrets in repository.

### Developer (wa_api contributor)
- Keep env naming consistent with current config:
  - `EVO_BASE_URL`
  - `EVO_API_KEY`
  - `PAUTH_API_KEY`
  - `ADMIN_API_KEY`
  - `WEBHOOK_SHARED_SECRET`
- Use `cargo check` after changes and fix warnings you introduce.
- Maintain backward-compatible API semantics when possible.
- Prefer additive migrations over destructive schema changes.

## Standard Developer Workflow

1. Update `.env.local` with non-secret placeholders.
2. Start infra (`postgres`, `redis`) and run `cargo check`.
3. Run API + worker + scheduler locally for integration tests.
4. Validate:
   - authenticated partner flow
   - admin flow
   - webhook flow with shared secret
5. Re-run `cargo check` before merge.

## Architecture Guardrails (Do Not Break)

- Campaigns must route to pool numbers only.
- Partner traffic must stay tenant-scoped.
- Webhooks must be authenticated.
- Rate limits must remain enforced per instance.
- Opt-out and spam-guard blocks must remain hard gates.

## Incident Playbook (Quick)

- **401 spike on partner APIs**: verify `PAUTH_API_KEY` mismatch first.
- **Webhook failures**: verify `WEBHOOK_SHARED_SECRET` header and value.
- **Campaigns failing to start**: check `pool:available` and pool health state.
- **Sustained delivery failures**: increase delay window and inspect instance health.
- **DLQ growth**: classify transient vs permanent errors before requeue.
