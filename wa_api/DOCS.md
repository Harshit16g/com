# wa_api — WhatsApp Engine Documentation

Complete guide for setup, deployment, and operations of the Leaex WhatsApp Engine.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Repository Structure](#2-repository-structure)
3. [Prerequisites](#3-prerequisites)
4. [Local Development Setup](#4-local-development-setup)
5. [Configuration Reference](#5-configuration-reference)
6. [API Reference](#6-api-reference)
7. [evo API Setup](#7-evo-api-setup)
8. [Deployment](#8-deployment)
9. [Scaling](#9-scaling)
10. [Database Schema](#10-database-schema)
11. [Operations & Monitoring](#11-operations--monitoring)
12. [Anti-Ban Rules](#12-anti-ban-rules)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Architecture Overview

```
Partners / Leaex Backend
         │
         ▼  (x-api-key + x-tenant-id)
┌─────────────────────────────────────────────────────┐
│                   wa_api Stack                      │
│                                                     │
│  ┌──────────────┐   Jobs    ┌──────────────────┐   │
│  │ API Gateway  │──────────▶│  Redis           │   │
│  │   :8080      │           │  (queues+cache)  │   │
│  └──────────────┘           └────────┬─────────┘   │
│                                      │ RPOP         │
│  ┌──────────────┐   ┌────────────────▼──────────┐  │
│  │  Scheduler   │   │  Worker Pool (4 replicas) │  │
│  │  (500ms tick)│   │  per-instance lock        │  │
│  └──────────────┘   │  rate-limit 8–15s delay   │  │
│                     └────────────┬──────────────┘  │
│  ┌──────────────┐                │                  │
│  │ Pool Manager │   ┌────────────▼──────────────┐  │
│  │  (15min tick)│   │  Health Monitor (5min)    │  │
│  └──────────────┘   └───────────────────────────┘  │
│                                                     │
│  ┌──────────────┐                                   │
│  │  PostgreSQL  │ ◀── all services read/write       │
│  └──────────────┘                                   │
└─────────────────────────────────────────────────────┘
         │  HTTP (evo API protocol)
         ▼
┌──────────────────────────────────────┐
│  Evo API (standalone)                │
│  :8081  — manages WhatsApp sessions  │
│  Scales independently per demand     │
└──────────────────────────────────────┘
         │  Baileys (WhatsApp Web)
         ▼
     WhatsApp Servers
```

### Two Deployment Units

| Stack | What's inside | Scale strategy |
|---|---|---|
| `docker-compose.yml` | API Gateway + Worker + Scheduler + Pool Manager + Health Monitor + PostgreSQL + Redis | Scale workers horizontally |
| `docker-compose.evo.yml` | evo API + its own PostgreSQL | One instance per agency OR shared pool |

### Key Design Decisions

- **No Supabase dependency** — Everything uses the bundled PostgreSQL. Zero external DB.
- **Campaign pool isolation** — Campaigns always go through `pool:available` instances, never a partner's personal number.
- **Anti-ban rate limiting** — 8–15s randomized delay per instance, per-instance SETNX lock prevents concurrent sends.
- **Spam guard** — SHA-256 hashed phones, platform daily limit 5, weekly cap 15, max 3 partners/day per number.
- **Idempotency** — `sha256(tenant_id + phone + template_hash + campaign_id)`, stored 48h in Redis.
- **Retry engine** — Transient failures: 30s → 2min → 10min backoff, max 3 retries then DLQ (7-day TTL).

---

## 2. Repository Structure

```
wa_api/
├── Cargo.toml                    # Workspace root
├── .env                          # Local dev environment (not committed)
├── docker-compose.yml            # wa_api stack (Rust + Postgres + Redis)
├── docker-compose.evo.yml        # evo API (standalone)
├── migrations/
│   ├── 001_initial_schema.sql    # Schema — auto-applied by Postgres on first start
│   └── cleanup_supabase_production.sql  # One-time: remove tables from Supabase
├── crates/
│   ├── shared/          # Common types, DB client, Redis client, evo client
│   ├── api_gateway/     # Axum HTTP server — all partner-facing endpoints
│   ├── worker/          # Job processor — sends via evo API
│   ├── scheduler/       # Promotes scheduled jobs → ready queue (500ms)
│   ├── pool_manager/    # Manages campaign pool numbers (15min)
│   └── health_monitor/  # Monitors instance health, fires alerts (5min)
└── Dockerfile.*          # One Dockerfile per binary
```

---

## 3. Prerequisites

### Required
- **Rust** 1.78+ with `cargo` — [rustup.rs](https://rustup.rs)
- **Docker** + **Docker Compose** v2 — for running the full stack
- **Node.js** 18+ + **npm** — for evo API
- **Git** with SSH/HTTPS access to `github.com/Harshit16g/whatsapp-api`

### Install Docker (Ubuntu/Debian)
```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker          # apply group change in current shell
docker --version       # verify
```

---

## 4. Local Development Setup

### Step 1 — Clone both repositories

```bash
mkdir -p ~/projects
cd ~/projects

# wa_api (this repo)
git clone <this-repo> wa_api

# evo API (WhatsApp bridge)
git clone git@github.com:Harshit16g/whatsapp-api evo
```

### Step 2 — Configure environment

```bash
cd ~/projects/wa_api
cp .env.example .env    # or edit .env directly
```

Minimum required in `.env`:

```env
DATABASE_URL=postgresql://wa_api:wa_api_secret@localhost:5433/wa_api
REDIS_URL=redis://localhost:6380
evo_BASE_URL=http://localhost:8081
evo_API_KEY=leaex-evo-api-key-2026
ADMIN_API_KEY=change-me-in-prod
```

### Step 3 — Start infrastructure (Postgres + Redis)

```bash
# Start just the database services for local dev
docker compose up postgres redis -d

# Postgres is exposed on :5433, Redis on :6380 (avoids conflicts with any local installs)
# Schema is auto-applied from migrations/001_initial_schema.sql on first start
```

Wait ~5 seconds for Postgres to initialise, then verify:

```bash
docker compose exec postgres psql -U wa_api -d wa_api -c "\dt"
# Should list: agencies, tenants, wa_campaigns, wa_interaction_log, etc.
```

### Step 4 — Seed development data

```bash
# Hash the dev API key
KEY_HASH=$(echo -n "leaex-dev-key-2026" | sha256sum | awk '{print $1}')

docker compose exec postgres psql -U wa_api -d wa_api -c "
INSERT INTO agencies (id, name, api_key, subscription_status, plan_tier, daily_msg_limit)
VALUES (
  '58af205e-7745-4d66-9cc0-ab2dd10fe35d',
  'Leaex Platform',
  '$KEY_HASH',
  'active', 'enterprise', 10000
) ON CONFLICT DO NOTHING;

INSERT INTO tenants (id, agency_id, instance_name, instance_status, campaign_enabled)
VALUES (
  '40e571f6-d966-4d49-8a1a-750adff9df34',
  '58af205e-7745-4d66-9cc0-ab2dd10fe35d',
  'wa_test_partner_01',
  'active', true
) ON CONFLICT DO NOTHING;
"
```

### Step 5 — Build and run Rust services

```bash
cd ~/projects/wa_api
cargo build --release

# Terminal 1 — API Gateway
./target/release/api_gateway

# Terminal 2 — Worker
./target/release/worker

# Terminal 3 — Scheduler
./target/release/scheduler
```

### Step 6 — Start evo API

```bash
cd ~/projects/evo

# First time only
npm install
DATABASE_PROVIDER=postgresql npm run db:generate
DATABASE_PROVIDER=postgresql npm run db:deploy   # uses .env DATABASE_CONNECTION_URI

# Start
npm start
# Listens on :8081
```

`.env` for evo API (`~/projects/evo/.env`):
```env
SERVER_PORT=8081
DATABASE_PROVIDER=postgresql
DATABASE_CONNECTION_URI=postgresql://evo:evo_secret@localhost:5432/evo
AUTHENTICATION_API_KEY=leaex-evo-api-key-2026
WEBHOOK_GLOBAL_URL=http://localhost:8080/webhook/evo
WEBHOOK_GLOBAL_ENABLED=true
WEBHOOK_EVENTS_MESSAGES_UPSERT=true
WEBHOOK_EVENTS_MESSAGES_UPDATE=true
WEBHOOK_EVENTS_CONNECTION_UPDATE=true
WEBHOOK_EVENTS_QRCODE_UPDATED=true
CACHE_LOCAL_ENABLED=true
```

### Step 7 — Create a WhatsApp instance and scan QR

```bash
# Create instance
curl -X POST http://localhost:8081/instance/create \
  -H "apikey: leaex-evo-api-key-2026" \
  -H "Content-Type: application/json" \
  -d '{"instanceName":"wa_test_partner_01","qrcode":true,"integration":"WHATSAPP-BAILEYS"}'

# Get QR code (base64 PNG in response — open in browser or save to file)
curl http://localhost:8081/instance/connect/wa_test_partner_01 \
  -H "apikey: leaex-evo-api-key-2026"
```

The `base64` field in the response is a PNG QR code. Scan it with WhatsApp on the partner's phone.

### Step 8 — Smoke test

```bash
curl -X POST http://localhost:8080/message/send \
  -H "Content-Type: application/json" \
  -H "x-api-key: leaex-dev-key-2026" \
  -H "x-tenant-id: 40e571f6-d966-4d49-8a1a-750adff9df34" \
  -d '{"phone":"+919876543210","message":"Hello from wa_api!"}'

# Expected response:
# {"job_id":"...","status":"queued","scheduled_at":"..."}
```

---

## 5. Configuration Reference

All configuration is via environment variables. Values shown are defaults.

### wa_api services

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string. Format: `postgresql://user:pass@host:port/db` |
| `REDIS_URL` | Yes | — | Redis connection string. `redis://host:port` or `rediss://...` for TLS |
| `evo_BASE_URL` | Yes | — | Base URL of the evo API instance, e.g. `http://localhost:8081` |
| `evo_API_KEY` | Yes | — | Must match `AUTHENTICATION_API_KEY` in evo API |
| `SERVER_PORT` | No | `8080` | API Gateway listen port |
| `ADMIN_API_KEY` | Yes | — | Secret key for admin endpoints (`x-admin-key` header) |
| `WORKER_COUNT` | No | `4` | Number of concurrent send goroutines per worker process |
| `MIN_SEND_DELAY_SECS` | No | `8` | Minimum inter-message delay per instance (anti-ban) |
| `MAX_SEND_DELAY_SECS` | No | `15` | Maximum inter-message delay per instance (anti-ban) |
| `ALERT_WEBHOOK_URL` | No | — | Slack webhook URL for BANNED/QR_REQUIRED alerts |
| `RUST_LOG` | No | `info` | Log level: `error`, `warn`, `info`, `debug`, `trace` |

### Docker Compose `.env` overrides

When running with `docker compose`, create a `.env` in `wa_api/` with:

```env
POSTGRES_PASSWORD=your_strong_password
evo_API_KEY=your-evo-key
ADMIN_API_KEY=your-admin-key
evo_BASE_URL=http://evo_api:8080   # if running in same network
ALERT_WEBHOOK_URL=https://hooks.slack.com/services/...
WORKER_REPLICAS=4        # number of worker containers
GATEWAY_PORT=8080        # host port for API gateway
RUST_LOG=info
```

---

## 6. API Reference

Base URL: `http://your-host:8080`

All partner endpoints require:
- `x-api-key: <raw-api-key>` — your agency API key
- `x-tenant-id: <uuid>` — the tenant/partner UUID

### Authentication

The API key is SHA-256 hashed and compared against the `agencies.api_key` column. Store the **raw** key securely on the client side. Store only the **hash** in the database.

```bash
# Hash a key for storage in DB
echo -n "your-raw-api-key" | sha256sum | awk '{print $1}'
```

---

### POST /message/send

Send a single CRM message to a phone number via the partner's personal WhatsApp instance.

**Request**
```json
{
  "phone": "+919876543210",
  "message": "Hi {{name}}, your appointment is confirmed for {{time}}.",
  "scheduled_at": "2026-04-15T10:00:00Z"   // optional, omit for immediate
}
```

**Response 200**
```json
{
  "job_id": "171cf65a-6c2c-4d5f-9762-b16ae6e9fa37",
  "status": "queued",
  "scheduled_at": "2026-04-15T10:00:00Z"
}
```

**Error codes**
| Status | Meaning |
|---|---|
| 401 | Missing or invalid `x-api-key` |
| 400 | Missing `x-tenant-id` header |
| 403 | Account suspended / instance banned |
| 409 | Instance needs QR re-authentication |
| 422 | Invalid phone number or message too long (>4096 chars) |
| 200 `status: blocked_optout` | Recipient has opted out |
| 200 `status: spam_guard_blocked` | Daily/weekly limit reached for this number |

**Phone format**: `+91XXXXXXXXXX` (E.164 format). Indian numbers only in current validation.

---

### POST /campaign/start

Start a bulk campaign via pool numbers (Pro/Enterprise plans only).

**Request**
```json
{
  "name": "April Offers",
  "template_name": "april_promo_v1",
  "variables": ["Priya", "20% off"],
  "message_body": "Hi Priya, enjoy 20% off this week!",
  "recipients": ["+919876543210", "+919876543211"],
  "scheduled_at": "2026-04-15T09:00:00Z"   // optional
}
```

**Response 202**
```json
{
  "campaign_id": "abc123...",
  "status": "running",
  "total_recipients": 2,
  "enqueued": 2,
  "spam_guard_dropped": 0
}
```

**Notes**
- Recipients that hit weekly spam guard limit or 7-day template dedup are silently dropped and counted in `spam_guard_dropped`.
- Campaign messages always route through pool numbers — the partner's personal instance is never used.
- Requires `campaign_enabled = true` on the tenant row (Pro/Enterprise feature).

---

### GET /campaign/status/:id

Get campaign progress.

**Response 200**
```json
{
  "campaign_id": "abc123...",
  "status": "running",
  "sent_count": 150,
  "delivered_count": 142,
  "failed_count": 3,
  "deferred_count": 5
}
```

---

### POST /campaign/pause/:id

Pause a running campaign. Jobs already in the ready queue will complete; new promotions stop.

---

### POST /campaign/resume/:id

Resume a paused campaign.

---

### POST /campaign/cancel/:id

Cancel a campaign. Removes it from the active tracking set.

---

### GET /instance/health

Check the connection status of the partner's WhatsApp instance.

**Response 200**
```json
{
  "instance_name": "wa_glamour_studio_01",
  "wa_number": "+919876543210",
  "status": "OPEN",
  "cached_status": "active"
}
```

**Status values**: `OPEN` (connected), `CLOSE` (disconnected), `CONNECTING` (QR pending)

---

### GET /analytics/messages

Partner message history. Scoped to own tenant.

**Query params**: `page`, `limit` (max 200), `status`, `message_type`, `from`, `to`

---

### Admin Endpoints

Require `x-admin-key: <ADMIN_API_KEY>` header.

#### GET /admin/interactions

Cross-tenant interaction log.

**Query params**: `tenant_id`, `page`, `limit` (max 500), `status`, `message_type`

---

### POST /webhook/evo

**No auth required.** Called by evo API to deliver:
- `messages.update` — delivery receipts (SENT → DELIVERED → READ)
- `messages.upsert` — incoming messages; handles `STOP`/`UNSUBSCRIBE` keyword for platform-wide opt-out
- `connection.update` — instance state changes; updates Redis health cache

This URL must be configured as `WEBHOOK_GLOBAL_URL` in your evo API `.env`.

---

## 7. evo API Setup

evo API is the WhatsApp bridge layer. It manages the actual WhatsApp Web sessions.

### Creating a partner instance

```bash
# Create a new instance
curl -X POST http://localhost:8081/instance/create \
  -H "apikey: <evo_API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "instanceName": "wa_partner_salon_xyz",
    "qrcode": true,
    "integration": "WHATSAPP-BAILEYS"
  }'

# Get the QR code to scan
curl http://localhost:8081/instance/connect/wa_partner_salon_xyz \
  -H "apikey: <evo_API_KEY>"
# Response contains base64 QR image — decode and scan with WhatsApp
```

### Registering in wa_api

After the partner scans QR and connects:

```sql
-- Insert the partner as a tenant in wa_api's database
INSERT INTO tenants (id, agency_id, instance_name, wa_number, instance_status, campaign_enabled)
VALUES (
  gen_random_uuid(),
  '<agency_id>',
  'wa_partner_salon_xyz',     -- must match instanceName in evo API exactly
  '+919876543210',            -- the number that was connected
  'active',
  false                       -- set true for Pro/Enterprise
);
```

### Campaign pool numbers

Pool numbers are shared platform instances used only for bulk campaigns (never CRM).

```bash
# Register a pool number in evo API
curl -X POST http://localhost:8081/instance/create \
  -H "apikey: <evo_API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"instanceName":"pool_number_01","qrcode":true,"integration":"WHATSAPP-BAILEYS"}'

# After scanning QR, register it in wa_api's database
INSERT INTO pool_number_stats (instance_name, state, daily_limit, warmup_day)
VALUES ('pool_number_01', 'warming', 20, 0);
```

Pool numbers go through a **warmup sequence**:
- Days 1–24: daily limit increases by ~20 messages/day (20 → 500)
- Day 25+: graduates to `active` state with 500 msg/day limit
- Pool Manager automatically advances warmup counters every 15 minutes

---

## 8. Deployment

### Production with Docker Compose

**Step 1 — Create production `.env`**

```bash
# wa_api/
cat > .env << 'EOF'
POSTGRES_PASSWORD=<strong-random-password>
evo_API_KEY=<strong-random-key>
ADMIN_API_KEY=<strong-random-key>
evo_BASE_URL=http://evo_api:8080
ALERT_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../...
WORKER_REPLICAS=4
GATEWAY_PORT=8080
RUST_LOG=info
MIN_SEND_DELAY_SECS=8
MAX_SEND_DELAY_SECS=15
EOF
```

**Step 2 — Start wa_api stack**

```bash
cd ~/projects/wa_api
docker compose up -d

# Check all services are healthy
docker compose ps
docker compose logs -f --tail=50
```

**Step 3 — Start evo API**

```bash
# Create .env for evo
cat > .env << 'EOF'
EVO_POSTGRES_PASSWORD=<another-strong-password>
evo_API_KEY=<same-key-as-wa-api>
WA_API_WEBHOOK_URL=http://<wa-api-host>:8080
EVO_PORT=8081
EOF

docker compose -f docker-compose.evo.yml up -d
```

**Step 4 — Seed first agency**

```bash
KEY_HASH=$(echo -n "your-raw-api-key" | sha256sum | awk '{print $1}')

docker compose exec postgres psql -U wa_api -d wa_api -c "
INSERT INTO agencies (name, api_key, subscription_status, plan_tier, daily_msg_limit)
VALUES ('Your Agency', '$KEY_HASH', 'active', 'enterprise', 10000);
"
```

### Railway Deployment

Railway supports multi-service deployments from a monorepo.

1. Create a Railway project with a PostgreSQL plugin — note the `DATABASE_URL` it provides.
2. Create a Redis plugin — note the `REDIS_URL`.
3. Deploy each service separately using the respective `Dockerfile.*`:
   - `Dockerfile.gateway` → service `api_gateway`, set `PORT=8080`
   - `Dockerfile.worker` → service `worker`, set `WORKER_COUNT=1`, scale to 4 replicas
   - `Dockerfile.scheduler` → service `scheduler`
   - `Dockerfile.pool` → service `pool_manager`
   - `Dockerfile.health` → service `health_monitor`
4. Set all env vars in Railway dashboard (no `.env` file needed).
5. Set `DATABASE_URL` and `REDIS_URL` to the Railway-provided values.
6. Deploy evo API as a separate Railway project (or separate service) with its own Postgres.

**Apply schema on first deploy:**
```bash
railway run psql $DATABASE_URL -f migrations/001_initial_schema.sql
```

---

## 9. Scaling

### Worker scaling

Workers are stateless and horizontally scalable. Each worker process runs N concurrent send loops (set by `WORKER_COUNT`).

```bash
# Docker Compose — scale to 8 worker containers, each with 1 loop
docker compose up --scale worker=8 -d

# Or increase WORKER_COUNT per container (e.g. 2 containers × 4 loops = 8 concurrent)
WORKER_REPLICAS=2 docker compose up -d
```

**Concurrency safety**: Workers use a per-instance Redis SETNX lock (`send_lock:<instance>`, 30s TTL) so multiple workers can never send on the same evo API instance simultaneously.

### evo API scaling

Each evo API instance can handle multiple WhatsApp sessions (one per instance name).

To handle more agencies/partners:
```bash
# Scale to 3 evo API containers (behind a load balancer)
docker compose -f docker-compose.evo.yml up --scale evo_api=3 -d
```

**Important**: All evo API containers must share the same database so sessions are visible across containers. This is handled by the shared `evo_postgres` service.

### Database scaling

The bundled PostgreSQL is suitable for up to ~50k messages/day with standard hardware. For higher volume:
- Enable connection pooling with PgBouncer (add as a service in `docker-compose.yml`)
- Or migrate to a managed Postgres (AWS RDS, Railway, Neon) by updating `DATABASE_URL`

---

## 10. Database Schema

All tables live in the bundled PostgreSQL (`wa_api` database, `public` schema).

### agencies
Stores API credentials for each agency (reseller/platform customer).

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | PK |
| `name` | TEXT | Display name |
| `api_key` | TEXT UNIQUE | SHA-256 hash of the raw API key |
| `subscription_status` | TEXT | `active` / `suspended` / `trial` |
| `plan_tier` | TEXT | `basic` / `pro` / `enterprise` |
| `daily_msg_limit` | INTEGER | Max CRM messages per day |

### tenants
Each tenant is a salon partner with their own WhatsApp instance.

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | PK |
| `agency_id` | UUID | FK → agencies |
| `partner_id` | UUID | Links to Leaex partners table (nullable) |
| `instance_name` | TEXT UNIQUE | evo API instance identifier |
| `wa_number` | TEXT | Connected WhatsApp number (`+91XXXXXXXXXX`) |
| `instance_status` | TEXT | `active` / `qr_required` / `disconnected` / `banned` / `suspended` |
| `daily_crm_limit` | INTEGER | Max CRM messages per day for this tenant |
| `campaign_enabled` | BOOLEAN | Whether bulk campaigns are allowed |

### wa_campaigns
Campaign metadata.

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | PK |
| `tenant_id` | UUID | FK → tenants |
| `name` | TEXT | |
| `template_name` | TEXT | |
| `status` | TEXT | `draft` / `running` / `paused` / `completed` / `cancelled` |
| `total_recipients` | INTEGER | |
| `sent_count` | INTEGER | |
| `delivered_count` | INTEGER | |
| `failed_count` | INTEGER | |
| `pool_rotation_ids` | TEXT[] | Admin only — pool numbers used |

### wa_interaction_log
Immutable audit log of every message attempt.

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | PK |
| `tenant_id` | UUID | |
| `campaign_id` | UUID | Null for CRM messages |
| `message_type` | TEXT | `campaign` / `booking_confirm` / `reminder` / `birthday` / etc. |
| `recipient_phone_hash` | TEXT | SHA-256 of phone — no PII stored |
| `recipient_phone_masked` | TEXT | `+91 XXXXX X1234` |
| `instance_used` | TEXT | `leaex_pool` for campaigns, instance name for CRM |
| `status` | TEXT | `pending` / `sent` / `delivered` / `read` / `failed` / `deferred_spam` / `blocked_optout` / `duplicate` |
| `evo_msg_id` | TEXT | Message ID returned by evo API |
| `idempotency_key` | TEXT UNIQUE | Prevents duplicate sends |

### wa_customer_consent
Platform-wide and tenant-scoped opt-out registry.

| Column | Type | Notes |
|---|---|---|
| `phone_hash` | TEXT | SHA-256 of phone |
| `tenant_id` | UUID | NULL = platform-wide opt-out |
| `opted_out` | BOOLEAN | |
| `opted_out_source` | TEXT | `stop_keyword` / `partner_crm` / `customer_request` / `admin` |

**Unique constraint**: `(phone_hash, tenant_id)` — allows a number to opt out globally or per-partner.

### pool_number_stats
Warmup and health tracking for campaign pool numbers.

| Column | Type | Notes |
|---|---|---|
| `instance_name` | TEXT UNIQUE | |
| `state` | TEXT | `warming` / `active` / `cooling` / `flagged` / `resting` / `retired` |
| `daily_sent` | INTEGER | Resets at midnight IST |
| `daily_limit` | INTEGER | Increases during warmup |
| `warmup_day` | INTEGER | 0–24 during warmup, 25+ = active |
| `delivery_rate` | NUMERIC | Used for adaptive rate limiting |

---

## 11. Operations & Monitoring

### Checking service health

```bash
# All services running?
docker compose ps

# Recent logs from all services
docker compose logs --tail=100 -f

# Specific service
docker compose logs worker --tail=50 -f
docker compose logs api_gateway --tail=50 -f
```

### Checking the job queue

```bash
# Connect to Redis
docker compose exec redis redis-cli

# Count jobs in ready queue for a tenant
LLEN jobs:ready:<tenant_id>

# Count scheduled jobs
ZCARD jobs:scheduled:<tenant_id>

# Count DLQ (dead letter queue)
LLEN jobs:dlq:<tenant_id>

# Check instance health in Redis
GET instance_health:wa_test_partner_01

# Check spam guard count for a phone hash
GET spam_guard:<phone_hash>:today
```

### Checking the database

```bash
docker compose exec postgres psql -U wa_api -d wa_api

-- Recent interaction log
SELECT status, count(*) FROM wa_interaction_log
WHERE created_at > now() - interval '1 hour'
GROUP BY status;

-- Failed messages in last 24h
SELECT * FROM wa_interaction_log
WHERE status = 'failed' AND created_at > now() - interval '24 hours'
ORDER BY created_at DESC LIMIT 20;

-- Instance health events
SELECT * FROM instance_health_log
ORDER BY logged_at DESC LIMIT 20;

-- Pool number status
SELECT instance_name, state, daily_sent, daily_limit, warmup_day, delivery_rate
FROM pool_number_stats ORDER BY warmup_day DESC;
```

### Re-processing the DLQ

Jobs in the DLQ have a 7-day TTL. To re-enqueue them:

```bash
docker compose exec redis redis-cli

# Inspect DLQ item
LINDEX jobs:dlq:<tenant_id> 0

# Move a job back to the ready queue manually
RPOPLPUSH jobs:dlq:<tenant_id> jobs:ready:<tenant_id>
```

### Reconnecting a disconnected instance

When an instance shows `qr_required` or `disconnected`:

1. **Get a fresh QR:**
   ```bash
   curl http://localhost:8081/instance/connect/<instance_name> \
     -H "apikey: <evo_API_KEY>"
   ```
2. **Scan with WhatsApp** on the partner's phone.
3. **evo API fires a webhook** (`connection.update` → `open`) to `POST /webhook/evo`.
4. **Health Monitor** updates Redis: `instance_health:<instance_name>` → `ACTIVE`.
5. **Worker** resumes sending for that instance automatically.

If the webhook is missed, force-update Redis:
```bash
docker compose exec redis redis-cli SET instance_health:<instance_name> ACTIVE
```

### Alerts

Health Monitor fires Slack alerts when:
- An instance transitions to `BANNED` — **critical, manual recovery needed**
- An instance transitions to `QR_REQUIRED` — **action required, scan QR**

Set `ALERT_WEBHOOK_URL` in your `.env` to enable this.

---

## 12. Anti-Ban Rules

These rules are enforced by the system. Do not bypass them.

| Rule | Value | Where enforced |
|---|---|---|
| Inter-message delay (normal) | 8–15s randomized | Worker: rate limiter |
| Inter-message delay (low delivery) | 15–25s | Worker: adaptive rate limiter |
| Per-instance concurrent sends | 1 | Worker: SETNX lock |
| Platform daily limit per number | 5 messages | Worker: spam guard check |
| Platform weekly limit per number | 15 messages | Scheduler + Campaign: pre-check |
| Max partner sources per number per day | 3 partners | Campaign: spam guard |
| 7-day template dedup (campaigns) | 1 send per template per number per 7 days | Campaign: dedup key |
| Campaign pool isolation | Campaigns NEVER use partner instances | Campaign route + Worker |

**Warmup schedule for new pool numbers:**
- Day 1–24: daily limit = `warmup_day × 20` messages (20 → 480)
- Day 25: graduates to `active` with 500 msg/day cap
- Pool Manager advances `warmup_day` counter every 15 minutes by checking if daily quota was used

---

## 13. Troubleshooting

### "No pool numbers available" on campaign start

The `pool:available` Redis set is empty.

**Fix:**
1. Verify pool instances are registered: `SELECT * FROM pool_number_stats;`
2. Check their state — only `active` instances are in the available set.
3. Pool Manager rebuilds the set every 15 minutes. Check its logs:
   ```bash
   docker compose logs pool_manager --tail=50
   ```
4. Manually add to available set while debugging:
   ```bash
   docker compose exec redis redis-cli SADD pool:available pool_number_01
   ```

### Worker keeps requeuing jobs (not sending)

The instance health in Redis is not `ACTIVE`.

**Diagnose:**
```bash
docker compose exec redis redis-cli GET instance_health:<instance_name>
# Returns: DISCONNECTED, QR_REQUIRED, BANNED, or ACTIVE
```

**Fix:**
- `DISCONNECTED` → scan QR or restart evo API instance
- `QR_REQUIRED` → force re-authentication via evo API manager
- `BANNED` → instance is banned from WhatsApp; retire it, register a replacement
- Missing key → Health Monitor hasn't run yet; wait 5min or set manually

### API returns 401 even with correct key

The API key hash in the DB doesn't match.

**Verify:**
```bash
# Hash your raw key
echo -n "your-raw-key" | sha256sum | awk '{print $1}'

# Compare with stored hash
docker compose exec postgres psql -U wa_api -d wa_api \
  -c "SELECT api_key FROM agencies WHERE name='Your Agency';"
```

Re-insert if mismatched.

### BRPOP timeout errors in worker logs

Upstash Redis (serverless) doesn't support blocking commands. The worker uses polling RPOP instead. This is already handled — these errors indicate an old worker binary is running.

**Fix:** Rebuild and restart worker.
```bash
cargo build --release -p worker
pkill -f target/release/worker
./target/release/worker &
```

### evo API returns 401 on send

The `evo_API_KEY` in `wa_api/.env` doesn't match `AUTHENTICATION_API_KEY` in `evo/.env`.

**Fix:** Set both to the same value and restart both services.

### "Instance disconnected" errors after evo API restart

evo API sessions are stored in the `instances/` volume. If the volume is lost, all sessions need re-authentication.

**With Docker Compose**, the `evo_instances` named volume persists across restarts. Never run `docker compose down -v` in production.

### Database connection errors on startup

Services wait for Postgres `healthcheck` before starting. If startup takes longer than expected:

```bash
docker compose logs postgres | tail -20
# Check for "database system is ready to accept connections"
```

Force recreate if corrupted:
```bash
docker compose down
docker volume rm wa_api_postgres_data   # ⚠️ destroys all data
docker compose up -d
```
