# WhatsApp Interaction Taxonomy — Full Design (Upcoming)

> This document defines the **complete interaction taxonomy** for wa_api v4.0.
> Phase 1 (v3.1) implements only `direction` + `intent`. The rest is for future implementation.

---

## 1. Core Dimensions

### 1.1 Direction (✅ Implemented in v3.1)
Who initiated the interaction.

| Value | Description |
|-------|-------------|
| `inbound` | Customer → Business (received message) |
| `outbound` | Business → Customer (sent message) |

### 1.2 Intent (✅ Implemented in v3.1)
Business purpose of the interaction.

| Value | Description | Example |
|-------|-------------|---------|
| `support` | Customer support / help request | "I have a problem with my booking" |
| `sales` | Sales inquiry / lead | "What are your prices?" |
| `marketing` | Promotional / marketing outreach | Campaign blast, newsletter |
| `feedback` | Feedback collection / surveys | "How was your experience?" |
| `transactional` | Booking confirmations, receipts | "Your appointment is confirmed" |
| `re_engagement` | Win-back / dormant customer outreach | "We miss you! Here's 20% off" |
| `general` | Default / uncategorized | Anything that doesn't fit above |

---

## 2. Future Dimensions (Not in v3.1)

### 2.1 Priority
Urgency level for routing/SLA.

| Value | Use Case | SLA Target |
|-------|----------|------------|
| `critical` | Payment failures, cancellations, complaints | < 5 minutes |
| `high` | Active support requests, booking changes | < 15 minutes |
| `medium` | Sales inquiries, general questions | < 1 hour |
| `low` | Marketing responses, general feedback | < 4 hours |

### 2.2 Channel
Communication channel (for multi-channel future).

| Value | Description |
|-------|-------------|
| `whatsapp` | Default for wa_api |
| `sms` | Future: SMS fallback |
| `email` | Future: Email integration |
| `voice` | Future: Voice call |

### 2.3 Interaction Status
Lifecycle state of the interaction.

| Value | Description |
|-------|-------------|
| `open` | New, unhandled |
| `pending` | Awaiting response (from partner or customer) |
| `in_progress` | Being actively handled |
| `resolved` | Issue resolved |
| `closed` | Interaction complete |
| `escalated` | Escalated to higher tier |

### 2.4 Handling Mode
Who/what is handling the interaction.

| Value | Description |
|-------|-------------|
| `bot` | Fully automated (AI/template) |
| `hybrid` | Bot-initiated, human-monitored |
| `human` | Fully human-handled |
| `handover` | Transferred from bot → human |

### 2.5 Lifecycle Stage
Where in the customer journey this interaction falls.

| Value | Description |
|-------|-------------|
| `pre_sales` | Before first purchase/booking |
| `conversion` | During purchase/booking flow |
| `post_sales` | After purchase (onboarding, support) |
| `retention` | Repeat engagement, loyalty |
| `win_back` | Re-engaging lapsed customers |

---

## 3. Schema Changes Required

When implementing the full taxonomy, add these columns to `wa_interaction_log`:

```sql
ALTER TABLE wa_interaction_log
  ADD COLUMN priority TEXT DEFAULT 'medium'
    CHECK (priority IN ('critical', 'high', 'medium', 'low')),
  ADD COLUMN channel TEXT DEFAULT 'whatsapp'
    CHECK (channel IN ('whatsapp', 'sms', 'email', 'voice')),
  ADD COLUMN interaction_status TEXT DEFAULT 'open'
    CHECK (interaction_status IN ('open', 'pending', 'in_progress', 'resolved', 'closed', 'escalated')),
  ADD COLUMN handling_mode TEXT DEFAULT 'bot'
    CHECK (handling_mode IN ('bot', 'hybrid', 'human', 'handover')),
  ADD COLUMN lifecycle_stage TEXT
    CHECK (lifecycle_stage IS NULL OR lifecycle_stage IN (
      'pre_sales', 'conversion', 'post_sales', 'retention', 'win_back'
    )),
  ADD COLUMN assigned_to UUID,
  ADD COLUMN resolved_at TIMESTAMPTZ,
  ADD COLUMN first_response_at TIMESTAMPTZ;
```

### New indexes:
```sql
CREATE INDEX idx_interaction_priority ON wa_interaction_log(priority);
CREATE INDEX idx_interaction_status ON wa_interaction_log(interaction_status);
CREATE INDEX idx_interaction_assigned ON wa_interaction_log(assigned_to) WHERE assigned_to IS NOT NULL;
CREATE INDEX idx_interaction_lifecycle ON wa_interaction_log(lifecycle_stage) WHERE lifecycle_stage IS NOT NULL;
```

---

## 4. API Impact

### New query params for analytics endpoint:
```
GET /analytics?direction=inbound&intent=support&priority=high&status=open
```

### New admin endpoints:
```
POST /admin/interaction/:id/assign     — Assign to agent
POST /admin/interaction/:id/escalate   — Escalate priority
POST /admin/interaction/:id/resolve    — Mark resolved
```

---

## 5. Auto-Classification Strategy

For inbound messages, use keyword-based classification initially:

```rust
fn classify_intent(message: &str) -> &str {
    let lower = message.to_lowercase();
    if lower.contains("help") || lower.contains("problem") || lower.contains("issue") {
        "support"
    } else if lower.contains("price") || lower.contains("cost") || lower.contains("buy") {
        "sales"
    } else if lower.contains("feedback") || lower.contains("rate") || lower.contains("review") {
        "feedback"
    } else {
        "general"
    }
}
```

Phase 2: Replace with LLM-based classification (Claude/GPT → Leaex AI layer).
