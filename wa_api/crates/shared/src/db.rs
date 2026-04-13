use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::debug;
use uuid::Uuid;

/// Direct Postgres client (bundled DB, no PostgREST layer).
#[derive(Clone)]
pub struct DbClient {
    pool: PgPool,
}

// ─── Row types ───────────────────────────────────────────────────────────────

#[derive(Debug, sqlx::FromRow, Clone)]
pub struct AgencyRow {
    pub id: Uuid,
    pub name: String,
    pub api_key: String,
    pub subscription_status: String,
    pub plan_tier: String,
    pub daily_msg_limit: i32,
}

#[derive(Debug, sqlx::FromRow, Clone)]
pub struct TenantRow {
    pub id: Uuid,
    pub agency_id: Uuid,
    pub partner_id: Option<Uuid>,
    pub instance_name: String,
    pub wa_number: Option<String>,
    pub instance_status: String,
    pub daily_crm_limit: i32,
    pub campaign_enabled: bool,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ConsentRow {
    pub phone_hash: String,
    pub tenant_id: Option<Uuid>,
    pub opted_out: bool,
}

// ─── Interaction log insert ───────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct InteractionLogInsert {
    pub tenant_id: Uuid,
    pub campaign_id: Option<Uuid>,
    pub message_type: String,
    pub recipient_phone_hash: String,
    pub recipient_phone: String,
    pub recipient_name: Option<String>,
    pub instance_used: String,
    pub status: String,
    pub evolution_msg_id: Option<String>,
    pub error_reason: Option<String>,
    pub retry_count: i16,
    pub scheduled_at: chrono::DateTime<chrono::Utc>,
    pub sent_at: Option<chrono::DateTime<chrono::Utc>>,
    pub idempotency_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CampaignStatusUpdate {
    pub status: String,
    pub sent_count: i32,
    pub delivered_count: i32,
    pub failed_count: i32,
    pub deferred_count: i32,
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct ContactRow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub phone: String,
    pub name: Option<String>,
    pub profile_pic_url: Option<String>,
    pub last_presence: Option<String>,
    pub last_seen_at: Option<chrono::DateTime<chrono::Utc>>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl DbClient {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await
            .map_err(|e| anyhow!("Failed to connect to Postgres: {}", e))?;
        Ok(DbClient { pool })
    }

    /// Expose pool for raw sqlx queries in route handlers.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ─── Auth: resolve API key to agency ─────────────────────────────────

    pub async fn get_agency_by_api_key_hash(&self, key_hash: &str) -> Result<Option<AgencyRow>> {
        debug!("DB agency lookup");
        let row = sqlx::query_as::<_, AgencyRow>(
            "SELECT id, name, api_key, subscription_status, plan_tier, daily_msg_limit \
             FROM agencies WHERE api_key = $1 LIMIT 1"
        )
        .bind(key_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_tenant(&self, tenant_id: &Uuid, agency_id: &Uuid) -> Result<Option<TenantRow>> {
        let row = sqlx::query_as::<_, TenantRow>(
            "SELECT id, agency_id, partner_id, instance_name, wa_number, instance_status, \
             daily_crm_limit, campaign_enabled \
             FROM tenants WHERE id = $1 AND agency_id = $2 LIMIT 1"
        )
        .bind(tenant_id)
        .bind(agency_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_tenant_by_instance_name(&self, instance_name: &str) -> Result<Option<TenantRow>> {
        let row = sqlx::query_as::<_, TenantRow>(
            "SELECT id, agency_id, partner_id, instance_name, wa_number, instance_status, \
             daily_crm_limit, campaign_enabled \
             FROM tenants WHERE instance_name = $1 LIMIT 1"
        )
        .bind(instance_name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    // ─── Consent ──────────────────────────────────────────────────────────

    pub async fn is_opted_out_platform(&self, phone_hash: &str) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM wa_customer_consent \
             WHERE phone_hash = $1 AND tenant_id IS NULL AND opted_out = true LIMIT 1"
        )
        .bind(phone_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn is_opted_out_tenant(&self, phone_hash: &str, tenant_id: &Uuid) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM wa_customer_consent \
             WHERE phone_hash = $1 AND tenant_id = $2 AND opted_out = true LIMIT 1"
        )
        .bind(phone_hash)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn upsert_opt_out(
        &self,
        phone_hash: &str,
        tenant_id: Option<&Uuid>,
        source: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO wa_customer_consent \
             (phone_hash, tenant_id, opted_out, opted_out_source, opted_out_at) \
             VALUES ($1, $2, true, $3, NOW()) \
             ON CONFLICT (phone_hash, tenant_id) DO UPDATE \
             SET opted_out = true, opted_out_source = $3, opted_out_at = NOW(), updated_at = NOW()"
        )
        .bind(phone_hash)
        .bind(tenant_id)
        .bind(source)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ─── Interaction log ──────────────────────────────────────────────────

    pub async fn insert_interaction(&self, log: &InteractionLogInsert) -> Result<()> {
        sqlx::query(
            "INSERT INTO wa_interaction_log \
             (tenant_id, campaign_id, message_type, recipient_phone_hash, recipient_phone, \
              recipient_name, instance_used, status, evolution_msg_id, error_reason, \
              retry_count, scheduled_at, sent_at, idempotency_key) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)"
        )
        .bind(log.tenant_id)
        .bind(log.campaign_id)
        .bind(&log.message_type)
        .bind(&log.recipient_phone_hash)
        .bind(&log.recipient_phone)
        .bind(&log.recipient_name)
        .bind(&log.instance_used)
        .bind(&log.status)
        .bind(&log.evolution_msg_id)
        .bind(&log.error_reason)
        .bind(log.retry_count)
        .bind(log.scheduled_at)
        .bind(log.sent_at)
        .bind(&log.idempotency_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_interaction_status(
        &self,
        evolution_msg_id: &str,
        status: &str,
        delivered_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE wa_interaction_log SET status = $1, delivered_at = $2 \
             WHERE evolution_msg_id = $3"
        )
        .bind(status)
        .bind(delivered_at)
        .bind(evolution_msg_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_interaction_name(
        &self,
        phone: &str,
        name: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE wa_interaction_log SET recipient_name = $1 \
             WHERE recipient_phone = $2 AND recipient_name IS NULL"
        )
        .bind(name)
        .bind(phone)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ─── Campaign ─────────────────────────────────────────────────────────

    pub async fn update_campaign_counts(&self, campaign_id: &Uuid, update: &CampaignStatusUpdate) -> Result<()> {
        sqlx::query(
            "UPDATE wa_campaigns SET status=$1, sent_count=$2, delivered_count=$3, \
             failed_count=$4, deferred_count=$5, updated_at=NOW() WHERE id=$6"
        )
        .bind(&update.status)
        .bind(update.sent_count)
        .bind(update.delivered_count)
        .bind(update.failed_count)
        .bind(update.deferred_count)
        .bind(campaign_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ─── Instance health log ──────────────────────────────────────────────

    pub async fn log_instance_health_event(
        &self,
        instance_name: &str,
        tenant_id: Option<&Uuid>,
        is_pool: bool,
        event_type: &str,
        previous_status: &str,
        new_status: &str,
        detail: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO instance_health_log \
             (instance_name, tenant_id, is_pool, event_type, previous_status, new_status, detail, logged_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())"
        )
        .bind(instance_name)
        .bind(tenant_id)
        .bind(is_pool)
        .bind(event_type)
        .bind(previous_status)
        .bind(new_status)
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ─── Contacts / Presence ──────────────────────────────────────────────

    pub async fn upsert_contact(
        &self,
        tenant_id: &Uuid,
        phone: &str,
        name: Option<&str>,
        profile_pic_url: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO wa_contacts (tenant_id, phone, name, profile_pic_url, updated_at) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (tenant_id, phone) DO UPDATE \
             SET name = COALESCE($3, wa_contacts.name), \
                 profile_pic_url = COALESCE($4, wa_contacts.profile_pic_url), \
                 updated_at = NOW()"
        )
        .bind(tenant_id)
        .bind(phone)
        .bind(name)
        .bind(profile_pic_url)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_contact_presence(
        &self,
        tenant_id: &Uuid,
        phone: &str,
        presence: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE wa_contacts \
             SET last_presence = $1, last_seen_at = NOW(), updated_at = NOW() \
             WHERE tenant_id = $2 AND phone = $3"
        )
        .bind(presence)
        .bind(tenant_id)
        .bind(phone)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
