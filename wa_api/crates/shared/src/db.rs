use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

/// Direct Postgres client (bundled DB, no PostgREST layer).
#[derive(Clone)]
pub struct DbClient {
    pool: PgPool,
}

// ─── Row types ───────────────────────────────────────────────────────────────

#[derive(Debug, sqlx::FromRow, Clone)]
pub struct TenantRow {
    pub id: Uuid,
    pub partner_id: Option<Uuid>,
    pub instance_name: String,
    pub wa_number: Option<String>,
    pub instance_status: String,
    pub daily_crm_limit: i32,
    pub campaign_enabled: bool,
    pub is_active: bool,
    pub cleanup_notes: Option<String>,
    pub business_name: Option<String>,
    pub partner_name: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ConsentRow {
    pub phone: String,
    pub tenant_id: Option<Uuid>,
    pub opted_out: bool,
}

// ─── Interaction log insert ───────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct InteractionLogInsert {
    pub tenant_id: Uuid,
    pub campaign_id: Option<Uuid>,
    pub message_type: String,
    pub recipient_phone: String,
    pub recipient_name: Option<String>,
    pub instance_used: String,
    pub status: String,
    pub evo_msg_id: Option<String>,
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
        // 1. Initialize pool with conservative connection limits for Railway
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| anyhow!("Failed to connect to Postgres: {}", e))?;

        // 2. Automatically run migrations on startup
        // Path is relative to the crate root (crates/shared/)
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .map_err(|e| anyhow!("Failed to run database migrations: {}", e))?;

        Ok(DbClient { pool })
    }

    /// Expose pool for raw sqlx queries in route handlers.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ─── Tenant Management (Now Platform-direct) ─────────────────────────

    pub async fn get_tenant(&self, tenant_id: &Uuid) -> Result<Option<TenantRow>> {
        let row = sqlx::query_as::<_, TenantRow>(
            "SELECT id, partner_id, instance_name, wa_number, instance_status, \
             daily_crm_limit, campaign_enabled, is_active, cleanup_notes, \
             business_name, partner_name \
             FROM tenants WHERE id = $1 LIMIT 1",
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Alias for get_tenant — used by admin campaign routes.
    pub async fn get_tenant_by_id(&self, tenant_id: &Uuid) -> Result<Option<TenantRow>> {
        self.get_tenant(tenant_id).await
    }

    pub async fn get_tenant_by_partner_id(&self, partner_id: &Uuid) -> Result<Option<TenantRow>> {
        let row = sqlx::query_as::<_, TenantRow>(
            "SELECT id, partner_id, instance_name, wa_number, instance_status, \
             daily_crm_limit, campaign_enabled, is_active, cleanup_notes, \
             business_name, partner_name \
             FROM tenants WHERE partner_id = $1 LIMIT 1",
        )
        .bind(partner_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_tenant_by_instance_name(
        &self,
        instance_name: &str,
    ) -> Result<Option<TenantRow>> {
        let row = sqlx::query_as::<_, TenantRow>(
            "SELECT id, partner_id, instance_name, wa_number, instance_status, \
             daily_crm_limit, campaign_enabled, is_active, cleanup_notes, \
             business_name, partner_name \
             FROM tenants WHERE instance_name = $1 LIMIT 1",
        )
        .bind(instance_name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Alias for get_tenant_by_partner_id — maps to Supabase org_id.
    pub async fn get_tenant_by_org_id(&self, org_id: &Uuid) -> Result<Option<TenantRow>> {
        self.get_tenant_by_partner_id(org_id).await
    }

    pub async fn delete_tenant(&self, tenant_id: &Uuid) -> Result<()> {
        sqlx::query("DELETE FROM tenants WHERE id = $1")
            .bind(tenant_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ─── Consent ──────────────────────────────────────────────────────────

    pub async fn is_opted_out_platform(&self, phone: &str) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM wa_customer_consent \
             WHERE phone = $1 AND tenant_id IS NULL AND opted_out = true LIMIT 1",
        )
        .bind(phone)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn is_opted_out_tenant(&self, phone: &str, tenant_id: &Uuid) -> Result<bool> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM wa_customer_consent \
             WHERE phone = $1 AND tenant_id = $2 AND opted_out = true LIMIT 1",
        )
        .bind(phone)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn upsert_opt_out(
        &self,
        phone: &str,
        tenant_id: Option<&Uuid>,
        source: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO wa_customer_consent \
             (phone, tenant_id, opted_out, opted_out_source, opted_out_at) \
             VALUES ($1, $2, true, $3, NOW()) \
             ON CONFLICT (phone, tenant_id) DO UPDATE \
             SET opted_out = true, opted_out_source = $3, opted_out_at = NOW(), updated_at = NOW()",
        )
        .bind(phone)
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
             (tenant_id, campaign_id, message_type, recipient_phone, \
              recipient_name, instance_used, status, evo_msg_id, error_reason, \
              retry_count, scheduled_at, sent_at, idempotency_key) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
             ON CONFLICT (idempotency_key) DO UPDATE \
             SET status = EXCLUDED.status, \
                 evo_msg_id = COALESCE(EXCLUDED.evo_msg_id, wa_interaction_log.evo_msg_id), \
                 error_reason = COALESCE(EXCLUDED.error_reason, wa_interaction_log.error_reason), \
                 sent_at = COALESCE(EXCLUDED.sent_at, wa_interaction_log.sent_at), \
                 retry_count = EXCLUDED.retry_count",
        )
        .bind(log.tenant_id)
        .bind(log.campaign_id)
        .bind(&log.message_type)
        .bind(&log.recipient_phone)
        .bind(&log.recipient_name)
        .bind(&log.instance_used)
        .bind(&log.status)
        .bind(&log.evo_msg_id)
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
        evo_msg_id: &str,
        status: &str,
        delivered_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Option<Uuid>> {
        let row: Option<(Option<Uuid>,)> = sqlx::query_as(
            "UPDATE wa_interaction_log SET status = $1, delivered_at = $2 \
             WHERE evo_msg_id = $3 \
             RETURNING campaign_id",
        )
        .bind(status)
        .bind(delivered_at)
        .bind(evo_msg_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| r.0))
    }

    pub async fn update_interaction_name(&self, phone: &str, name: &str) -> Result<()> {
        sqlx::query(
            "UPDATE wa_interaction_log SET recipient_name = $1 \
             WHERE recipient_phone = $2 AND recipient_name IS NULL",
        )
        .bind(name)
        .bind(phone)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ─── Campaign ─────────────────────────────────────────────────────────

    pub async fn increment_campaign_counters(
        &self,
        campaign_id: &Uuid,
        sent: i32,
        delivered: i32,
        failed: i32,
    ) -> Result<()> {
        sqlx::query("SELECT increment_campaign_counters($1, $2, $3, $4)")
            .bind(campaign_id)
            .bind(sent)
            .bind(delivered)
            .bind(failed)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_campaign_counts(
        &self,
        campaign_id: &Uuid,
        update: &CampaignStatusUpdate,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE wa_campaigns SET status=$1, sent_count=$2, delivered_count=$3, \
             failed_count=$4, deferred_count=$5, updated_at=NOW() WHERE id=$6",
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
    #[allow(clippy::too_many_arguments)]
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

    // ─── Instance Management ──────────────────────────────────────────────

    pub async fn get_all_instance_names(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT instance_name FROM tenants WHERE is_active = true")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }

    pub async fn get_all_tenants(&self) -> Result<Vec<TenantRow>> {
        let rows = sqlx::query_as::<_, TenantRow>(
            "SELECT id, partner_id, instance_name, wa_number, instance_status, \
             daily_crm_limit, campaign_enabled, is_active, cleanup_notes, \
             business_name, partner_name \
             FROM tenants WHERE is_active = true",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn mark_tenant_orphan(&self, instance_name: &str, notes: &str) -> Result<()> {
        sqlx::query(
            "UPDATE tenants SET is_active = false, cleanup_notes = $1 \
             WHERE instance_name = $2",
        )
        .bind(notes)
        .bind(instance_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ─── Fuzzy Contact Search (pg_trgm) ───────────────────────────────────

    pub async fn search_contacts_fuzzy(
        &self,
        tenant_id: &Uuid,
        query: &str,
    ) -> Result<Vec<ContactRow>> {
        // If query looks like a phone number, use exact phone match
        if query.starts_with('+') || query.chars().all(|c| c.is_ascii_digit()) {
            let rows = sqlx::query_as::<_, ContactRow>(
                "SELECT id, tenant_id, phone, name, profile_pic_url, last_presence, last_seen_at, updated_at \
                 FROM wa_contacts WHERE tenant_id = $1 AND phone LIKE $2",
            )
            .bind(tenant_id)
            .bind(format!("%{}%", query))
            .fetch_all(&self.pool)
            .await?;
            return Ok(rows);
        }

        // Otherwise, use trigram similarity search by name
        let rows = sqlx::query_as::<_, ContactRow>(
            "SELECT id, tenant_id, phone, name, profile_pic_url, last_presence, last_seen_at, updated_at \
             FROM wa_contacts \
             WHERE tenant_id = $1 AND (name % $2 OR name ILIKE $3) \
             ORDER BY similarity(name, $2) DESC LIMIT 50",
        )
        .bind(tenant_id)
        .bind(query)
        .bind(format!("%{}%", query))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    // ─── Platform RPC Sync ────────────────────────────────────────────────

    pub async fn sync_to_platform_rpc(
        &self,
        platform_db: &PgPool,
        org_id: &Uuid,
        instance_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        sqlx::query("SELECT comms.handle_wa_api_event($1, $2, $3, $4)")
            .bind(org_id)
            .bind(instance_id)
            .bind(event_type)
            .bind(payload)
            .execute(platform_db)
            .await?;
        Ok(())
    }

    /// Fetches the business name and owner name for an organization from the platform database.
    pub async fn fetch_platform_partner_info(
        &self,
        platform_db: &PgPool,
        org_id: &Uuid,
    ) -> Result<(String, String)> {
        let row = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT 
                o.name as business_name,
                p.name as partner_name
            FROM orgs.organizations o
            JOIN public.profiles p ON o.owner_id = p.id
            WHERE o.id = $1
            "#,
        )
        .bind(org_id)
        .fetch_optional(platform_db)
        .await?;

        if let Some(r) = row {
            Ok((r.0, r.1))
        } else {
            Err(anyhow::anyhow!(
                "Organization {} not found in platform database",
                org_id
            ))
        }
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
                 updated_at = NOW()",
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
             WHERE tenant_id = $2 AND phone = $3",
        )
        .bind(presence)
        .bind(tenant_id)
        .bind(phone)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
