use anyhow::Result;
use fred::{
    clients::RedisPool,
    interfaces::*,
    prelude::*,
    types::{Expiration, MultipleZaddValues, SetOptions},
};
use futures::stream::StreamExt;
use serde::{de::DeserializeOwned, Serialize};

use crate::types::{InstanceHealth, WhatsAppJob};

#[derive(Clone)]
pub struct RedisClient {
    pool: RedisPool,
}

impl RedisClient {
    pub async fn new(url: &str) -> Result<Self> {
        let config = RedisConfig::from_url(url)?;
        let pool = RedisPool::new(config, None, None, Some(ReconnectPolicy::default()), 6)?;
        pool.connect();
        pool.wait_for_connect().await?;
        Ok(RedisClient { pool })
    }

    // ─── Generic helpers ─────────────────────────────────────────────────

    pub async fn set_ex<T: Serialize>(&self, key: &str, value: &T, ttl_secs: u64) -> Result<()> {
        let v = serde_json::to_string(value)?;
        let _: () = self
            .pool
            .set(key, v, Some(Expiration::EX(ttl_secs as i64)), None, false)
            .await?;
        Ok(())
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let raw: Option<String> = self.pool.get(key).await?;
        match raw {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    pub async fn get_string(&self, key: &str) -> Result<Option<String>> {
        Ok(self.pool.get(key).await?)
    }

    pub async fn set_string(&self, key: &str, value: &str) -> Result<()> {
        let _: () = self.pool.set(key, value, None, None, false).await?;
        Ok(())
    }

    pub async fn set_string_ex(&self, key: &str, value: &str, ttl_secs: u64) -> Result<()> {
        let _: () = self
            .pool
            .set(
                key,
                value,
                Some(Expiration::EX(ttl_secs as i64)),
                None,
                false,
            )
            .await?;
        Ok(())
    }

    pub async fn del(&self, key: &str) -> Result<()> {
        let _: () = self.pool.del(key).await?;
        Ok(())
    }

    pub async fn incr(&self, key: &str) -> Result<i64> {
        Ok(self.pool.incr(key).await?)
    }

    pub async fn incr_ex(&self, key: &str, ttl_secs: u64) -> Result<i64> {
        let count: i64 = self.pool.incr(key).await?;
        if count == 1 {
            let _: () = self.pool.expire(key, ttl_secs as i64).await?;
        }
        Ok(count)
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        let count: i64 = self.pool.exists(key).await?;
        Ok(count > 0)
    }

    /// SET NX EX — returns true if key was newly set.
    pub async fn set_nx_ex(&self, key: &str, value: &str, ttl_secs: u64) -> Result<bool> {
        let result: Option<String> = self
            .pool
            .set(
                key,
                value,
                Some(Expiration::EX(ttl_secs as i64)),
                Some(SetOptions::NX),
                false,
            )
            .await?;
        Ok(result.is_some())
    }

    // ─── Job queue operations ─────────────────────────────────────────────

    pub async fn zadd_scheduled(&self, tenant_id: &str, score: f64, job_id: &str) -> Result<()> {
        let key = format!("jobs:scheduled:{}", tenant_id);
        let values = MultipleZaddValues::try_from((score, job_id.to_string()))?;
        let _: () = self
            .pool
            .zadd(&key, None, None, false, false, values)
            .await?;
        Ok(())
    }

    pub async fn zrangebyscore_ready(&self, tenant_id: &str, now: f64) -> Result<Vec<String>> {
        let key = format!("jobs:scheduled:{}", tenant_id);
        let result: Vec<String> = self
            .pool
            .zrangebyscore(&key, 0.0_f64, now, false, None)
            .await?;
        Ok(result)
    }

    pub async fn zrem_scheduled(&self, tenant_id: &str, job_id: &str) -> Result<()> {
        let key = format!("jobs:scheduled:{}", tenant_id);
        let _: () = self.pool.zrem(&key, job_id).await?;
        Ok(())
    }

    pub async fn lpush_ready(&self, tenant_id: &str, job_json: &str) -> Result<()> {
        let key = format!("jobs:ready:{}", tenant_id);
        let _: () = self.pool.lpush(&key, job_json).await?;
        Ok(())
    }

    /// Poll-based pop (Upstash does not support blocking BRPOP).
    /// Tries each tenant's ready queue in order and returns the first job found.
    pub async fn brpop_ready(
        &self,
        tenant_ids: &[String],
        _timeout_secs: f64,
    ) -> Result<Option<(String, String)>> {
        if tenant_ids.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            return Ok(None);
        }
        for tenant_id in tenant_ids {
            let key = format!("jobs:ready:{}", tenant_id);
            let result: Option<String> = self.pool.rpop(&key, None).await?;
            if let Some(job_json) = result {
                return Ok(Some((key, job_json)));
            }
        }
        // No jobs found — short sleep before next poll cycle
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        Ok(None)
    }

    pub async fn lpush_dlq(&self, tenant_id: &str, job_json: &str) -> Result<()> {
        let key = format!("jobs:dlq:{}", tenant_id);
        let _: () = self.pool.lpush(&key, job_json).await?;
        let _: () = self.pool.expire(&key, 7 * 24 * 3600_i64).await?;
        Ok(())
    }

    // ─── Job metadata ─────────────────────────────────────────────────────

    pub async fn save_job(&self, job: &WhatsAppJob) -> Result<()> {
        let key = format!("job:{}", job.job_id);
        let json = serde_json::to_string(job)?;
        let _: () = self
            .pool
            .set(&key, json, Some(Expiration::EX(48 * 3600)), None, false)
            .await?;
        Ok(())
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Option<WhatsAppJob>> {
        let key = format!("job:{}", job_id);
        let raw: Option<String> = self.pool.get(&key).await?;
        match raw {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }

    // ─── Instance health ──────────────────────────────────────────────────

    pub async fn get_instance_health(&self, instance_name: &str) -> Result<InstanceHealth> {
        let key = format!("instance_health:{}", instance_name);
        let raw: Option<String> = self.pool.get(&key).await?;
        Ok(raw
            .and_then(|s| s.parse().ok())
            .unwrap_or(InstanceHealth::Disconnected))
    }

    pub async fn set_instance_health(
        &self,
        instance_name: &str,
        health: &InstanceHealth,
    ) -> Result<()> {
        let key = format!("instance_health:{}", instance_name);
        let _: () = self
            .pool
            .set(&key, health.as_str(), None, None, false)
            .await?;
        Ok(())
    }

    // ─── Rate limiting ────────────────────────────────────────────────────

    pub async fn get_last_sent(&self, instance_name: &str) -> Result<Option<i64>> {
        let key = format!("rate_limit:{}:last_sent", instance_name);
        let raw: Option<String> = self.pool.get(&key).await?;
        Ok(raw.and_then(|s| s.parse().ok()))
    }

    pub async fn set_last_sent(&self, instance_name: &str, timestamp: i64) -> Result<()> {
        let key = format!("rate_limit:{}:last_sent", instance_name);
        let _: () = self
            .pool
            .set(
                &key,
                timestamp.to_string(),
                Some(Expiration::EX(60)),
                None,
                false,
            )
            .await?;
        Ok(())
    }

    pub async fn incr_partner_daily(&self, tenant_id: &str, phone_hash: &str) -> Result<i64> {
        let key = format!("rate_limit:{}:{}:daily", tenant_id, phone_hash);
        self.incr_ex(&key, 24 * 3600).await
    }

    pub async fn get_partner_daily(&self, tenant_id: &str, phone_hash: &str) -> Result<i64> {
        let key = format!("rate_limit:{}:{}:daily", tenant_id, phone_hash);
        let raw: Option<String> = self.pool.get(&key).await?;
        Ok(raw.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    // ─── Spam guard ───────────────────────────────────────────────────────

    pub async fn spam_guard_incr_today(&self, phone_hash: &str) -> Result<i64> {
        let key = format!("spam_guard:{}:today", phone_hash);
        self.incr_ex(&key, 24 * 3600).await
    }

    pub async fn spam_guard_get_today(&self, phone_hash: &str) -> Result<i64> {
        let key = format!("spam_guard:{}:today", phone_hash);
        let raw: Option<String> = self.pool.get(&key).await?;
        Ok(raw.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn spam_guard_incr_week(&self, phone_hash: &str) -> Result<i64> {
        let key = format!("spam_guard:{}:week", phone_hash);
        self.incr_ex(&key, 7 * 24 * 3600).await
    }

    pub async fn spam_guard_get_week(&self, phone_hash: &str) -> Result<i64> {
        let key = format!("spam_guard:{}:week", phone_hash);
        let raw: Option<String> = self.pool.get(&key).await?;
        Ok(raw.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn spam_guard_add_partner_today(
        &self,
        phone_hash: &str,
        partner_hash: &str,
    ) -> Result<()> {
        let key = format!("spam_guard:{}:partners_today", phone_hash);
        let _: () = self.pool.sadd(&key, partner_hash).await?;
        let _: () = self.pool.expire(&key, 24 * 3600_i64).await?;
        Ok(())
    }

    pub async fn spam_guard_partner_count_today(&self, phone_hash: &str) -> Result<i64> {
        let key = format!("spam_guard:{}:partners_today", phone_hash);
        Ok(self.pool.scard(&key).await?)
    }

    // ─── Idempotency ──────────────────────────────────────────────────────

    pub async fn mark_sent(&self, idempotency_key: &str) -> Result<bool> {
        self.set_nx_ex(&format!("idem:{}", idempotency_key), "1", 48 * 3600)
            .await
    }

    pub async fn is_already_sent(&self, idempotency_key: &str) -> Result<bool> {
        self.exists(&format!("idem:{}", idempotency_key)).await
    }

    // ─── Campaign pool ────────────────────────────────────────────────────

    pub async fn pool_get_available(&self) -> Result<Vec<String>> {
        Ok(self.pool.smembers("pool:available").await?)
    }

    pub async fn pool_add_available(&self, instance_name: &str) -> Result<()> {
        let _: () = self.pool.sadd("pool:available", instance_name).await?;
        Ok(())
    }

    pub async fn pool_remove_available(&self, instance_name: &str) -> Result<()> {
        let _: () = self.pool.srem("pool:available", instance_name).await?;
        Ok(())
    }

    // ─── Active campaigns ─────────────────────────────────────────────────

    pub async fn campaigns_add_active(&self, campaign_id: &str, score: f64) -> Result<()> {
        let values = MultipleZaddValues::try_from((score, campaign_id.to_string()))?;
        let _: () = self
            .pool
            .zadd("campaigns:active", None, None, false, false, values)
            .await?;
        Ok(())
    }

    pub async fn campaigns_remove_active(&self, campaign_id: &str) -> Result<()> {
        let _: () = self.pool.zrem("campaigns:active", campaign_id).await?;
        Ok(())
    }

    // ─── Tenant scanning ─────────────────────────────────────────────────

    pub async fn scan_scheduled_tenants(&self) -> Result<Vec<String>> {
        let mut tenants = Vec::new();
        let mut stream = self
            .pool
            .next()
            .scan_buffered("jobs:scheduled:*", Some(100), None);
        while let Some(res) = stream.next().await {
            let key: fred::types::RedisKey = res?;
            if let Some(k_str) = key.as_str() {
                if let Some(tenant) = k_str.strip_prefix("jobs:scheduled:") {
                    tenants.push(tenant.to_string());
                }
            }
        }
        Ok(tenants)
    }

    pub async fn scan_ready_tenants(&self) -> Result<Vec<String>> {
        let mut tenants = Vec::new();
        let mut stream = self
            .pool
            .next()
            .scan_buffered("jobs:ready:*", Some(100), None);
        while let Some(res) = stream.next().await {
            let key: fred::types::RedisKey = res?;
            if let Some(k_str) = key.as_str() {
                if let Some(tenant) = k_str.strip_prefix("jobs:ready:") {
                    tenants.push(tenant.to_string());
                }
            }
        }
        Ok(tenants)
    }

    // ─── API key cache ────────────────────────────────────────────────────

    pub async fn cache_api_key<T: Serialize>(&self, api_key_hash: &str, ctx: &T) -> Result<()> {
        let key = format!("apikey:{}", api_key_hash);
        let json = serde_json::to_string(ctx)?;
        let _: () = self
            .pool
            .set(&key, json, Some(Expiration::EX(300)), None, false)
            .await?;
        Ok(())
    }

    pub async fn get_cached_api_key<T: DeserializeOwned>(
        &self,
        api_key_hash: &str,
    ) -> Result<Option<T>> {
        let key = format!("apikey:{}", api_key_hash);
        let raw: Option<String> = self.pool.get(&key).await?;
        match raw {
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
            None => Ok(None),
        }
    }
}
