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

    pub async fn expire(&self, key: &str, ttl_secs: i64) -> Result<()> {
        let _: () = self.pool.expire(key, ttl_secs).await?;
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

    pub async fn is_locked(&self, key: &str) -> Result<bool> {
        self.exists(key).await
    }

    pub async fn wait_for_lock(&self, key: &str, max_wait_ms: u64) -> Result<()> {
        let start = std::time::Instant::now();
        while self.is_locked(key).await? {
            if start.elapsed().as_millis() >= max_wait_ms as u128 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Ok(())
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

    /// R3: Reliable pop using LMOVE to move job to a processing list.
    pub async fn reliable_pop(
        &self,
        tenant_id: &str,
        worker_id: usize,
    ) -> Result<Option<String>> {
        let source = format!("jobs:ready:{}", tenant_id);
        let dest = format!("jobs:processing:{}", worker_id);

        let job_id: Option<String> = self.pool.custom(
            fred::types::CustomCommand::new_static("LMOVE", None, false),
            vec![source, dest, "RIGHT".to_string(), "LEFT".to_string()]
        ).await?;

        Ok(job_id)
    }

    pub async fn remove_processing(&self, worker_id: usize, job_id: &str) -> Result<()> {
        let key = format!("jobs:processing:{}", worker_id);
        let _: () = self.pool.lrem(&key, 1, job_id).await?;
        Ok(())
    }

    pub async fn lpop_processing(&self, list_key: &str) -> Result<Option<String>> {
        Ok(self.pool.lpop(list_key, None).await?)
    }

    /// Scan all worker processing lists for recovery.
    pub async fn scan_processing_lists(&self) -> Result<Vec<String>> {
        let mut lists = Vec::new();
        let mut stream = self
            .pool
            .next()
            .scan_buffered("jobs:processing:*", Some(100), None);
        while let Some(res) = stream.next().await {
            let key: fred::types::RedisKey = res?;
            if let Some(k_str) = key.as_str() {
                lists.push(k_str.to_string());
            }
        }
        Ok(lists)
    }

    pub async fn lpush_ready(&self, tenant_id: &str, job_json: &str) -> Result<()> {
        let key = format!("jobs:ready:{}", tenant_id);
        let _: () = self.pool.lpush(&key, job_json).await?;
        Ok(())
    }

    pub async fn lpush_dlq(&self, tenant_id: &str, job_json: &str) -> Result<()> {
        let key = format!("jobs:dlq:{}", tenant_id);
        let _: () = self.pool.lpush(&key, job_json).await?;
        let _: () = self.pool.expire(&key, 7 * 24 * 3600_i64).await?;
        Ok(())
    }

    pub async fn queue_len_ready(&self, tenant_id: &str) -> Result<usize> {
        let key = format!("jobs:ready:{}", tenant_id);
        let len: usize = self.pool.llen(&key).await?;
        Ok(len)
    }

    /// R2: Atomic move from scheduled ZSET to ready LIST using Lua.
    pub async fn move_scheduled_to_ready(&self, tenant_id: &str, now: f64) -> Result<usize> {
        let scheduled_key = format!("jobs:scheduled:{}", tenant_id);
        let ready_key = format!("jobs:ready:{}", tenant_id);

        let script = r#"
            local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
            for _, job_id in ipairs(jobs) do
                redis.call('LPUSH', KEYS[2], job_id)
                redis.call('ZREM', KEYS[1], job_id)
            end
            return #jobs
        "#;

        // Using custom command for EVAL to avoid trait/type issues
        let moved_count: usize = self.pool.custom(
            fred::types::CustomCommand::new_static("EVAL", None, false),
            vec![
                script.to_string(),
                "2".to_string(),
                scheduled_key,
                ready_key,
                now.to_string(),
            ]
        ).await?;

        Ok(moved_count)
    }

    // ─── Campaign Lifecycle (Lua) ──────────────────────────────────────────

    pub async fn move_jobs_to_paused(&self, tenant_id: &str, campaign_id: &str) -> Result<usize> {
        let ready_key = format!("jobs:ready:{}", tenant_id);
        let paused_key = format!("jobs:paused:{}:{}", tenant_id, campaign_id);

        // Lua script to iterate through ready list and move matching campaign jobs.
        // This is a O(N) operation on the ready list.
        let script = r#"
            local ready_key = KEYS[1]
            local paused_key = KEYS[2]
            local campaign_id = ARGV[1]
            local moved = 0
            
            local jobs = redis.call('LRANGE', ready_key, 0, -1)
            -- Clear ready list temporarily
            redis.call('DEL', ready_key)
            
            for _, job_id in ipairs(jobs) do
                -- Fetch job metadata to check campaign_id
                local job_meta = redis.call('GET', 'job:' .. job_id)
                if job_meta and string.find(job_meta, campaign_id) then
                    redis.call('LPUSH', paused_key, job_id)
                    moved = moved + 1
                else
                    -- Put back in ready list
                    redis.call('RPUSH', ready_key, job_id)
                end
            end
            return moved
        "#;

        let moved_count: usize = self.pool.custom(
            fred::types::CustomCommand::new_static("EVAL", None, false),
            vec![
                script.to_string(),
                "2".to_string(),
                ready_key,
                paused_key,
                campaign_id.to_string(),
            ]
        ).await?;

        Ok(moved_count)
    }

    pub async fn move_jobs_to_ready(&self, tenant_id: &str, campaign_id: &str) -> Result<usize> {
        let ready_key = format!("jobs:ready:{}", tenant_id);
        let paused_key = format!("jobs:paused:{}:{}", tenant_id, campaign_id);

        let script = r#"
            local ready_key = KEYS[1]
            local paused_key = KEYS[2]
            local moved = 0
            
            while true do
                local job_id = redis.call('RPOP', paused_key)
                if not job_id then break end
                redis.call('LPUSH', ready_key, job_id)
                moved = moved + 1
            end
            return moved
        "#;

        let moved_count: usize = self.pool.custom(
            fred::types::CustomCommand::new_static("EVAL", None, false),
            vec![
                script.to_string(),
                "2".to_string(),
                ready_key,
                paused_key,
            ]
        ).await?;

        Ok(moved_count)
    }

    pub async fn purge_campaign_jobs(&self, tenant_id: &str, campaign_id: &str) -> Result<usize> {
        let ready_key = format!("jobs:ready:{}", tenant_id);
        let paused_key = format!("jobs:paused:{}:{}", tenant_id, campaign_id);

        let script = r#"
            local ready_key = KEYS[1]
            local paused_key = KEYS[2]
            local campaign_id = ARGV[1]
            local purged = 0
            
            -- Purge paused list
            local paused_count = redis.call('LLEN', paused_key)
            redis.call('DEL', paused_key)
            purged = purged + paused_count
            
            -- Purge from ready list
            local jobs = redis.call('LRANGE', ready_key, 0, -1)
            redis.call('DEL', ready_key)
            
            for _, job_id in ipairs(jobs) do
                local job_meta = redis.call('GET', 'job:' .. job_id)
                if job_meta and string.find(job_meta, campaign_id) then
                    purged = purged + 1
                else
                    redis.call('RPUSH', ready_key, job_id)
                end
            end
            return purged
        "#;

        let purged_count: usize = self.pool.custom(
            fred::types::CustomCommand::new_static("EVAL", None, false),
            vec![
                script.to_string(),
                "2".to_string(),
                ready_key,
                paused_key,
                campaign_id.to_string(),
            ]
        ).await?;

        Ok(purged_count)
    }

    // ─── Opt-out cache ────────────────────────────────────────────────────

    pub async fn cache_opt_out(&self, phone_hash: &str, status: bool) -> Result<()> {
        let key = format!("opt_out:{}", phone_hash);
        let val = if status { "1" } else { "0" };
        let _: () = self
            .pool
            .set(&key, val, Some(Expiration::EX(24 * 3600)), None, false)
            .await?;
        Ok(())
    }

    pub async fn get_cached_opt_out(&self, phone_hash: &str) -> Result<Option<bool>> {
        let key = format!("opt_out:{}", phone_hash);
        let raw: Option<String> = self.pool.get(&key).await?;
        match raw {
            Some(s) => Ok(Some(s == "1")),
            None => Ok(None),
        }
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

    /// R6: Atomic spam guard increment and check using Lua.
    pub async fn spam_guard_check_and_incr(
        &self,
        phone_hash: &str,
        limit: i64,
        ttl_secs: u64,
    ) -> Result<bool> {
        let key = format!("spam_guard:{}:today", phone_hash);

        let script = r#"
            local key = KEYS[1]
            local limit = tonumber(ARGV[1])
            local ttl = tonumber(ARGV[2])
            local current = redis.call('INCR', key)
            if current == 1 then
                redis.call('EXPIRE', key, ttl)
            end
            if current > limit then
                redis.call('DECR', key)  -- roll back
                return 0  -- blocked
            end
            return 1  -- allowed
        "#;

        let result: i32 = self.pool.custom(
            fred::types::CustomCommand::new_static("EVAL", None, false),
            vec![
                script.to_string(),
                "1".to_string(),
                key,
                limit.to_string(),
                ttl_secs.to_string(),
            ]
        ).await?;

        Ok(result == 1)
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

    /// Track pool instance names in a Redis SET (supports per-instance key pattern).
    pub async fn pool_add_instance_name(&self, name: &str) -> Result<()> {
        let _: () = self.pool.sadd("pool:instance_names", name).await?;
        Ok(())
    }

    /// Get all tracked pool instance names.
    pub async fn pool_get_instance_names(&self) -> Result<Vec<String>> {
        Ok(self.pool.smembers("pool:instance_names").await?)
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

    // ─── CRM Daily Counter ────────────────────────────────────────────────────

    /// Increment the daily CRM send counter for a tenant. Uses IST midnight TTL.
    pub async fn incr_crm_daily(&self, tenant_id: &str) -> Result<i64> {
        let key = format!("crm_daily:{}", tenant_id);
        let count: i64 = self.pool.incr(&key).await?;
        // Set TTL only on first increment (when count == 1)
        if count == 1 {
            let ttl = crate::utils::seconds_until_ist_midnight();
            let _: () = self.pool.expire(&key, ttl as i64).await?;
        }
        Ok(count)
    }

    /// Get the current daily CRM send count for a tenant.
    pub async fn get_crm_daily(&self, tenant_id: &str) -> Result<i64> {
        let key = format!("crm_daily:{}", tenant_id);
        let count: Option<i64> = self.pool.get(&key).await?;
        Ok(count.unwrap_or(0))
    }
}
