**LEAEX**

**WhatsApp Automation Engine \-- Architecture Specification**

*Rust API Gateway \| Multi-Tenant Queue \| Campaign Pool Numbers \|
Cross-Partner Spam Guard*

  ----------------------------------------------------------------------------------
  **Attribute**                       **Value**
  ----------------------------------- ----------------------------------------------
  Document                            Leaex_WhatsApp_Architecture_v3.0

  Supersedes                          Leaex_WhatsApp_Integration_v2_Amendment.docx

  Architecture                        Rust (Axum + Tokio) + Redis + Supabase +
                                      evo API

  Key Additions                       Campaign pool numbers, cross-partner spam
                                      guard, partner visibility scoping

  Version                             v3.0 \-- April 2026

  Classification                      Internal \-- Engineering & Product Teams
  ----------------------------------------------------------------------------------

# 0. evo API \-- Known Constraints & Why This Architecture Solves Them

evo API is a self-hosted WhatsApp Web bridge. It is cost-effective
and gives per-partner instance isolation, but it has hard operational
constraints that shaped every architectural decision in this document.
Understanding these constraints is mandatory before reading the rest of
the spec.

## 0.1 evo API Constraint Register

  --------------------------------------------------------------------------
  **Constraint**    **Root Cause**    **Severity**      **How This
                                                        Architecture
                                                        Mitigates It**
  ----------------- ----------------- ----------------- --------------------
  Session is a      evo API     HIGH              Campaign pool
  WhatsApp Web      connects via                        numbers use official
  browser session   WhatsApp Web                        WABA for broadcast.
  \-- not the       protocol. Meta                      Partner personal
  official Business does not sanction                   numbers only used
  API               this.                               for 1:1 CRM messages
                                                        within safe rate
                                                        limits.

  No native message evo has no  HIGH              All sends go through
  queue \--         internal queue.                     our Rust worker
  evo API     Concurrent POSTs                    pool + Redis ZSET
  sends immediately flood the session                   scheduler. evo
  or drops          and trigger bans.                   API never receives
                                                        concurrent requests
                                                        for the same
                                                        instance.

  Instance ban if   Meta bot          CRITICAL          Per-instance Rust
  messages sent too detection                           rate limiter
  fast              triggers on send                    enforces 8-15s
                    velocity \> 1 msg                   randomized delay.
                    per 8-15 sec on                     Adaptive backoff
                    unverified                          increases delay when
                    numbers.                            delivery failures
                                                        spike above
                                                        threshold.

  Instance crashes  evo API     HIGH              Worker pool polls
  are silent        process dies                        instance health
                    without emitting                    endpoint before each
                    an error to the                     send batch. Dead
                    caller. Sends                       instance jobs are
                    appear to succeed                   re-routed to Dead
                    but messages are                    Letter Queue, not
                    never delivered.                    retried on same
                                                        instance.

  No delivery       evo         MEDIUM            Supabase stores
  receipt webhook   WebSocket                           last-known status.
  reliability       disconnect causes                   Redis tracks
                    webhook events to                   in-flight sends.
                    be lost. Delivery                   Reconciliation
                    status becomes                      worker polls
                    stale.                              evo status
                                                        every 5 minutes for
                                                        in-flight records
                                                        older than 10
                                                        minutes.

  Multi-instance    Each evo    HIGH              Rust Tenant Resolver
  state not shared  API instance is                     maintains
                    an isolated                         instance-to-tenant
                    process. There is                   mapping in Redis.
                    no built-in                         All routing
                    cross-instance                      decisions go through
                    coordination.                       the resolver, never
                                                        hardcoded.

  QR re-auth        WhatsApp Web      MEDIUM            Session health
  required          sessions expire.                    monitor detects
  periodically      evo                           QR_REQUIRED state
                    requires QR                         and fires
                    rescan or pairing                   Slack/webhook alert
                    code re-entry.                      to partner. Sends
                                                        for that instance
                                                        pause automatically
                                                        until
                                                        re-authenticated.

  Bulk broadcast on Using the         CRITICAL          Campaign broadcasts
  partner number =  partner\'s own WA                   NEVER use the
  high ban risk     number for                          partner\'s personal
                    campaign blasts                     instance. All
                    mimics spam                         campaigns route
                    behavior.                           through the
                                                        platform-managed
                                                        Campaign Pool
                                                        Numbers (see Section
                                                        3).
  --------------------------------------------------------------------------

**CRITICAL** *The two most dangerous failure modes are: (1) sending
campaigns through the partner\'s personal WA number \-- instant ban
risk; (2) tenant isolation failure \-- sending Client A\'s messages
through Client B\'s instance. Both are architectural safeguards, not
just code checks.*

**DESIGN DECISION** *This architecture deliberately separates three
number types: Partner Personal Instance (1:1 CRM only), Campaign Pool
Numbers (broadcasts), and Future WABA (official, for high-volume
verified partners). Each has different rate limits, ban risk profiles,
and routing rules.*

# 1. System Architecture Overview

## 1.1 High-Level Component Map

\[ Leaex Partner Dashboard / API \]

\|

x-api-key + tenant_id

\|

+\-\-\-\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\-\-\-\-\--+

\| Rust API Gateway (Axum) \|

\| auth / validation / routing \|

+\-\-\-\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\-\-\-\-\--+

\|

+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+

\| \| \|

\[Campaign Service\] \[Message Service\] \[Admin Service\]

\| \| \|

+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+

\|

+\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\--+

\| Tenant Resolver \| \<\-- CRITICAL SAFETY LAYER

\| API Key -\> Instance \|

+\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\--+

\|

+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+

\| Redis Layer \|

\| ZSET (scheduled) \|

\| LIST (ready queue per tenant) \|

\| HASH (job state) \|

\| STRING (rate limit counters) \|

+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+

\|

+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+

\| Rust Worker Pool (Tokio) \|

\| health check -\> rate limit -\> send \|

+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--+

\| \|

\[Partner Instance Pool\] \[Campaign Pool Numbers\]

(1:1 CRM messages only) (broadcasts / campaigns)

\| \|

evo API evo API

\| \|

WhatsApp WhatsApp

## 1.2 Number Type Taxonomy (Critical Design Decision)

  ------------------------------------------------------------------------------------------------
  **Number     **Managed By**   **Used For**           **Rate       **Ban Risk**     **Visible To
  Type**                                               Limit**                       Partner?**
  ------------ ---------------- ---------------------- ------------ ---------------- -------------
  Partner      Partner          1:1 CRM: booking       1 msg /      MEDIUM \--       Yes \--
  Personal     self-registers   confirmations,         10-20s. Max  controlled by    partner sees
  Instance     via evo    reminders,             200 unique   our rate         send history
               API QR scan.     birthday/anniversary   recipients   limiter.         from their
               Leaex manages    wishes, individual     per day.                      own number.
               the session.     follow-ups triggered                                 
                                by service events.                                   

  Campaign     Leaex platform   All bulk campaign      1 msg /      LOW \-- numbers  No \--
  Pool Numbers owns and         broadcasts. Any        12-20s per   are rotated and  partner sees
               manages. A pool  message to a recipient pool number. warmed up. Never campaign
               of numbers       list \> 20 numbers.    Load         used for         results
               registered as                           balanced     personal-style   (delivered,
               evo API                           across pool. messages.        read, failed
               instances, not                          Max 500                       counts) but
               linked to any                           msgs/day per                  NOT the
               specific                                pool number.                  originating
               partner.                                                              pool number.

  Future WABA  Leaex applies    High-volume verified   Per Meta     LOWEST \--       Partner sees
  (Official)   for Business API broadcasts.            WABA limits  official Meta    delivery
               access for       Template-only          (up to 100k  API, not         analytics
               high-volume      messages. Verified     msgs/day per WhatsApp Web     only.
               verified         green tick senders.    WABA number  bridge.          
               partners                                after                         
               (Enterprise                             tier-up).                     
               tier).                                                                
  ------------------------------------------------------------------------------------------------

**WARNING** *Campaign Pool Numbers are NEVER disclosed to partners in
the UI. If a partner asks \"what number will my campaign send from?\"
the answer is \"A Leaex platform number on your behalf.\" This protects
the pool from misuse and the partner from Meta scrutiny.*

# 2. Rust API Gateway (Axum)

## 2.1 Authentication & Request Flow

  ----------------------------------------------------------------------------
  **Step**                **Action**                   **Failure Behaviour**
  ----------------------- ---------------------------- -----------------------
  1\. Extract header      Read x-api-key from request  401 Unauthorized
                          header.                      immediately. No further
                                                       processing.

  2\. Tenant Resolve      Look up API key in Redis     403 Forbidden if key
                          cache (TTL 5 min). On miss:  not found or agency
                          query Supabase agencies      suspended.
                          table.                       

  3\. Tenant Validation   Confirm                      403 Forbidden. Log
                          agency.subscription_status = attempt to Supabase
                          active. Confirm requested    audit_log with
                          tenant_id belongs to this    agency_id + IP.
                          agency.                      

  4\. Instance Map        Resolve tenant -\>           503 if instance not
                          wa_instance_name -\>         found. 409 if instance
                          evo API instance.      in QR_REQUIRED state
                                                       (needs re-auth).

  5\. Payload Validation  Validate request body:       422 Unprocessable
                          required fields, phone       Entity with field-level
                          format (+91XXXXXXXXXX),      error detail.
                          message length \<= 4096      
                          chars.                       

  6\. Route to Service    Pass validated               500 with request_id for
                          TenantContext + payload to   tracing.
                          the appropriate service      
                          (Campaign or Message).       
  ----------------------------------------------------------------------------

## 2.2 API Endpoints

  ------------------------------------------------------------------------------------
  **Method**     **Endpoint**           **Auth**       **Purpose**      **Rate Limit
                                                                        (per tenant)**
  -------------- ---------------------- -------------- ---------------- --------------
  POST           /message/send          x-api-key      Send a single    60 req/min
                                                       1:1 CRM message  
                                                       through          
                                                       partner\'s       
                                                       personal         
                                                       instance.        

  POST           /campaign/start        x-api-key      Create and       5 req/min
                                                       enqueue a        
                                                       campaign.        
                                                       Accepts          
                                                       template +       
                                                       recipient list.  

  GET            /campaign/status/:id   x-api-key      Poll campaign    120 req/min
                                                       progress: sent,  
                                                       failed, pending, 
                                                       delivery_rate.   

  POST           /campaign/pause/:id    x-api-key      Pause a running  10 req/min
                                                       campaign. Jobs   
                                                       already in ready 
                                                       queue are moved  
                                                       back to          
                                                       scheduled ZSET.  

  POST           /campaign/resume/:id   x-api-key      Resume a paused  10 req/min
                                                       campaign from    
                                                       where it         
                                                       stopped.         

  POST           /campaign/cancel/:id   x-api-key      Cancel a         10 req/min
                                                       campaign.        
                                                       Remaining jobs   
                                                       deleted. Sent    
                                                       records          
                                                       retained.        

  GET            /instance/health       x-api-key      Check partner\'s 30 req/min
                                                       WA instance      
                                                       connection       
                                                       status.          

  GET            /analytics/messages    x-api-key      Partner: own     30 req/min
                                                       messages only.   
                                                       Admin: all (with 
                                                       ?tenant_id       
                                                       filter).         

  GET            /admin/interactions    Admin key      Full             60 req/min
                                                       cross-tenant     (admin only)
                                                       interaction log. 
                                                       Paginated,       
                                                       filterable.      
  ------------------------------------------------------------------------------------

## 2.3 TenantContext Object (passed through all layers)

struct TenantContext {

agency_id: Uuid,

tenant_id: Uuid,

partner_id: Uuid, // maps to Leaex partner/salon

instance_name: String, // evo API instance identifier

wa_number: String, // +91XXXXXXXXXX of the connected WA number

plan_tier: PlanTier, // Basic \| Pro \| Enterprise

daily_limit: u32, // max msgs today (from plan)

campaign_allowed: bool, // Pro+ only

}

**CRITICAL** *TenantContext is constructed ONCE at the API Gateway auth
layer and passed immutably through the entire request pipeline. No
service downstream is permitted to modify or re-resolve the
instance_name. This is the primary tenant isolation guarantee.*

# 3. Campaign Pool Numbers (Anti-Ban Architecture)

## 3.1 Why Campaign Pool Numbers Exist

When a salon partner sends a campaign to 500 customers from their own
WhatsApp number, the behavior pattern is: one number, 500 different
recipients, messages sent within hours. Meta\'s detection systems
classify this as spam. The number gets soft-banned (limited sending)
then hard-banned within days. The partner\'s personal number \-- which
they use daily for business \-- is permanently damaged.

Campaign Pool Numbers are platform-managed, purpose-built numbers that
exist purely for broadcast. They are warmed up gradually, rotated
intelligently, and never used for personal conversations. Partners\'
personal instances are protected entirely.

## 3.2 Pool Architecture

  -----------------------------------------------------------------------
  **Component**                       **Specification**
  ----------------------------------- -----------------------------------
  Pool Size (Phase 1, 0-20 partners)  10 pool numbers. Each capable of
                                      500 msgs/day = 5,000 msgs/day total
                                      pool capacity.

  Pool Size (Phase 2, 20-100          30 pool numbers. Grouped in sets of
  partners)                           10 for load balancing.

  Number Type                         Regular SIM-registered Indian
                                      mobile numbers. NOT linked to any
                                      partner\'s name or business.

  Instance Management                 Each pool number has its own
                                      evo API instance on the
                                      platform server. Named:
                                      pool_leaex_01, pool_leaex_02, etc.

  Warm-Up Protocol                    New pool numbers start at 20
                                      msgs/day. Increase by 20/day over
                                      25 days to reach 500/day. Never add
                                      un-warmed numbers to active
                                      rotation.

  Rotation Logic                      Round-robin across warmed pool
                                      numbers per campaign. Same campaign
                                      never uses same number twice in \<
                                      48h.

  Health Check                        Pool Manager (Rust service) pings
                                      each pool instance every 15 min.
                                      Numbers with \> 3 failed sends in
                                      1h are pulled from rotation and
                                      flagged for review.

  Partner Attribution                 Each send from a pool number
                                      carries campaign_id + tenant_id in
                                      job metadata. Partner sees
                                      analytics linked to their campaign,
                                      not to the pool number.
  -----------------------------------------------------------------------

## 3.3 Campaign Routing Decision Tree

fn route_campaign_job(job: &CampaignJob) -\> Instance {

// Step 1: Is this a campaign (bulk) or a 1:1 CRM message?

match job.message_type {

MessageType::Campaign =\> {

// ALWAYS route to pool, never to partner instance

pool_manager.get_available_instance()

// pool_manager applies:

// - round-robin across warmed numbers

// - skips instances in cooldown or health-check-fail

// - skips if daily_sent \>= daily_limit for that number

}

MessageType::CrmDirect =\> {

// 1:1 messages: use partner personal instance

// (booking confirm, reminder, birthday, manual from CRM)

tenant_resolver.get_partner_instance(job.tenant_id)

}

}

}

**DESIGN DECISION** *This single routing decision \-- campaign vs CRM
\-- is the most important anti-ban measure in the architecture. It keeps
the partner\'s personal number clean, the pool numbers purpose-built,
and the risk profile of each number type predictable.*

## 3.4 Pool Number Lifecycle

  -----------------------------------------------------------------------
  **State**               **Trigger**             **Behaviour**
  ----------------------- ----------------------- -----------------------
  WARMING                 New number added to     Restricted to 20
                          pool.                   msgs/day. Not used for
                                                  real campaigns \-- only
                                                  internal test sends.
                                                  Warms for 25 days.

  ACTIVE                  Warmup complete. Daily  Available for campaign
                          quota \>= 500.          rotation. Subject to
                                                  round-robin selection.

  COOLING                 Sent \>= 80% of daily   Removed from rotation
                          quota in \< 6 hours     for remainder of day.
                          (unusual velocity).     Resumes next calendar
                                                  day.

  FLAGGED                 \> 3 failed sends in 1h Removed from rotation.
                          OR delivery rate \< 60% Alert fired to ops
                          in last 100 sends.      team. Manual review
                                                  required before
                                                  reactivation.

  RESTING                 Voluntarily pulled for  Not available for
                          48h rotation gap.       selection. Prevents
                                                  pattern detection from
                                                  sustained daily use.

  RETIRED                 Number permanently      Removed from pool
                          disconnected, banned by entirely. Replaced by
                          Meta, or SIM expired.   new warm-up number.
  -----------------------------------------------------------------------

# 4. Tenant Isolation & Data Visibility Rules

## 4.1 The Three Isolation Guarantees

**CRITICAL** *These three guarantees must never be broken. Any
architecture change must be evaluated against all three.*

  ---------------------------------------------------------------------------
  **Guarantee**           **What It Means**       **Enforcement Point**
  ----------------------- ----------------------- ---------------------------
  Message Isolation       A message for Partner   Tenant Resolver at API
                          A\'s customer is NEVER  Gateway. TenantContext
                          sent through Partner    carried immutably through
                          B\'s WA instance.       worker. Instance validation
                                                  at send time.

  Data Isolation          Partner A cannot query, Supabase RLS policies on
                          view, or receive any    all
                          data belonging to       message/campaign/customer
                          Partner B\'s customers  tables. API layer enforces
                          or interactions.        tenant_id filter on all
                                                  reads.

  Campaign Attribution    Even though campaigns   Job schema always carries
                          use shared pool         tenant_id. Analytics
                          numbers, each           queries always filter by
                          message\'s metadata     tenant_id. Pool number
                          traces back to exactly  identity never surfaced.
                          one tenant_id +         
                          campaign_id.            
  ---------------------------------------------------------------------------

## 4.2 Admin Visibility (Leaex HQ)

Admin users (Leaex internal team) have unrestricted read access across
all tenants. All interactions, campaigns, delivery statuses, and audit
events are visible.

  -----------------------------------------------------------------------
  **Admin Can See**       **Source Table**        **Notes**
  ----------------------- ----------------------- -----------------------
  All WA messages ever    wa_interaction_log      Full log: tenant_id,
  sent (any partner, any                          partner_id, recipient
  type)                                           phone, message type,
                                                  status, timestamps,
                                                  pool/personal number
                                                  used (masked last 4
                                                  digits).

  All campaign records    wa_campaigns            Status, total sent,
  across all partners                             delivery rate,
                                                  pause/resume history.

  Cross-partner customer  wa_interaction_log +    Aggregated by phone
  contact history         customer_profiles       number to detect
                                                  cross-partner spam.
                                                  Visible in admin spam
                                                  guard dashboard.

  Instance health history instance_health_log     QR events,
                                                  disconnections, ban
                                                  flags per instance per
                                                  tenant.

  Rate limit breach       rate_limit_events       Any tenant that hit
  events                                          rate limit ceiling.
                                                  With timestamp and
                                                  message count.

  Audit log (security     audit_log               Failed auth attempts,
  events)                                         cross-tenant access
                                                  attempts, API key
                                                  misuse.

  Pool number analytics   pool_number_stats       Per pool number: daily
                                                  sent, delivery rate,
                                                  health state, rotation
                                                  history.
  -----------------------------------------------------------------------

## 4.3 Partner Visibility Rules

Partners have strictly scoped visibility. The scoping is enforced at
both the Supabase RLS layer and the API response filter \-- two
independent layers so a bug in one does not expose the other.

  -----------------------------------------------------------------------
  **What Partner Can      **Scope Rule**          **Rationale**
  See**                                           
  ----------------------- ----------------------- -----------------------
  CRM direct messages     Only messages where     These are sent from the
  (booking confirmations, tenant_id = partner\'s  partner\'s own WA
  reminders, birthday     own tenant_id AND       number to their own
  wishes)                 message_type IN         customers. Full
                          (booking_confirm,       visibility appropriate.
                          reminder, birthday,     
                          anniversary,            
                          manual_crm).            

  Campaign sends and      Only campaigns where    Partner created the
  delivery analytics      campaign.tenant_id =    campaign, they own the
                          partner\'s tenant_id.   result data. Pool
                          Can see: sent count,    number used is NOT
                          delivered count, read   shown \-- only \"sent
                          count, failed count,    via Leaex Platform.\"
                          per-recipient status    
                          for their own customer  
                          list.                   

  Customer interaction    A customer\'s message   Critical for privacy
  history (within partner history visible to      and competitive
  context)                Partner A ONLY includes isolation. Partners
                          interactions that       must not know which
                          originated from Partner other Leaex partners a
                          A\'s tenant_id. If the  customer uses.
                          same customer visited   
                          Partner B, Partner A    
                          cannot see those        
                          interactions.           

  Customer consent status Can see own customers\' DPDP compliance.
                          WA opt-in/opt-out       Partner is responsible
                          status. Cannot see      for their own customer
                          consent records from    consent.
                          other tenants.          

  Manual contact history  Any message a partner   These are explicit
                          manually sent from the  partner-initiated
                          CRM (not automated, not messages. Full
                          campaign) to a specific visibility for
                          customer.               accountability.
  -----------------------------------------------------------------------

  -----------------------------------------------------------------------
  **What Partner CANNOT See**         **Why**
  ----------------------------------- -----------------------------------
  The pool number(s) used to send     Prevents partners from misusing
  their campaign                      pool number identity. Prevents
                                      competitive partners from
                                      contacting each other\'s pool
                                      numbers.

  Message interactions from other     Customer privacy and competitive
  partners with the same customer     isolation.

  Other partners\' campaign           Trade secret and privacy
  structures, templates, or target    protection.
  lists                               

  Global cross-partner spam scores    Spam scoring is an admin-only tool
  for customers                       to manage platform health, not a
                                      partner feature.

  Admin audit log entries             Internal Leaex operations.
  -----------------------------------------------------------------------

# 5. Cross-Partner Customer Spam Prevention

## 5.1 The Problem

A customer who visits 5 different Leaex partner salons could
theoretically receive automated WA messages from all 5. Each partner
individually stays within their own 30 msg/day limit, but the customer
receives 150 messages/day from \"Leaex Platform numbers.\" This destroys
the customer experience, generates spam reports, and risks Meta blocking
the platform\'s pool numbers entirely.

## 5.2 Platform-Level Spam Guard (Cross-Tenant)

The platform maintains a global contact frequency register keyed by
recipient phone number. This is the only cross-tenant data store. It
contains NO customer PII beyond phone number hashes \-- it is purely a
rate-limiting structure.

// Redis key structure for global spam guard

// Key: spam_guard:{sha256(phone_number)}

// Value: HASH {

// today_count: u32 \-- total platform messages today

// today_partner_count: u32 \-- unique partners who messaged today

// last_msg_at: timestamp

// week_count: u32

// partner_ids: SET \-- hashed partner IDs who sent this week (no PII)

// }

// TTL: 24h for daily keys, 7d for weekly keys

## 5.3 Spam Guard Rules

  --------------------------------------------------------------------------------------------------------------
  **Rule**          **Limit**         **Enforcement**                              **Action on Breach**
  ----------------- ----------------- -------------------------------------------- -----------------------------
  Global daily      Max 5 messages    Checked in Worker before every send. Redis   Job moved to next_day queue.
  platform limit    from the Leaex    INCR + check.                                Counter noted in
  per recipient     platform to any                                                wa_interaction_log with
                    single phone                                                   status=DEFERRED_SPAM_GUARD.
                    number per                                                     
                    calendar day                                                   
                    (across ALL                                                    
                    partners                                                       
                    combined).                                                     

  Partner daily     Max 3 messages    Checked in Worker. Redis key:                Job rejected. Partner-side
  limit per         from any single   partner_daily:{tenant_id}:{sha256(phone)}.   rate_limit_events log
  recipient         partner to the                                                 updated.
                    same customer per                                              
                    day.                                                           

  Campaign send     A recipient       Campaign job creation checks: was this       Job silently dropped (not an
  deduplication     cannot receive    template_hash sent to this phone_hash in the error). Counted as deduped in
                    the same campaign last 7 days?                                 campaign analytics.
                    template more                                                  
                    than once per                                                  
                    7-day window from                                              
                    ANY partner.                                                   

  Multi-partner     If a recipient    Checked via partner_ids SET cardinality in   All further jobs for this
  daily cap         has already       spam_guard hash.                             recipient today go to
                    received messages                                              DEFERRED_SPAM_GUARD.
                    from 3 or more                                                 
                    different Leaex                                                
                    partners today,                                                
                    block all further                                              
                    sends regardless                                               
                    of remaining                                                   
                    daily quota.                                                   

  Weekly volume cap Max 15 platform   Checked at job creation time in Campaign     Campaign jobs for this
  per recipient     messages to any   Service.                                     recipient are dropped for the
                    single phone                                                   campaign run. Noted in
                    number per                                                     campaign analytics as
                    rolling 7-day                                                  spam_guard_dropped.
                    window.                                                        
  --------------------------------------------------------------------------------------------------------------

**INFO** *Phone numbers in the spam guard register are stored as SHA-256
hashes, not plaintext. The guard system cannot be used to enumerate or
identify customers. Only send/block decisions are made \-- no PII lookup
is possible from this store.*

## 5.4 Customer Opt-Out (Platform-Wide)

  -----------------------------------------------------------------------
  **Scenario**                        **Behaviour**
  ----------------------------------- -----------------------------------
  Customer replies \"STOP\" to any    evo API webhook fires. Rust
  Leaex message (pool number or       worker processes STOP keyword.
  partner number)                     wa_customer_consent table updated:
                                      opted_out = true for this
                                      phone_number at platform level. All
                                      future sends to this number blocked
                                      platform-wide, all tenants.

  Customer opts out in one partner\'s Partner sets opt_out in their
  CRM                                 customer record. Blocks sends from
                                      this partner only. Other partners
                                      unaffected \-- they have their own
                                      consent records.

  Partner wants to re-contact an      Blocked at Worker layer.
  opted-out customer                  wa_interaction_log records
                                      status=BLOCKED_OPT_OUT. Partner
                                      sees \"opted out\" in customer
                                      profile. No workaround \-- this is
                                      non-negotiable for DPDP compliance.

  Customer opts back in               Via explicit keyword \"START\"
                                      reply, or via in-app opt-in on QR
                                      booking page. Updates
                                      wa_customer_consent. Re-enables
                                      sends.
  -----------------------------------------------------------------------

# 6. Queue System (Redis)

## 6.1 Redis Data Structures

  -----------------------------------------------------------------------------------------------------
  **Structure**     **Key Pattern**                             **Purpose**           **TTL**
  ----------------- ------------------------------------------- --------------------- -----------------
  ZSET (sorted set) jobs:scheduled:{tenant_id}                  Scheduled jobs. Score None (persistent
                                                                = Unix timestamp to   until consumed)
                                                                send at. Scheduler    
                                                                pulls when score \<=  
                                                                now.                  

  LIST              jobs:ready:{tenant_id}                      Ready-to-send queue   None
                                                                per tenant. Workers   
                                                                BRPOP from this list. 

  LIST              jobs:dlq:{tenant_id}                        Dead Letter Queue.    7 days
                                                                Jobs that exhausted   
                                                                retries.              

  HASH              job:{job_id}                                Full job metadata:    48 hours after
                                                                tenant_id, instance,  completion
                                                                phone, payload,       
                                                                retry_count, status,  
                                                                scheduled_at,         
                                                                sent_at.              

  HASH              spam_guard:{phone_hash}                     Cross-partner         24h (daily keys),
                                                                daily/weekly send     7d (weekly)
                                                                counter per phone     
                                                                hash.                 

  STRING            rate_limit:{instance_name}:last_sent        Unix timestamp of     60 seconds
                                                                last send for this    
                                                                instance. Used for    
                                                                inter-message delay   
                                                                enforcement.          

  STRING            rate_limit:{tenant_id}:{phone_hash}:daily   Partner-to-customer   24h (resets
                                                                daily send count.     midnight IST)

  STRING            instance_health:{instance_name}             Current state: ACTIVE No TTL (managed
                                                                \| QR_REQUIRED \|     explicitly)
                                                                DISCONNECTED \|       
                                                                FLAGGED. Updated by   
                                                                health check worker.  

  SET               pool:available                              Set of pool instance  No TTL
                                                                names currently       
                                                                ACTIVE and under      
                                                                daily quota. Updated  
                                                                by Pool Manager.      

  ZSET              campaigns:active                            Running campaign IDs  No TTL
                                                                with last-updated     
                                                                score. Used by        
                                                                monitor to detect     
                                                                stuck campaigns.      
  -----------------------------------------------------------------------------------------------------

## 6.2 Job Schema

struct WhatsAppJob {

job_id: Uuid,

tenant_id: Uuid,

partner_id: Uuid,

campaign_id: Option\<Uuid\>, // None for CRM direct messages

instance_name: String, // which evo API instance to use

message_type: MessageType, // Campaign \| CrmDirect

recipient_phone: String, // +91XXXXXXXXXX

payload: MessagePayload, // text or template + variables

retry_count: u8, // 0-3, then DLQ

scheduled_at: DateTime\<Utc\>,

created_at: DateTime\<Utc\>,

idempotency_key: String, //
sha256(tenant_id+phone+template_hash+campaign_id)

}

**INFO** *idempotency_key prevents duplicate sends if a job is re-queued
after a worker crash. Before sending, the worker checks: has a message
with this idempotency_key already been delivered? If yes, mark as
duplicate and skip.*

## 6.3 Scheduler Worker Loop

loop {

// Pull jobs whose scheduled_at \<= now from all tenant ZSETs

let ready_jobs = redis.zrangebyscore(

\"jobs:scheduled:\*\", // scanned per-tenant for fairness

0, now_unix_timestamp()

);

for job in ready_jobs {

// Check idempotency (crash recovery)

if already_sent(&job.idempotency_key) { continue; }

// Check spam guard BEFORE moving to ready queue

if spam_guard_blocked(&job.recipient_phone, &job.tenant_id) {

defer_job_to_tomorrow(&job);

log_deferred(&job, DeferReason::SpamGuard);

continue;

}

// Move to per-tenant ready queue

redis.lpush(format!(\"jobs:ready:{}\", job.tenant_id), &job);

redis.zrem(format!(\"jobs:scheduled:{}\", job.tenant_id), &job.job_id);

}

sleep(Duration::from_millis(500)); // 500ms scheduler tick

}

# 7. Worker Pool & Rate Limiter (Rust + Tokio)

## 7.1 Worker Architecture

  -----------------------------------------------------------------------
  **Property**                        **Specification**
  ----------------------------------- -----------------------------------
  Implementation                      Rust async workers using Tokio
                                      runtime. Each worker is a Tokio
                                      task, not an OS thread.

  Worker Count (Phase 1)              4 concurrent workers. Enough for
                                      \~20 active partners without
                                      exceeding evo API capacity.

  Worker Count (Phase 2)              Scale to 16 workers. Each worker
                                      handles a dedicated queue shard.

  Queue Assignment                    Workers use BRPOP (blocking pop) on
                                      their assigned ready queues.
                                      Work-stealing: idle worker can pop
                                      from any tenant queue.

  Fairness                            Per-tenant queues prevent one
                                      high-volume partner from starving
                                      others. Each tenant\'s queue is
                                      processed in round-robin order
                                      across the worker pool.

  Crash Recovery                      Worker state is stateless \-- all
                                      state in Redis. Worker crash means
                                      job remains in ready queue. Another
                                      worker picks it up on next BRPOP.
  -----------------------------------------------------------------------

## 7.2 Worker Send Flow (per job)

async fn process_job(job: WhatsAppJob, redis: &Redis, evo:
&evoClient) {

// Step 1: Instance health check

let health = redis.get(format!(\"instance_health:{}\",
job.instance_name));

if health != \"ACTIVE\" {

if health == \"QR_REQUIRED\" { move_to_dlq(&job,
\"instance_needs_auth\"); return; }

requeue_with_delay(&job, Duration::from_secs(300)); return;

}

// Step 2: Rate limit check + enforce delay

let last_sent = redis.get(format!(\"rate_limit:{}:last_sent\",
job.instance_name));

let required_gap = random_delay(8, 15); // 8-15 seconds, randomized

let elapsed = now() - last_sent;

if elapsed \< required_gap {

sleep(required_gap - elapsed).await; // wait out the gap

}

// Step 3: Spam guard double-check (may have changed since scheduler)

if spam_guard_blocked(&job.recipient_phone, &job.tenant_id) {

defer_to_tomorrow(&job); return;

}

// Step 4: Idempotency check

if already_sent(&job.idempotency_key) {

mark_duplicate(&job); return;

}

// Step 5: Send via evo API

let result = evo.send_text(

&job.instance_name, &job.recipient_phone, &job.payload

).await;

// Step 6: Update rate limit timestamp

redis.set_ex(format!(\"rate_limit:{}:last_sent\", job.instance_name),
now(), 60);

// Step 7: Handle result

match result {

Ok(msg_id) =\> {

update_job_status(&job, Status::Sent, msg_id);

increment_spam_counter(&job.recipient_phone, &job.tenant_id);

persist_to_supabase(&job, Status::Sent, msg_id);

}

Err(e) if is_retryable(&e) =\> { retry_with_backoff(&job); }

Err(e) =\> { move_to_dlq(&job, &e.to_string()); }

}

}

## 7.3 Rate Limiter Rules

  -----------------------------------------------------------------------
  **Rule**                **Default Value**       **Adaptive Behaviour**
  ----------------------- ----------------------- -----------------------
  Inter-message delay     Random 8-15 seconds     If delivery_rate drops
  (per instance)          between each send on    below 70% in last 50
                          the same instance.      sends: increase to
                                                  15-25s. If
                                                  delivery_rate recovers
                                                  above 85%: return to
                                                  8-15s.

  Partner personal        200 unique recipients   Configurable per
  instance daily limit    per day.                partner plan. Basic:
                                                  100. Pro: 200.
                                                  Enterprise: 500 (via
                                                  partner instance).

  Campaign pool number    500 messages per pool   Automatic. Hard
  daily limit             number per day.         ceiling. Pool Manager
                                                  rotates to next
                                                  available pool number
                                                  when limit reached.

  Failure spike threshold \> 10% failure rate in  Instance cooling: 30
                          last 100 sends.         min pause. Adaptive
                                                  delay increase applied.
                                                  Alert fired if not
                                                  recovered after 1h.

  Concurrent sends per    1\. evo API       Non-negotiable. Worker
  instance                instances are           acquires per-instance
                          single-threaded.        lock (Redis SETNX)
                                                  before sending. Lock
                                                  TTL = 30s (prevents
                                                  deadlock if worker
                                                  crashes mid-send).
  -----------------------------------------------------------------------

# 8. Retry Engine & Dead Letter Queue

## 8.1 Retry Classification

  ----------------------------------------------------------------------------------------
  **Error Type**    **Examples**           **Retry?**        **Strategy**
  ----------------- ---------------------- ----------------- -----------------------------
  Network /         Connection timeout to  YES               Exponential backoff: +30s,
  transient         evo API, HTTP                      +2min, +10min. Max 3 retries
                    503, evo process                   then DLQ.
                    restart.                                 

  evo rate    HTTP 429 from          YES               Respect Retry-After header.
  limit             evo API (too                       If none: wait 60s. Do NOT
                    many concurrent                          retry immediately.
                    requests to same                         
                    instance).                               

  Instance          evo returns      PAUSE             Move all jobs for this
  disconnected      instance_not_found or                    instance to PAUSED state.
                    session_closed.                          Fire health alert. Resume
                                                             when instance reconnects.

  QR re-auth        evo returns      NO                Move all jobs for this tenant
  required          qr_required or                           to DLQ with
                    auth_failed.                             reason=instance_needs_auth.
                                                             Partner must reconnect. Jobs
                                                             held 7 days.

  Invalid recipient evo returns      NO                Mark as FAILED permanently.
                    invalid_number,                          Log to Supabase. Do NOT retry
                    not_on_whatsapp.                         \-- wastes quota and flags
                                                             number.

  Banned instance   evo returns      NO \-- ESCALATE   Move ALL jobs for instance to
                    banned or                                DLQ. Alert ops team
                    account_restricted.                      immediately. Instance
                                                             quarantined.

  Spam guard block  Recipient hit          DEFERRED          Move to next_day queue. Not a
                    daily/weekly limit.                      failure \-- just a scheduling
                                                             adjustment.

  Opt-out           Customer is opted out  NO                Permanent drop. Log
                    in                                       status=BLOCKED_OPT_OUT. No
                    wa_customer_consent.                     retry ever.
  ----------------------------------------------------------------------------------------

## 8.2 Dead Letter Queue Processing

  ---------------------------------------------------------------------------
  **DLQ Action**    **Trigger**       **Admin UI**          **Partner UI**
  ----------------- ----------------- --------------------- -----------------
  View DLQ          Admin navigates   Full DLQ: all         Not visible to
                    to /admin/dlq in  tenants, all reasons, partners.
                    admin panel.      job details, phone    
                                      (masked), error       
                                      reason, retry         
                                      history.              

  Bulk requeue      Admin selects     Available for network N/A
  (admin)           jobs and clicks   error DLQ jobs only.  
                    \"Requeue\".      Not available for     
                                      invalid_number,       
                                      banned, opt-out.      

  Auto-expire       7 days after DLQ  DLQ entries older     Partners see
                    entry.            than 7d automatically \"Message
                                      deleted from Redis.   expired\" in
                                      Supabase record       their send
                                      retained with         history for
                                      status=EXPIRED_DLQ.   DLQ-expired jobs.

  Instance recovery Ops team          Admin can trigger     Partner receives
  flow              reconnects a      \"Release DLQ for     in-app
                    partner\'s WA     tenant X\" which      notification:
                    instance.         moves                 \"Your WhatsApp
                                      instance_needs_auth   messages are
                                      jobs back to          being resent. X
                                      scheduled queue.      messages will be
                                                            delivered
                                                            shortly.\"
  ---------------------------------------------------------------------------

# 9. Database Schema (Supabase + Postgres)

## 9.1 Core Tables

All tables include created_at, updated_at (auto-managed by Supabase),
and row-level security (RLS) policies. Partner-facing tables have
tenant_id as the RLS discriminator. Admin tables have no RLS restriction
but require service_role key.

### agencies

  -----------------------------------------------------------------------
  **Column**              **Type**                **Notes**
  ----------------------- ----------------------- -----------------------
  id                      uuid PRIMARY KEY        Auto-generated.

  name                    text NOT NULL           Agency / Leaex platform
                                                  identifier.

  api_key                 text UNIQUE NOT NULL    Hashed with bcrypt.
                                                  Used for x-api-key
                                                  auth.

  subscription_status     enum: active \|         Controls API Gateway
                          suspended \| trial      auth.

  plan_tier               enum: basic \| pro \|   Determines feature
                          enterprise              flags and limits.

  daily_msg_limit         integer DEFAULT 200     Platform-enforced
                                                  ceiling across all
                                                  tenants for this
                                                  agency.
  -----------------------------------------------------------------------

### tenants (= salon partners)

  --------------------------------------------------------------------------
  **Column**              **Type**                **Notes**
  ----------------------- ----------------------- --------------------------
  id                      uuid PRIMARY KEY        

  agency_id               uuid REFERENCES         Foreign key. Used to
                          agencies(id)            validate API key -\>
                                                  tenant ownership.

  partner_id              uuid REFERENCES         Links to the Leaex
                          partners(id)            partner/salon record.

  instance_name           text UNIQUE             evo API instance
                                                  identifier: e.g.
                                                  \"wa_glamour_studio_01\"

  wa_number               text                    Connected WA number in
                                                  +91XXXXXXXXXX format.

  instance_status         enum: active \|         Synced from Redis health
                          qr_required \|          state into Supabase
                          disconnected \| banned  hourly.
                          \| suspended            

  daily_crm_limit         integer DEFAULT 200     Max 1:1 CRM messages per
                                                  day for this tenant.

  campaign_enabled        boolean DEFAULT false   Pro+ only. Enables
                                                  campaign endpoints for
                                                  this tenant.
  --------------------------------------------------------------------------

### wa_campaigns

  -----------------------------------------------------------------------
  **Column**              **Type**                **Notes**
  ----------------------- ----------------------- -----------------------
  id                      uuid PRIMARY KEY        

  tenant_id               uuid REFERENCES         RLS: partner sees only
                          tenants(id)             own rows.

  name                    text NOT NULL           Campaign name as
                                                  entered by partner.

  template_name           text                    WhatsApp template ID or
                                                  message template slug.

  template_hash           text                    SHA-256 of template
                                                  content. Used for 7-day
                                                  dedup check.

  status                  enum: draft \| running  
                          \| paused \| completed  
                          \| cancelled            

  total_recipients        integer                 Count at campaign
                                                  creation time.

  sent_count              integer DEFAULT 0       Incremented by worker
                                                  on each successful
                                                  send.

  delivered_count         integer DEFAULT 0       Incremented by
                                                  evo webhook on
                                                  delivery receipt.

  failed_count            integer DEFAULT 0       

  deferred_count          integer DEFAULT 0       Spam guard deferrals.

  started_at              timestamptz             

  completed_at            timestamptz             

  pool_rotation_ids       text\[\]                Which pool instance
                                                  names were used. ADMIN
                                                  ONLY \-- not exposed in
                                                  partner API.
  -----------------------------------------------------------------------

### wa_interaction_log (primary interaction record)

  ------------------------------------------------------------------------
  **Column**               **Type**                **Notes**
  ------------------------ ----------------------- -----------------------
  id                       uuid PRIMARY KEY        

  tenant_id                uuid REFERENCES         RLS enforced. Partner
                           tenants(id)             sees only own tenant_id
                                                   rows.

  campaign_id              uuid REFERENCES         NULL for CRM direct
                           wa_campaigns(id)        messages.
                           NULLABLE                

  message_type             enum: campaign \|       
                           booking_confirm \|      
                           reminder \| birthday \| 
                           anniversary \|          
                           manual_crm \|           
                           re_engagement           

  recipient_phone_hash     text                    SHA-256 of recipient
                                                   phone. Stored in log
                                                   for analytics without
                                                   PII leakage.

  recipient_phone_masked   text                    Last 4 digits only: +91
                                                   XXXXX X1234. Shown in
                                                   partner UI.

  instance_used            text                    evo instance
                                                   name. For pool sends:
                                                   shows \"leaex_pool\"
                                                   (never pool number
                                                   identity).

  status                   enum: pending \| sent   
                           \| delivered \| read \| 
                           failed \| deferred_spam 
                           \| blocked_optout \|    
                           duplicate \|            
                           expired_dlq             

  evo_msg_id         text NULLABLE           Message ID returned by
                                                   evo API on
                                                   success.

  error_reason             text NULLABLE           Human-readable error
                                                   for FAILED status.

  retry_count              smallint DEFAULT 0      

  scheduled_at             timestamptz             

  sent_at                  timestamptz NULLABLE    

  delivered_at             timestamptz NULLABLE    Populated by webhook.

  idempotency_key          text UNIQUE             Prevents duplicate
                                                   records on worker
                                                   retry.
  ------------------------------------------------------------------------

### wa_customer_consent

  -----------------------------------------------------------------------
  **Column**              **Type**                **Notes**
  ----------------------- ----------------------- -----------------------
  id                      uuid PRIMARY KEY        

  phone_hash              text NOT NULL           SHA-256 of phone
                                                  number. Platform-wide
                                                  opt-out is checked by
                                                  hash.

  tenant_id               uuid NULLABLE           NULL = platform-wide
                                                  opt-out. Non-null =
                                                  tenant-specific opt-out
                                                  only.

  opted_out               boolean DEFAULT false   

  opted_out_at            timestamptz NULLABLE    

  opted_out_source        enum: stop_keyword \|   How the opt-out was
                          partner_crm \|          registered.
                          customer_request \|     
                          admin                   

  opted_in_at             timestamptz NULLABLE    If customer re-opted in
                                                  after opting out.
  -----------------------------------------------------------------------

### instance_health_log (admin-only)

  -----------------------------------------------------------------------
  **Column**              **Type**                **Notes**
  ----------------------- ----------------------- -----------------------
  id                      uuid PRIMARY KEY        

  instance_name           text NOT NULL           

  tenant_id               uuid NULLABLE           NULL for pool numbers.

  is_pool                 boolean DEFAULT false   

  event_type              enum: connected \|      
                          disconnected \|         
                          qr_required \| banned   
                          \| health_check_ok \|   
                          health_check_fail       

  previous_status         text                    

  new_status              text                    

  detail                  jsonb                   Raw evo API
                                                  response or error
                                                  detail.

  logged_at               timestamptz             
  -----------------------------------------------------------------------

### rate_limit_events (admin-only)

  --------------------------------------------------------------------------
  **Column**              **Type**                   **Notes**
  ----------------------- -------------------------- -----------------------
  id                      uuid PRIMARY KEY           

  tenant_id               uuid REFERENCES            
                          tenants(id)                

  instance_name           text                       

  event_type              enum: daily_limit_reached  
                          \| spam_guard_triggered \| 
                          adaptive_delay_increased   
                          \| failure_spike           

  msg_count_at_event      integer                    

  detail                  text                       Human-readable
                                                     description.

  logged_at               timestamptz                
  --------------------------------------------------------------------------

# 10. Supabase RLS Policies

Row Level Security is enforced at the Postgres layer independently of
the API layer. Even if there is a bug in the Rust API, the database will
not return cross-tenant data.

## 10.1 Key RLS Policy Definitions

\-- wa_interaction_log: partners see only their own tenant

CREATE POLICY \"tenant_isolation_interactions\"

ON wa_interaction_log

FOR SELECT

USING (tenant_id = auth.jwt() -\>\> \'tenant_id\');

\-- wa_campaigns: partners see only own campaigns

CREATE POLICY \"tenant_isolation_campaigns\"

ON wa_campaigns

FOR SELECT

USING (tenant_id = auth.jwt() -\>\> \'tenant_id\');

\-- wa_customer_consent: partner sees only their tenant-scoped consent
records

CREATE POLICY \"tenant_isolation_consent\"

ON wa_customer_consent

FOR SELECT

USING (tenant_id = auth.jwt() -\>\> \'tenant_id\' OR tenant_id IS NULL);

\-- Note: tenant_id IS NULL = platform-wide opt-out, readable by all
tenants

\-- for blocking purposes but no PII is exposed.

\-- Admin bypass (service_role key bypasses all RLS)

\-- Admin API routes use service_role key, not partner JWT

\-- This is the ONLY way to read cross-tenant data

\-- pool_number_stats: admin-only, no partner access

CREATE POLICY \"admin_only_pool_stats\"

ON pool_number_stats

FOR SELECT

USING (auth.role() = \'service_role\');

# 11. Deployment Architecture

## 11.1 Docker Compose Layout (Phase 1)

version: \"3.9\"

services:

api_gateway:

build: ./rust/api_gateway

ports: \[\"8080:8080\"\]

environment:

\- REDIS_URL=redis://redis:6379

\- SUPABASE_URL=\${SUPABASE_URL}

\- SUPABASE_SERVICE_KEY=\${SUPABASE_SERVICE_KEY}

depends_on: \[redis\]

scheduler:

build: ./rust/scheduler

environment:

\- REDIS_URL=redis://redis:6379

depends_on: \[redis\]

restart: unless-stopped

worker:

build: ./rust/worker

deploy:

replicas: 4 \# Phase 1: 4 workers

environment:

\- REDIS_URL=redis://redis:6379

\- evo_BASE_URL=http://evo_api:8080

\- SUPABASE_URL=\${SUPABASE_URL}

depends_on: \[redis, evo_api\]

restart: unless-stopped

pool_manager:

build: ./rust/pool_manager

environment:

\- REDIS_URL=redis://redis:6379

\- evo_BASE_URL=http://evo_api:8080

restart: unless-stopped

health_monitor:

build: ./rust/health_monitor

environment:

\- REDIS_URL=redis://redis:6379

\- evo_BASE_URL=http://evo_api:8080

\- ALERT_WEBHOOK=\${SLACK_WEBHOOK_URL}

restart: unless-stopped

evo_api:

image: atendai/evo-api:latest

ports: \[\"8081:8080\"\]

volumes:

\- evo_data:/evo/instances

environment:

\- AUTHENTICATION_API_KEY=\${evo_API_KEY}

restart: unless-stopped

redis:

image: redis:7-alpine

command: redis-server \--appendonly yes \# AOF persistence

volumes: \[redis_data:/data\]

restart: unless-stopped

## 11.2 Railway Deployment (Phase 1 target)

  -----------------------------------------------------------------------
  **Service**             **Railway Resource**    **Estimated Cost/mo
                                                  (INR)**
  ----------------------- ----------------------- -----------------------
  Rust API Gateway        Railway \-- 512MB RAM,  \~Rs.800
                          0.5 vCPU                

  Rust Workers (x4)       Railway \-- 1GB RAM, 1  \~Rs.1,600
                          vCPU                    

  Scheduler + Pool        Railway \-- 512MB RAM   \~Rs.800
  Manager + Health        combined                
  Monitor                                         

  evo API           Railway \-- 1GB RAM     \~Rs.1,200
                          (session data           
                          persistent)             

  Redis (AOF persistence) Railway \-- 512MB RAM   \~Rs.600

  Supabase                Supabase Pro (\$25/mo)  \~Rs.2,100

  TOTAL (Phase 1)                                 \~Rs.7,100/month
  -----------------------------------------------------------------------

## 11.3 Scaling Strategy

  -----------------------------------------------------------------------
  **Phase**               **Partners**            **Architecture Change**
  ----------------------- ----------------------- -----------------------
  Phase 1                 0-20                    Single worker replica
                                                  (x4 workers). Single
                                                  Redis. 10 pool numbers.
                                                  All on Railway.
                                                  Monorepo deploy.

  Phase 2                 20-100                  Worker replicas scaled
                                                  to 16. Redis Cluster (3
                                                  nodes). Pool expanded
                                                  to 30 numbers. Worker
                                                  count scaled per active
                                                  tenant count.

  Phase 3                 100+                    Worker clusters with
                                                  queue sharding by
                                                  tenant_id hash.
                                                  Dedicated Redis for
                                                  pool manager vs tenant
                                                  queues. VPS migration
                                                  (Hetzner / AWS) for
                                                  cost efficiency.
  -----------------------------------------------------------------------

# 12. Implementation Timeline

  ---------------------------------------------------------------------------
  **Sprint**        **Duration**      **Deliverable**   **Key Milestones**
  ----------------- ----------------- ----------------- ---------------------
  Week 1            5 days            Rust API Gateway  x-api-key auth
                                      (Axum) \-- auth,  working.
                                      tenant            TenantContext
                                      resolution,       resolving from
                                      routing. Redis    Supabase.
                                      setup with AOF.   /message/send
                                      Basic job schema. endpoint reachable.
                                                        Redis ZSET + LIST
                                                        queue operational.

  Week 2            5 days            Worker pool       End-to-end: enqueue
                                      (Tokio, 4         job -\> worker picks
                                      workers).         up -\> evo API
                                      evo API     sends. Instance
                                      integration.      health check before
                                      BRPOP-based job   send. Rate limit
                                      consumption.      (8-15s delay)
                                      Basic send flow.  enforced.

  Week 3            5 days            Campaign Service. POST /campaign/start
                                      Scheduler worker. creates jobs +
                                      Spam guard (Redis enqueues. Scheduler
                                      counters). Retry  moves from ZSET to
                                      engine with DLQ.  ready LIST. Spam
                                                        guard blocking
                                                        cross-tenant
                                                        over-messaging. Retry
                                                        with exponential
                                                        backoff.

  Week 4            5 days            Campaign Pool     Pool instances
                                      Numbers. Pool     registered. Routing
                                      Manager service.  decision: campaign
                                      Routing logic     -\> pool, crm -\>
                                      (campaign vs      partner instance.
                                      CRM). Pool        Pool round-robin +
                                      lifecycle states. daily quota
                                                        enforcement. Pool
                                                        health monitoring.

  Week 5            5 days            Supabase schema   wa_interaction_log
                                      finalization. RLS populated on every
                                      policies.         send. RLS verified
                                      Interaction log   (partner cannot read
                                      writes from       other tenant data).
                                      workers. Admin +  /analytics/messages
                                      Partner analytics returning scoped
                                      endpoints.        data. Admin
                                                        cross-tenant view
                                                        working.

  Week 6            3 days            Opt-out handling  STOP keyword -\>
                                      (STOP keyword     wa_customer_consent
                                      webhook).         update -\> all future
                                      Delivery receipt  sends blocked. 5-min
                                      reconciliation    reconciliation cron
                                      worker. Sentry    for stale in-flight
                                      integration.      records. Production
                                      Deploy to         deploy. Smoke test
                                      Railway.          with real evo
                                                        API instance.
  ---------------------------------------------------------------------------

**WARNING** *Do not begin Week 4 (pool numbers) until Week 3 (spam
guard + retry) is fully tested. The pool numbers have real-world ban
consequences if the rate limiter or spam guard has bugs when they go
live.*

# 13. Admin Panel UI Requirements (WhatsApp Module)

These are the screens needed in Module A (Admin Panel) to operate,
monitor, and debug the WhatsApp engine. All screens are admin-only and
inaccessible to partners.

  --------------------------------------------------------------------------------------------------------------
  **Screen**        **Route**                      **Key Data Shown**                          **Key Actions**
  ----------------- ------------------------------ ------------------------------------------- -----------------
  WA Platform       /admin/whatsapp                Total messages sent today (all tenants).    Quick links to
  Overview                                         Pool health summary (X of Y active). Active all sub-screens.
                                                   campaigns count. Delivery rate              Alert banner if
                                                   (platform-wide, last 24h). Spam guard       any instance
                                                   trigger count today.                        BANNED or pool \<
                                                                                               3 active numbers.

  Interaction Log   /admin/whatsapp/interactions   Full wa_interaction_log: tenant, message    Filter by tenant,
  (all tenants)                                    type, status, timestamp. Phone masked to    status, date,
                                                   last 4 digits. Instance used (partner       message type.
                                                   instance or \"Leaex Pool\"). Campaign ID if Export to CSV.
                                                   applicable.                                 View full job
                                                                                               detail in drawer.

  Campaign Monitor  /admin/whatsapp/campaigns      All campaigns across all partners: name,    Pause any
                                                   tenant, status,                             campaign
                                                   total/sent/delivered/failed/deferred        (emergency).
                                                   counts, delivery rate %, started_at.        Cancel any
                                                                                               campaign. View
                                                                                               per-recipient
                                                                                               status breakdown.

  Instance Health   /admin/whatsapp/instances      All partner instances + all pool instances: Force QR re-auth
  Dashboard                                        name, tenant, status, last_health_check,    prompt (fires
                                                   last_send, sessions active.                 webhook to
                                                                                               partner). Release
                                                                                               DLQ for instance.
                                                                                               Quarantine
                                                                                               instance (stops
                                                                                               all sends). View
                                                                                               health log
                                                                                               history.

  Pool Number       /admin/whatsapp/pool           All pool numbers: state                     Add new pool
  Manager                                          (warming/active/cooling/flagged/resting),   number (triggers
                                                   daily_sent, daily_limit, delivery_rate,     warmup flow).
                                                   last_used, warmup_day.                      Pull number from
                                                                                               rotation. Force
                                                                                               rest. View
                                                                                               per-number send
                                                                                               history.

  Spam Guard        /admin/whatsapp/spamguard      Recipients approaching daily/weekly cap     Adjust platform
  Monitor                                          (top 50 by send count). Partners with       daily limit
                                                   highest spam_guard_deferred counts today.   (emergency
                                                   Cross-partner contact frequency             override). View
                                                   distribution chart.                         recipients\'
                                                                                               interaction
                                                                                               history (admin
                                                                                               only, phone
                                                                                               masked). Export
                                                                                               spam guard
                                                                                               events.

  Dead Letter Queue /admin/whatsapp/dlq            All DLQ jobs: tenant, error_reason,         Bulk requeue (for
                                                   retry_count, job age, instance.             network error
                                                                                               jobs only).
                                                                                               Delete. Mark
                                                                                               resolved. Filter
                                                                                               by reason.

  Audit Log         /admin/whatsapp/audit          All audit_log entries: failed auth          Filter by event
                                                   attempts, cross-tenant access attempts, API type, date, IP.
                                                   key misuse, admin actions.                  Export.
  --------------------------------------------------------------------------------------------------------------

*End of Document \-- Leaex WhatsApp Architecture v3.0*

*Rust API Gateway \| Redis Queue \| Campaign Pool Numbers \|
Multi-Tenant Isolation \| Cross-Partner Spam Guard*

*For Engineering & Product Teams Only \-- Confidential*
