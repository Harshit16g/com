-- 1. Ensure structural requirements for intelligent upsert
ALTER TABLE comms.wa_conversations 
DROP CONSTRAINT IF EXISTS wa_conversations_org_phone_key;

ALTER TABLE comms.wa_conversations 
ADD CONSTRAINT wa_conversations_org_phone_key UNIQUE (org_id, evo_contact_phone);

ALTER TABLE comms.wa_conversations 
ADD COLUMN IF NOT EXISTS chat_history jsonb DEFAULT '[]'::jsonb;

-- 2. New Intelligent Sync Function
CREATE OR REPLACE FUNCTION comms.handle_wa_api_event(
  p_org_id uuid,
  p_instance_id text,
  p_event_type text,
  p_payload jsonb
) RETURNS void AS $$
DECLARE
  v_customer_id UUID;
  v_phone TEXT;
BEGIN
  v_phone := p_payload->>'phone';
  
  -- Resolve existing customer if possible
  SELECT id INTO v_customer_id FROM crm.customers 
  WHERE org_id = p_org_id AND phone = v_phone 
  LIMIT 1;

  -- Upsert conversation header + APPEND history
  INSERT INTO comms.wa_conversations (
    org_id, 
    customer_id, 
    evo_contact_phone, 
    direction,
    last_message_at,
    chat_history,
    metadata
  ) VALUES (
    p_org_id,
    v_customer_id,
    v_phone,
    COALESCE(p_payload->>'direction', 'inbound'),
    NOW(),
    jsonb_strip_nulls(jsonb_build_array(p_payload)),
    jsonb_strip_nulls(jsonb_build_object('last_instance', p_instance_id))
  )
  ON CONFLICT (org_id, evo_contact_phone) DO UPDATE SET
    customer_id = COALESCE(v_customer_id, comms.wa_conversations.customer_id),
    last_message_at = NOW(),
    message_count = comms.wa_conversations.message_count + 1,
    -- THE "INTELIGENCE": Append new payload to history array, preserving history
    chat_history = comms.wa_conversations.chat_history || p_payload,
    metadata = comms.wa_conversations.metadata || jsonb_build_object('last_instance', p_instance_id),
    updated_at = NOW();

END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
