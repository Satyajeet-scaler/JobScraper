-- Migration 003: Conversation Assistant Pipeline
-- Based on PRD and Workflow Requirements

-- 1. Extend recruiter_conversations for thread-level state
ALTER TABLE recruiter_conversations
    ADD COLUMN current_intent ENUM('positive', 'neutral', 'non_relevant') NULL AFTER status,
    ADD COLUMN auto_reply_enabled BOOLEAN NOT NULL DEFAULT TRUE AFTER current_intent;

-- 2. Extend conversation_messages for per-message metadata and audit
ALTER TABLE conversation_messages
    ADD COLUMN delivery_status ENUM('pending', 'sent', 'delivered', 'failed', 'blocked') DEFAULT 'pending' AFTER received_at,
    ADD COLUMN pipeline_run_id BIGINT UNSIGNED NULL AFTER delivery_status;

-- 3. Create pipeline_runs table for audit trailing of each execution loop
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_type ENUM('inbox_watch', 'outreach_batch', 'response_generator', 'full_pipeline') NOT NULL,
    status ENUM('running', 'completed', 'failed', 'partial') NOT NULL DEFAULT 'running',
    conversations_scanned INT UNSIGNED NOT NULL DEFAULT 0,
    messages_processed INT UNSIGNED NOT NULL DEFAULT 0,
    replies_sent INT UNSIGNED NOT NULL DEFAULT 0,
    errors_json JSON NULL,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. Create policy_logs table for guardrail audit (PRD FR6)
CREATE TABLE IF NOT EXISTS policy_logs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    message_id BIGINT UNSIGNED NOT NULL,
    pipeline_run_id BIGINT UNSIGNED NULL,
    rule_name VARCHAR(120) NOT NULL,
    action ENUM('approve', 'block', 'modify', 'escalate') NOT NULL,
    reason TEXT NULL,
    draft_content TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_policy_logs_message (message_id),
    INDEX idx_policy_logs_run (pipeline_run_id),
    CONSTRAINT fk_policy_logs_message_id
        FOREIGN KEY (message_id) REFERENCES conversation_messages(id) ON DELETE CASCADE,
    CONSTRAINT fk_policy_logs_run_id
        FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. Indexes for the new columns
CREATE INDEX idx_recruiter_conversations_intent ON recruiter_conversations(current_intent);
CREATE INDEX idx_conversation_messages_delivery ON conversation_messages(delivery_status);
CREATE INDEX idx_conversation_messages_pipeline_run ON conversation_messages(pipeline_run_id);
