-- Migration 005: Polymorphic Message Ownership & LinkedIn PM Senders
-- Creates linkedin_pm_senders table and adds owner_type/owner_id to conversation_messages

-- 1. Create linkedin_pm_senders table for unknown/non-Lusha senders
CREATE TABLE IF NOT EXISTS linkedin_pm_senders (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    sender_name VARCHAR(255) NOT NULL,
    linkedin_profile_url VARCHAR(512) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_linkedin_pm_senders_url (linkedin_profile_url)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Add polymorphic owner columns to conversation_messages
ALTER TABLE conversation_messages
    MODIFY COLUMN conversation_id BIGINT UNSIGNED NULL,
    ADD COLUMN owner_type ENUM('recruiter_conversation', 'linkedin_sender') NOT NULL DEFAULT 'recruiter_conversation' AFTER conversation_id,
    ADD COLUMN owner_id BIGINT UNSIGNED NULL AFTER owner_type;

-- 3. Backfill: set owner_id = conversation_id for all existing rows
UPDATE conversation_messages SET owner_id = conversation_id WHERE owner_type = 'recruiter_conversation';

-- 4. Index for polymorphic lookups
CREATE INDEX idx_conversation_messages_owner ON conversation_messages(owner_type, owner_id);
