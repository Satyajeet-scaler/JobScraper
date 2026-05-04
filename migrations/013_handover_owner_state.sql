-- Migration 013: Handover owner round-robin state tracking
-- Persist the last assigned owner across Slack handover runs so
-- round-robin rotation does not reset to the first owner each time.

CREATE TABLE IF NOT EXISTS handover_owner_state (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    state_key VARCHAR(120) NOT NULL,
    last_owner_identifier VARCHAR(255) NOT NULL DEFAULT '',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_state_key (state_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
