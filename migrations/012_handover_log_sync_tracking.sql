ALTER TABLE job_recruiter_contacts
    ADD COLUMN handover_log_synced BOOLEAN NOT NULL DEFAULT FALSE,
    ADD INDEX idx_jrc_role_run_sync (handover_log_synced, run_date);

ALTER TABLE linkedin_post_relevance
    ADD COLUMN handover_log_synced BOOLEAN NOT NULL DEFAULT FALSE,
    ADD INDEX idx_lpr_handover_synced (handover_log_synced);
