CREATE TABLE IF NOT EXISTS job_candidate_matches (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_id BIGINT UNSIGNED NOT NULL,
    run_date DATE NOT NULL,
    role_slug VARCHAR(160) NOT NULL,
    candidate_email VARCHAR(512) NOT NULL,
    ai_score INT NULL,
    ai_reason TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_job_candidate_match (job_id, candidate_email),
    KEY idx_job_candidate_match_role (role_slug),
    KEY idx_job_candidate_match_date (run_date),
    CONSTRAINT fk_job_candidate_match_job_id FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
