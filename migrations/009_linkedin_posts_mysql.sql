-- Existing migrations:
-- 001_recruiter_crm_mysql.sql (lush_recruiters)
-- 002_enforce_unique_linkedin_url.sql
-- 003_conversation_pipeline.sql
-- 005_polymorphic_ownership.sql
-- 006_jobs_pipeline_mysql.sql (jobs, job_scrapes, job_relevance, job_recruiter_contacts)
-- 007_candidate_evaluator_mysql.sql (job_candidate_matches)
-- 008_jobs_pipeline_stage_tracking.sql (jobs column adds)

CREATE TABLE IF NOT EXISTS linkedin_posts (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    post_url TEXT NOT NULL,
    post_url_normalized VARCHAR(512) NOT NULL,
    post_id VARCHAR(255) NULL,
    search_query VARCHAR(512) NULL,
    content_type VARCHAR(50) NULL,
    post_text LONGTEXT NULL,
    posted_at VARCHAR(120) NULL,
    author_name VARCHAR(512) NULL,
    author_profile_url TEXT NULL,
    author_info TEXT NULL,
    author_type VARCHAR(100) NULL,
    company VARCHAR(255) NULL,
    job_title_hint VARCHAR(512) NULL,
    likes_count INT NULL,
    comments_count INT NULL,
    reposts_count INT NULL,
    requested_role VARCHAR(100) NULL,
    role_slug VARCHAR(100) NULL,
    run_date DATE NOT NULL,
    run_id VARCHAR(64) NULL,
    run_seq INT NULL,
    classify_done BOOLEAN NOT NULL DEFAULT FALSE,
    classified_at DATETIME NULL,
    raw_payload_json JSON NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_linkedin_posts_identity (post_url_normalized, requested_role, run_date),
    KEY idx_linkedin_posts_run_date (run_date),
    KEY idx_linkedin_posts_role_slug (role_slug),
    KEY idx_linkedin_posts_company (company)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS linkedin_post_relevance (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    linkedin_post_id BIGINT UNSIGNED NOT NULL,
    is_relevant BOOLEAN NOT NULL DEFAULT FALSE,
    tier VARCHAR(10) NULL,
    role_category VARCHAR(255) NULL,
    reason TEXT NULL,
    author_company VARCHAR(255) NULL,
    hiring_company VARCHAR(255) NULL,
    confidence VARCHAR(120) NULL,
    priority VARCHAR(120) NULL,
    assigned_owner VARCHAR(255) NULL,
    handover_sent BOOLEAN NOT NULL DEFAULT FALSE,
    classify_run_id VARCHAR(64) NULL,
    classify_run_seq INT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_linkedin_post_relevance_post (linkedin_post_id),
    KEY idx_linkedin_post_relevance_post (linkedin_post_id),
    KEY idx_linkedin_post_relevance_handover (handover_sent, assigned_owner),
    CONSTRAINT fk_linkedin_post_relevance_post_id FOREIGN KEY (linkedin_post_id) REFERENCES linkedin_posts(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
