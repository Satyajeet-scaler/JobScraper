CREATE TABLE IF NOT EXISTS jobs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    site VARCHAR(100) NOT NULL,
    job_url TEXT NOT NULL,
    job_url_normalized VARCHAR(512) NOT NULL,
    title VARCHAR(512) NULL,
    company VARCHAR(255) NULL,
    location VARCHAR(255) NULL,
    date_posted VARCHAR(120) NULL,
    requested_role VARCHAR(100) NULL,
    run_date DATE NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_jobs_identity (site, job_url_normalized, requested_role, run_date),
    KEY idx_jobs_run_date (run_date),
    KEY idx_jobs_company (company)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS job_scrapes (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_id BIGINT UNSIGNED NOT NULL,
    role_query VARCHAR(160) NULL,
    experience VARCHAR(255) NULL,
    salary VARCHAR(255) NULL,
    job_type VARCHAR(120) NULL,
    description_full LONGTEXT NULL,
    raw_payload_json JSON NULL,
    role_pipeline_run_id VARCHAR(64) NULL,
    role_pipeline_run_seq INT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_job_scrapes_run (job_id, role_pipeline_run_id, role_pipeline_run_seq),
    KEY idx_job_scrapes_job (job_id),
    CONSTRAINT fk_job_scrapes_job_id FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS job_relevance (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_id BIGINT UNSIGNED NOT NULL,
    is_relevant BOOLEAN NOT NULL DEFAULT TRUE,
    matched_role VARCHAR(255) NULL,
    role_category VARCHAR(255) NULL,
    priority VARCHAR(120) NULL,
    reason TEXT NULL,
    company_size VARCHAR(120) NULL,
    confidence VARCHAR(120) NULL,
    assigned_owner VARCHAR(255) NULL,
    handover_sent BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_job_relevance_job (job_id),
    KEY idx_job_relevance_job (job_id),
    KEY idx_job_relevance_handover (handover_sent, assigned_owner),
    CONSTRAINT fk_job_relevance_job_id FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS job_recruiter_contacts (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_id BIGINT UNSIGNED NOT NULL,
    run_date DATE NOT NULL,
    recruiter_name VARCHAR(255) NULL,
    recruiter_headline VARCHAR(512) NULL,
    recruiter_profile_url VARCHAR(1024) NULL,
    recruiter_profile_url_normalized VARCHAR(512) NULL,
    recruiter_email VARCHAR(200) NULL,
    recruiter_source VARCHAR(40) NULL,
    scrape_error TEXT NULL,
    assigned_owner VARCHAR(255) NULL,
    handover_sent BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_job_recruiter_contact (
        job_id,
        recruiter_profile_url_normalized,
        recruiter_email,
        recruiter_source
    ),
    KEY idx_job_recruiter_job (job_id),
    KEY idx_job_recruiter_run (run_date),
    KEY idx_job_recruiter_handover (handover_sent, assigned_owner),
    CONSTRAINT fk_job_recruiter_contacts_job_id FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
