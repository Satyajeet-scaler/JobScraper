ALTER TABLE linkedin_posts
    ADD COLUMN classify_status VARCHAR(20) NOT NULL DEFAULT 'pending' AFTER run_seq,
    ADD COLUMN classified_at DATETIME NULL AFTER classify_status;

CREATE INDEX idx_linkedin_posts_classify_status ON linkedin_posts (classify_status);
CREATE INDEX idx_linkedin_posts_classify_lookup ON linkedin_posts (requested_role, run_date, classify_status, id);
