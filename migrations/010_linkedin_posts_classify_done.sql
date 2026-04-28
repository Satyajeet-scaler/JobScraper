CREATE INDEX idx_linkedin_posts_classify_done ON linkedin_posts (classify_done);
CREATE INDEX idx_linkedin_posts_classify_lookup ON linkedin_posts (requested_role, run_date, classify_done, id);
