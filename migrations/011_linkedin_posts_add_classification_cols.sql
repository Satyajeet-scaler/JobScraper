-- Add missing columns for LinkedIn posts classification tracking
-- Run this if your linkedin_posts table was created before the classify_done/classified_at columns were added

ALTER TABLE linkedin_posts
    ADD COLUMN classify_done BOOLEAN NOT NULL DEFAULT FALSE AFTER run_seq,
    ADD COLUMN classified_at DATETIME NULL AFTER classify_done;
