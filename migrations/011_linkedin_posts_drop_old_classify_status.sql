-- Transition from old PR columns to the final classify_done/classified_at schema.
-- Safe to run on a DB that already has the old columns from the previous PR.

ALTER TABLE linkedin_posts
    DROP COLUMN IF EXISTS classify_status,
    DROP COLUMN IF EXISTS classified_at;

-- Re-add classified_at cleanly (classify_done was added inline in 009)
ALTER TABLE linkedin_posts
    ADD COLUMN IF NOT EXISTS classified_at DATETIME NULL AFTER classify_done;
