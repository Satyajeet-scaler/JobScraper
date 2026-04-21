-- Normalize existing values first (best effort, lower-case + trim trailing slash)
UPDATE lusha_recruiters
SET linkedin_url = NULLIF(TRIM(TRAILING '/' FROM LOWER(linkedin_url)), '')
WHERE linkedin_url IS NOT NULL;

-- Remove duplicates by linkedin_url, keep the most recently updated row.
DELETE lr1
FROM lusha_recruiters lr1
JOIN lusha_recruiters lr2
  ON lr1.linkedin_url = lr2.linkedin_url
 AND lr1.id < lr2.id
WHERE lr1.linkedin_url IS NOT NULL;

-- Replace non-unique index (if present) with a unique key.
ALTER TABLE lusha_recruiters
  DROP INDEX idx_lusha_recruiters_linkedin_url,
  ADD UNIQUE KEY uq_lusha_recruiters_linkedin_url (linkedin_url);
