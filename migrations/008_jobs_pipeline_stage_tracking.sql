ALTER TABLE jobs
  ADD COLUMN relevancy_checked BOOLEAN NOT NULL DEFAULT FALSE AFTER run_date,
  ADD COLUMN recruiter_info_checked BOOLEAN NOT NULL DEFAULT FALSE AFTER relevancy_checked,
  ADD COLUMN candidates_jd_eval_done BOOLEAN NOT NULL DEFAULT FALSE AFTER recruiter_info_checked;

CREATE INDEX idx_jobs_relevancy_pending
  ON jobs (requested_role, run_date, relevancy_checked);

CREATE INDEX idx_jobs_recruiter_pending
  ON jobs (requested_role, run_date, recruiter_info_checked);

CREATE INDEX idx_jobs_jd_eval_pending
  ON jobs (requested_role, run_date, candidates_jd_eval_done);
