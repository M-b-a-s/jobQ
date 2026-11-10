CREATE TABLE jobs (
  id          BIGSERIAL PRIMARY KEY,
  type        TEXT        NOT NULL,
  payload     JSONB       NOT NULL DEFAULT '{}',
  status      TEXT        NOT NULL DEFAULT 'pending'
              CHECK (status IN ('pending','running','done','failed')),
  attempts    INT         NOT NULL DEFAULT 0,
  run_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at   TIMESTAMPTZ,
  last_error  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);

CREATE INDEX jobs_claimable_idx ON jobs (run_at, id) WHERE status = 'pending';

-- Only for verifying "no job runs twice". Not part of a real queue.
CREATE TABLE job_runs (
  id        BIGSERIAL PRIMARY KEY,
  job_id    BIGINT      NOT NULL,
  worker_id TEXT        NOT NULL,
  ran_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);