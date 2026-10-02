CREATE TABLE IF NOT EXISTS run_status (
    run_id          VARCHAR(512)  PRIMARY KEY,   -- executionArn
    source          VARCHAR(32)   NOT NULL,      -- STEP_FUNCTIONS | INFORMATICA
    job_id          VARCHAR(512)  NOT NULL,      -- stateMachineArn
    job_name        VARCHAR(256),
    run_name        VARCHAR(256),
    status          VARCHAR(32)   NOT NULL,
    started_at      TIMESTAMP WITH TIME ZONE,
    stopped_at      TIMESTAMP WITH TIME ZONE,
    error_code      VARCHAR(512),
    error_cause     VARCHAR(4000),
    redrive_count   INT           NOT NULL DEFAULT 0,
    failed_step     VARCHAR(256),                -- filled later by the enricher
    event_time      TIMESTAMP WITH TIME ZONE,    -- EventBridge event time
    observed_at     TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_run_status_job ON run_status (job_id, started_at);
CREATE INDEX IF NOT EXISTS ix_run_status_status ON run_status (status);
