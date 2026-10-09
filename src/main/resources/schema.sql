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

-- Task-level detail for Informatica workflow runs (sessions, commands, worklets).
CREATE TABLE IF NOT EXISTS infa_task_run (
    run_id            VARCHAR(512) NOT NULL,       -- run_status.run_id of the workflow run
    instance_id       BIGINT       NOT NULL,
    worklet_run_id    BIGINT       NOT NULL,
    instance_name     VARCHAR(256),
    task_type         VARCHAR(64),
    status            VARCHAR(32),
    started_at        TIMESTAMP WITH TIME ZONE,
    stopped_at        TIMESTAMP WITH TIME ZONE,
    error_code        VARCHAR(64),
    error_msg         VARCHAR(4000),
    mapping_name      VARCHAR(256),
    src_success_rows  BIGINT,
    src_failed_rows   BIGINT,
    targ_success_rows BIGINT,
    targ_failed_rows  BIGINT,
    first_error_code  VARCHAR(64),
    first_error_msg   VARCHAR(4000),
    observed_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (run_id, instance_id, worklet_run_id)
);

CREATE INDEX IF NOT EXISTS ix_run_status_job ON run_status (job_id, started_at);
CREATE INDEX IF NOT EXISTS ix_run_status_status ON run_status (status);
