# Step Functions SQS collector

First engine component: long-polls `sfn-status-events`, maps each EventBridge
"Step Functions Execution Status Change" message to a `RunStatus`, stores it, and
deletes the message only after the database write commits.

## Run on the laptop

```bash
aws sso login --profile hca-dev          # your SSO profile name
export AWS_PROFILE=hca-dev
export AWS_REGION=us-west-2
mvn spring-boot:run
```

Start an execution of the test state machine. You should see lines like:

```
INSERTED test-machine exec-1 -> RUNNING
UPDATED  test-machine exec-1 -> FAILED (Glue.AWSGlueException)
```

Run history is kept in `./data/monitor.mv.db` (H2). Browse it with any JDBC
client using `jdbc:h2:file:./data/monitor;AUTO_SERVER=TRUE`, user `sa`, no password:

```sql
SELECT job_name, run_name, status, started_at, stopped_at, error_code
FROM run_status ORDER BY observed_at DESC;
```

## Behaviour

| Situation | What happens |
| --- | --- |
| Valid Step Functions event | Stored, then deleted from the queue |
| Duplicate or late event (e.g. RUNNING after SUCCEEDED) | Ignored as stale, deleted |
| Redrive (higher `redriveCount`) | Replaces the earlier attempt's status |
| Other valid JSON event | Deleted, ignored |
| Unparseable message | Left on the queue; moves to `sfn-status-dlq` after 5 receives |
| Database write fails | Not deleted; retried when it becomes visible again |
| Keys expired / credentials missing | Logs `COLLECTOR_CREDENTIALS_FAILED` once, retries every 60 s, logs `COLLECTOR_RECOVERED` once fresh keys are in place |
| Wrong queue URL, region or account | Logs `COLLECTOR_CONFIG_ERROR`, retries every 60 s |

## Informatica (PowerCenter) collector

Polls the PowerCenter repository tables every 60 s (read-only) and writes each workflow run
into the same `run_status` table, with `source = 'INFORMATICA'`.

| Tables | Used for |
| --- | --- |
| `OPB_WFLOW_RUN` + `OPB_SUBJECT` | Workflow runs and folder names (every poll) |
| `OPB_TASK_INST_RUN` + `OPB_OBJECT_TYPE` + `OPB_SESS_TASK_LOG` | Task detail when a workflow fails, aborts or suspends: which session failed, its first error, source/target row counts. Stored in `infa_task_run`; the failed task name goes into `run_status.failed_step` |

Add these to the IntelliJ run configuration's environment variables:

```
INFA_REPO_ENABLED=true
INFA_REPO_JDBC_URL=jdbc:oracle:thin:@//dbhost:1521/SERVICE      (or the SQL Server form in application.yml)
INFA_REPO_USER=<read-only user>
INFA_REPO_PASSWORD=<password>
INFA_REPO_SCHEMA=INFA_REPO_PRD
```

Expected log lines:

```
Informatica repository poller enabled: jdbc:... every 60000 ms
INSERTED LOANS/wf_LOANS_EXTRACT run 12345 -> RUNNING
UPDATED  LOANS/wf_LOANS_EXTRACT run 12345 -> SUCCEEDED
```

The first poll loads every run from the last 24 hours, so expect a burst of INSERTED lines.
Later polls log only changes.

Status mapping (`RUN_STATUS_CODE`): 1 → SUCCEEDED, 3/15 → FAILED, 4/5 → ABORTED,
6/9/10 → RUNNING, 7/8 → SUSPENDED (stopped on error, waiting for recovery);
2, 11–14 (disabled, waiting, scheduled, unknown) are skipped.

`START_TIME`/`END_TIME` are in the Informatica server's local time and are converted using
`monitor.informatica.server-time-zone` (default America/Los_Angeles).

Combined view of both sources:

```sql
SELECT source, job_name, run_name, status, started_at, stopped_at, error_code
FROM run_status ORDER BY started_at DESC;
```

## Pipelines: linking workflows to state machines

`config/pipelines.yml` declares which Informatica workflows feed which state machines
(see `config/pipelines.example.yml`). The engine reloads it within 30 s of saving; an invalid
edit is rejected with every problem listed and the previous version stays active.

What the engine does with it:

| When | Check | Result |
| --- | --- | --- |
| A state machine starts | Did each upstream workflow finish successfully before it? | `run_link` row: OK (with lag), UPSTREAM_STILL_RUNNING, UPSTREAM_FAILED or NO_UPSTREAM_RUN. Non-OK is re-checked after 3 min, then raised as `RAN_ON_...` |
| A workflow or state machine fails | What is downstream of it? | `UPSTREAM_FAILED` alert listing every blocked node |
| Every 5 min | Upstream succeeded more than `max_start_lag_minutes` ago, downstream never started | `MISSING_TRIGGER` alert |

Alerts are logged as `PIPELINE_ALERT` and stored once each in `pipeline_alert`.

### Drafting the registry from history (edge discovery)

Writing 500 entries by hand isn't practical. The engine drafts them from timing:

1. Load history once. In the run configuration set `SFN_BACKFILL_ENABLED=true` and
   `INFA_BACKFILL_DAYS=30`, start the app and wait for `SFN_BACKFILL_DONE` and
   `INFORMATICA_BACKFILL_DONE` (paced at 2 ListExecutions calls/s: roughly 5–10 min for 500
   state machines). Then remove both variables.
2. About 10 minutes after startup (and every 6 h), the engine writes `data/edge-candidates.yml`:
   one block per state machine with the workflows that consistently finish shortly before it,
   the evidence for each, and a suggested `max_start_lag_minutes`.
3. Review it and copy the blocks you agree with into `config/pipelines.yml`. Confirmed edges
   drop out of the next draft.

Both sides must come from the same environment: discovery between prod Informatica and dev
Step Functions finds nothing meaningful.

Useful queries:

```sql
-- Open dependency alerts
SELECT created_at, alert_type, pipeline, node_id, detail FROM pipeline_alert
WHERE acknowledged = FALSE ORDER BY created_at DESC;

-- How each state machine run related to its upstream
SELECT l.pipeline, l.downstream_node, l.upstream_node, l.state, l.lag_seconds / 60 AS lag_min, l.downstream_start
FROM run_link l ORDER BY l.downstream_start DESC;
```

## Moving to EC2 later

No code change. Don't set `AWS_PROFILE`; the default credential chain picks up the
`hca-pipeline-monitor-engine` instance role. Swap the H2 URL for PostgreSQL when ready
(the schema is plain SQL).

## Troubleshooting on a corporate laptop

- `Unable to load credentials from any of the providers` with an SSO profile:
  check the `sso` and `ssooidc` SDK modules are on the classpath (they are in this pom).
- `PKIX path building failed`: import the company root CA into the JDK truststore
  (`keytool -importcert -cacerts -alias corp-root -file corp-root.cer`), and set
  `HTTPS_PROXY` / `-Dhttps.proxyHost` if a proxy is required.
