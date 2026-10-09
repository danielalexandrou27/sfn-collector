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

Polls `REP_WFLOW_RUN` in the PowerCenter repository every 60 s (read-only) and writes each
workflow run into the same `run_status` table, with `source = 'INFORMATICA'`.

Add these to the IntelliJ run configuration's environment variables:

```
INFA_REPO_ENABLED=true
INFA_REPO_JDBC_URL=jdbc:oracle:thin:@//dbhost:1521/SERVICE      (or the SQL Server form in application.yml)
INFA_REPO_USER=<read-only user>
INFA_REPO_PASSWORD=<password>
INFA_REPO_SCHEMA=<schema owning the MX views, if not the user's default>
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
