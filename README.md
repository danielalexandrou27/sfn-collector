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
Informatica wf_LOANS_EXTRACT [LOANS] run 12345: RUNNING | started 2026-10-13 02:00:05
Informatica wf_LOANS_EXTRACT [LOANS] run 12345: SUCCEEDED | started 2026-10-13 02:00:05 | ended 02:31:40 (31m 35s)
```

The first poll loads every run from the last 24 hours, so expect a burst of lines.
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

## Step Functions error details

The FAILED event from SQS carries only the execution's top-level `error`/`cause`, often just
`States.TaskFailed` or a custom error from a Fail state. When an execution fails or times out, the
engine reads its execution history (`states:GetExecutionHistory`) on a background thread and
records the real root cause in `run_failure_detail`:

- **failed state**: the state that actually failed, even behind a Catch -> notify -> Fail chain
- **attempts**: how many times that state failed (Retry count + 1)
- **service error**: parsed from the task's cause. Glue gives job name, job run id and
  `ErrorMessage`; Lambda gives `errorType` and `errorMessage`; other services give their message field
- **summary**: one line, e.g. `glue hca_loans_load_job: AnalysisException: Table or view not found: stg.loans_daily`

Failures that recovered on retry are ignored. The failed state is also written to
`run_status.failed_step`. Log line: `FAILURE_DETAIL <machine> <execution>: state '<name>' -> <summary>`.

```sql
SELECT s.job_name, s.run_name, s.stopped_at, d.failed_state, d.attempts, d.summary
FROM run_status s JOIN run_failure_detail d ON d.run_id = s.run_id
ORDER BY s.stopped_at DESC;
```

## ServiceNow incidents (prefilled link + one-click submit, no API)

For each failed run (FAILED, TIMED_OUT, SUSPENDED) the engine builds a prefilled new-incident link:

```
https://<instance>.service-now.com/incident.do?sys_id=-1&sysparm_query=caller_id=...^category=...^short_description=<job> failed^description=<error>
```

- **DRY_RUN** (default): the link is logged (`INCIDENT_DRY_RUN`) and written to `data/incident-outbox/`. Open it, check the form, click Submit yourself.
- **BROWSER**: the engine opens the link in Edge, signed in as you, checks that every field prefilled exactly as
  configured (it will not submit otherwise), clicks Submit, and confirms the INC number exists (`INCIDENT_CREATED INC...`).
  If anything goes wrong it saves a screenshot to `data/incident-screenshots/` and logs the link so you can raise it by hand.
- **Auto-resolve** (`resolve.enabled`): when the same job later runs successfully, the engine opens the incident,
  sets the resolve fields and clicks Resolve (`INCIDENT_RESOLVED`).
- One open incident per job: a repeat failure while it is open is recorded against it (`INCIDENT_ALREADY_OPEN`).
  Only failures that end after the engine starts are ticketed; at most 5 per minute.
- Step Functions incidents wait up to 3 minutes for the failure detail so the description has the real error.

### How duplicates are prevented

| Layer | Rule |
| --- | --- |
| One row per run | `run_status` is keyed by run id (execution ARN / Informatica run), so repeated events never create a second failure |
| One ticket per run | `sn_incident` is keyed by the same run id; only runs with no row are picked up |
| One open ticket per job | a repeat failure while the job's INC is open is recorded as `DUPLICATE` against it |
| Closed by hand | before recording a duplicate, the INC's state is checked in ServiceNow; if Resolved/Closed/Canceled (`closed-states`), a new INC is raised |
| Submit clicked, then a later step failed | the INC number (assigned before Submit) is saved with the failed attempt; the retry checks that INC exists before submitting again |
| No back-ticketing | only failures that end after the engine starts are ticketed |

Keep the `data/` folder: it holds this history. Deleting it while failures are open can lead to repeat tickets.

### One-time setup: get the internal field values

Reference fields need sys_ids and choice fields need their internal values, not the labels.
No developer tools needed:

1. Open `https://<instance>.service-now.com/incident_list.do` and click the filter (funnel) icon.
2. Add one "is" condition per field (Caller, Category, Subcategory, Configuration item, Vendor Related,
   Channel, Assignment group, Assigned to) and click Run. It doesn't matter if nothing matches.
3. Right-click the last item in the breadcrumb and choose **Copy query**. The clipboard holds
   `caller_id=<sys_id>^category=application^...`: every internal field name and value.
4. Put each `name=value` pair under `fields:` in `application.yml` (already filled in for HCA EDW Application Support).
5. For auto-resolve: filter the list on Resolution code is <your usual code>, Copy query, and put the
   `close_code` value in `resolve.fields.close_code`.

### Running it

```
INCIDENTS_ENABLED=true
SERVICENOW_URL=https://<instance>.service-now.com
INCIDENTS_MODE=BROWSER            # once DRY_RUN links look right
```

The first BROWSER run opens Edge with its own profile (`data/servicenow-browser-profile`). Sign in when asked
(`SERVICENOW_LOGIN_REQUIRED`). The sign-in is kept for later runs until your SSO session expires.
BROWSER mode stays in DRY_RUN while any `<...>` placeholder remains in `fields`.

Uses Playwright with the installed Edge (`browser.channel: msedge`), so no browser download is needed.

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

### Maintaining the registry

Links are entered by hand in `config/pipelines.yml`; the engine only acts on links listed there.

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

## Dashboard API (for the pipeline-dashboard web page)

The engine serves a read-only API on port 8080 (`SERVER_PORT` to change). The web page itself is a
separate project, `pipeline-dashboard`; see its README for running and hosting it.

Set the shared sign-in before starting the engine (never commit these):

```
DASHBOARD_USER=pipeline
DASHBOARD_PASSWORD=<shared team password>     # if blank, a random one is printed in the log at startup
```

| Endpoint | Returns |
|---|---|
| `POST /api/login` `{username, password}` | Signs in (session cookie, 8 h). 5 wrong tries locks that machine out for 5 min. |
| `POST /api/logout` | Signs out. |
| `GET /api/dashboard?date=YYYY-MM-DD` | One row per workflow → step function link for the batch day (default today, midnight Pacific), plus counts and unregistered step functions. |
| `GET /api/attempts?type=STEP_FUNCTIONS&name=...&date=...` | Every run of one job that day, newest first. |
| `GET /api/stream` | Server-sent events: `changed` whenever a run or incident changes (at most every 2 s), `heartbeat` every 25 s. |

To flag a step function as **Late** when it hasn't run by a certain time, add `expected_by` (24-hour
Pacific time) to its node in `pipelines.yml`:

```yaml
      - id: load
        step_function: hca-loans-load
        after: [extract, dims]
        expected_by: "04:30"
```

Every call except login needs a signed-in session. The engine only reads its own database for these;
nothing on the dashboard can change a run, an incident or the registry.
