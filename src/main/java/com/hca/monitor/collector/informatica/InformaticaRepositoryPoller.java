package com.hca.monitor.collector.informatica;

import com.hca.monitor.collector.RunStatus;
import com.hca.monitor.collector.RunStatusRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Polls the PowerCenter repository tables every minute and feeds each workflow run into the
 * same run store as Step Functions. When a workflow fails, aborts or suspends, it also reads the
 * task-level detail (which session failed, its first error, row counts).
 *
 * Read-only; uses its own small connection pool so the engine's local database is unaffected.
 *
 * Tables used (schema from monitor.informatica.repo-schema):
 *   OPB_WFLOW_RUN, OPB_SUBJECT                               - workflow runs and folder names
 *   OPB_TASK_INST_RUN, OPB_OBJECT_TYPE, OPB_SESS_TASK_LOG    - task detail for failed runs
 */
@Component
@ConditionalOnProperty(prefix = "monitor.informatica", name = "enabled", havingValue = "true")
public class InformaticaRepositoryPoller implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(InformaticaRepositoryPoller.class);
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$#]*");

    /** Runs still in flight are returned even if they started before the lookback window. */
    private static final String IN_FLIGHT_CODES = "6, 7, 8, 9, 10";

    /** Workflow outcomes that trigger the task-level drill-down. */
    private static final Set<RunStatus.Status> DRILL_DOWN = Set.of(
            RunStatus.Status.FAILED, RunStatus.Status.ABORTED, RunStatus.Status.SUSPENDED);

    private final InformaticaProperties props;
    private final RunStatusRepository repository;
    private final TaskRunStore taskStore;
    private final HikariDataSource dataSource;
    private final ZoneId serverZone;
    private final String workflowSql;
    private final String taskSql;

    private volatile boolean failing;

    public InformaticaRepositoryPoller(InformaticaProperties props, RunStatusRepository repository,
                                       TaskRunStore taskStore) {
        this.props = props;
        this.repository = repository;
        this.taskStore = taskStore;
        this.serverZone = ZoneId.of(props.serverTimeZone());
        String p = schemaPrefix(props.repoSchema());
        this.workflowSql = buildWorkflowSql(p, props.folders() == null ? 0 : props.folders().size());
        this.taskSql = buildTaskSql(p);
        this.dataSource = createDataSource(props);
        log.info("Informatica repository poller enabled: {} (schema {}) every {} ms",
                props.jdbcUrl(), props.repoSchema(), props.pollIntervalMs());
    }

    static String schemaPrefix(String schema) {
        if (schema == null || schema.isBlank()) return "";
        if (!IDENTIFIER.matcher(schema).matches()) {
            throw new IllegalArgumentException("Invalid repo-schema: " + schema);
        }
        return schema + ".";
    }

    static String buildWorkflowSql(String p, int folderCount) {
        StringBuilder s = new StringBuilder("""
                SELECT s.SUBJ_NAME, w.WORKFLOW_NAME, w.WORKFLOW_ID, w.WORKFLOW_RUN_ID,
                       w.START_TIME, w.END_TIME, w.RUN_STATUS_CODE, w.RUN_ERR_CODE, w.RUN_ERR_MSG
                FROM %sOPB_WFLOW_RUN w
                JOIN %sOPB_SUBJECT s ON s.SUBJ_ID = w.SUBJECT_ID
                WHERE (w.START_TIME >= ? OR w.RUN_STATUS_CODE IN (%s))""".formatted(p, p, IN_FLIGHT_CODES));
        if (folderCount > 0) {
            s.append(" AND s.SUBJ_NAME IN (").append("?,".repeat(folderCount - 1)).append("?)");
        }
        return s.toString();
    }

    static String buildTaskSql(String p) {
        return """
                SELECT t.INSTANCE_ID, t.WORKLET_RUN_ID, t.INSTANCE_NAME, o.OBJECT_TYPE_NAME,
                       t.START_TIME, t.END_TIME, t.RUN_STATUS_CODE, t.RUN_ERR_CODE, t.RUN_ERR_MSG,
                       l.MAPPING_NAME, l.SRC_SUCCESS_ROWS, l.SRC_FAILED_ROWS,
                       l.TARG_SUCCESS_ROWS, l.TARG_FAILED_ROWS, l.FIRST_ERROR_CODE, l.FIRST_ERROR_MSG
                FROM %sOPB_TASK_INST_RUN t
                LEFT JOIN %sOPB_OBJECT_TYPE o ON o.OBJECT_TYPE_ID = t.TASK_TYPE
                LEFT JOIN %sOPB_SESS_TASK_LOG l
                       ON l.WORKFLOW_ID     = t.WORKFLOW_ID
                      AND l.WORKFLOW_RUN_ID = t.WORKFLOW_RUN_ID
                      AND l.WORKLET_RUN_ID  = t.WORKLET_RUN_ID
                      AND l.INSTANCE_ID     = t.INSTANCE_ID
                WHERE t.WORKFLOW_ID = ? AND t.WORKFLOW_RUN_ID = ?
                ORDER BY t.START_TIME""".formatted(p, p, p);
    }

    private static HikariDataSource createDataSource(InformaticaProperties p) {
        HikariConfig c = new HikariConfig();
        c.setPoolName("informatica-repo");
        c.setJdbcUrl(p.jdbcUrl());
        c.setUsername(p.username());
        c.setPassword(p.password());
        c.setReadOnly(true);
        c.setMaximumPoolSize(2);
        c.setMinimumIdle(0);
        c.setConnectionTimeout(15_000);
        // Don't fail application startup if the repository is unreachable; the poller retries.
        c.setInitializationFailTimeout(-1);
        return new HikariDataSource(c);
    }

    @Scheduled(fixedDelayString = "${monitor.informatica.poll-interval-ms:60000}", initialDelay = 5_000)
    public void poll() {
        Instant observedAt = Instant.now();
        LocalDateTime since = LocalDateTime.now(serverZone).minusHours(props.lookbackHours());

        try (Connection conn = dataSource.getConnection()) {
            List<InformaticaStatusMapper.Row> rows = readWorkflowRuns(conn, since);

            if (failing) {
                log.info("INFORMATICA_COLLECTOR_RECOVERED: repository query working again");
                failing = false;
            }

            Map<RunStatusRepository.Outcome, Integer> counts = new EnumMap<>(RunStatusRepository.Outcome.class);
            for (InformaticaStatusMapper.Row row : rows) {
                var mapped = InformaticaStatusMapper.map(row, serverZone, observedAt);
                if (mapped.isEmpty()) continue;
                RunStatus r = mapped.get();
                RunStatusRepository.Outcome o = repository.upsert(r);
                counts.merge(o, 1, Integer::sum);

                boolean changed = o == RunStatusRepository.Outcome.INSERTED || o == RunStatusRepository.Outcome.UPDATED;
                if (!changed) continue;

                log.info("{} {} run {} -> {}{}", o, r.jobId(), r.runName(), r.status(),
                        r.errorCode() != null ? " (" + r.errorCode() + ": " + firstLine(r.errorCause()) + ")" : "");

                if (DRILL_DOWN.contains(r.status())) {
                    drillDown(conn, row, r, observedAt);
                }
            }
            log.debug("Informatica poll: {} rows, outcomes {}", rows.size(), counts);

        } catch (Exception e) {
            if (!failing) {
                log.error("INFORMATICA_COLLECTOR_FAILED: {} (check VPN/network, JDBC URL, credentials, "
                        + "repo-schema and SELECT access on the OPB_ tables). Retrying every poll.", e.toString());
                failing = true;
            }
        }
    }

    private List<InformaticaStatusMapper.Row> readWorkflowRuns(Connection conn, LocalDateTime since) throws SQLException {
        List<InformaticaStatusMapper.Row> rows = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(workflowSql)) {
            ps.setQueryTimeout(props.queryTimeoutSeconds());
            ps.setObject(1, since);
            int i = 2;
            for (String f : props.folders() == null ? List.<String>of() : props.folders()) {
                ps.setString(i++, f);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new InformaticaStatusMapper.Row(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getLong(3),
                            rs.getLong(4),
                            rs.getObject(5, LocalDateTime.class),
                            rs.getObject(6, LocalDateTime.class),
                            rs.getInt(7),
                            rs.getString(8),
                            rs.getString(9)));
                }
            }
        }
        return rows;
    }

    /** Reads task-level detail for a failed/aborted/suspended run and records the failing task. */
    private void drillDown(Connection conn, InformaticaStatusMapper.Row row, RunStatus r, Instant observedAt) {
        List<TaskRun> tasks = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(taskSql)) {
            ps.setQueryTimeout(props.queryTimeoutSeconds());
            ps.setLong(1, row.workflowId());
            ps.setLong(2, row.workflowRunId());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tasks.add(new TaskRun(
                            r.runId(),
                            rs.getLong(1),
                            rs.getLong(2),
                            rs.getString(3),
                            rs.getString(4),
                            InformaticaStatusMapper.statusName(rs.getInt(7)),
                            toInstant(rs.getObject(5, LocalDateTime.class)),
                            toInstant(rs.getObject(6, LocalDateTime.class)),
                            blankToNull(rs.getString(8)),
                            trim(rs.getString(9)),
                            rs.getString(10),
                            longOrNull(rs, 11),
                            longOrNull(rs, 12),
                            longOrNull(rs, 13),
                            longOrNull(rs, 14),
                            blankToNull(rs.getString(15)),
                            trim(rs.getString(16))));
                }
            }
        } catch (Exception e) {
            // Task detail is a nice-to-have; the workflow-level status is already stored.
            log.warn("Could not read task detail for {} run {}: {}", r.jobId(), r.runName(), e.toString());
            return;
        }

        taskStore.replace(r.runId(), tasks, observedAt);

        tasks.stream().filter(TaskRun::failed).findFirst().ifPresent(t -> {
            String cause = t.firstErrorMsg() != null ? t.firstErrorMsg() : t.errorMsg();
            repository.setFailedStep(r.runId(), t.instanceName(), cause);
            log.info("  failed task: {} [{}] {}{}", t.instanceName(), t.taskType(),
                    cause != null ? firstLine(cause) : "",
                    t.targSuccessRows() != null
                            ? " (target rows ok=" + t.targSuccessRows() + ", rejected=" + t.targFailedRows() + ")" : "");
        });
    }

    private Instant toInstant(LocalDateTime t) {
        return t == null ? null : t.atZone(serverZone).toInstant();
    }

    private static Long longOrNull(ResultSet rs, int col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank() || "0".equals(s.trim())) ? null : s.trim();
    }

    private static String trim(String s) {
        if (s == null || s.isBlank()) return null;
        s = s.trim();
        return s.length() <= InformaticaStatusMapper.MAX_MSG_CHARS ? s : s.substring(0, InformaticaStatusMapper.MAX_MSG_CHARS);
    }

    private static String firstLine(String s) {
        if (s == null) return "";
        int nl = s.indexOf('\n');
        String line = nl >= 0 ? s.substring(0, nl) : s;
        return line.length() > 200 ? line.substring(0, 200) + "..." : line;
    }

    @Override
    public void destroy() {
        dataSource.close();
    }
}
