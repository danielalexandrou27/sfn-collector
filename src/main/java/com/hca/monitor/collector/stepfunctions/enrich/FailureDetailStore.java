package com.hca.monitor.collector.stepfunctions.enrich;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
public class FailureDetailStore {

    private final JdbcTemplate jdbc;

    public FailureDetailStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean exists(String runId) {
        Integer c = jdbc.queryForObject("SELECT COUNT(*) FROM run_failure_detail WHERE run_id = ?", Integer.class, runId);
        return c != null && c > 0;
    }

    /** Saves the detail and copies the failed state / readable message onto run_status. */
    @Transactional
    public void save(String runId, FailureAnalyzer.Analysis a, CauseParser.Parsed p, String summary) {
        jdbc.update("DELETE FROM run_failure_detail WHERE run_id = ?", runId);
        jdbc.update("""
                INSERT INTO run_failure_detail (run_id, failed_state, failure_event, service, resource,
                    service_job, service_run_id, error, error_type, message, summary, attempts,
                    execution_error, execution_cause, task_cause, analysed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                runId, a.failedState(), a.failureEvent(), p.service(), a.resource(),
                p.serviceJob(), p.serviceRunId(), cut(a.error(), 512), p.errorType(), cut(p.message(), 4000),
                summary, a.attempts(), cut(a.executionError(), 512), cut(a.executionCause(), 4000),
                cut(a.cause(), 8000), Timestamp.from(Instant.now()));
        jdbc.update("UPDATE run_status SET failed_step = ? WHERE run_id = ?", a.failedState(), runId);
    }

    private static String cut(String s, int max) {
        return s == null || s.length() <= max ? s : s.substring(0, max);
    }
}
