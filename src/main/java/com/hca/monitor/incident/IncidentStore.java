package com.hca.monitor.incident;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class IncidentStore {

    static final int MAX_ATTEMPTS = 3;

    private final JdbcTemplate jdbc;

    public IncidentStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Failed runs that ended (or were first seen) after {@code since} and have no ticket yet.
     * Runs whose earlier attempt failed are retried up to MAX_ATTEMPTS times.
     */
    public List<IncidentCandidate> candidates(Collection<String> statuses, Instant since, int limit) {
        String in = String.join(",", statuses.stream().map(s -> "?").toList());
        Object[] args = new Object[statuses.size() + 3];
        int i = 0;
        for (String s : statuses) args[i++] = s;
        args[i++] = Timestamp.from(since);
        args[i++] = MAX_ATTEMPTS;
        args[i] = limit;
        return jdbc.query("""
                SELECT s.run_id, s.source, s.job_id, s.job_name, s.run_name, s.status,
                       s.started_at, s.stopped_at, s.observed_at, s.error_code, s.error_cause, s.failed_step,
                       d.summary, d.message, d.attempts,
                       (SELECT MAX(a.detail) FROM pipeline_alert a
                         WHERE a.run_id = s.run_id AND a.alert_type = 'UPSTREAM_FAILED') AS downstream,
                       COALESCE(n.attempts, 0) AS prev_attempts, n.incident_number AS prev_number
                FROM run_status s
                LEFT JOIN run_failure_detail d ON d.run_id = s.run_id
                LEFT JOIN sn_incident n ON n.run_id = s.run_id
                WHERE s.status IN (%s)
                  AND COALESCE(s.stopped_at, s.observed_at) >= ?
                  AND (n.run_id IS NULL OR (n.status = 'FAILED' AND n.attempts < ?))
                ORDER BY COALESCE(s.stopped_at, s.observed_at)
                LIMIT ?""".formatted(in),
                (rs, k) -> new IncidentCandidate(
                        rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                        rs.getString(6), inst(rs.getObject(7, OffsetDateTime.class)),
                        inst(rs.getObject(8, OffsetDateTime.class)), inst(rs.getObject(9, OffsetDateTime.class)),
                        rs.getString(10), rs.getString(11), rs.getString(12), rs.getString(13), rs.getString(14),
                        (Integer) rs.getObject(15), rs.getString(16), rs.getInt(17), rs.getString(18)),
                args);
    }

    public record OpenIncident(String number, String sysId) {}

    /** An open (created, not resolved) incident already raised for this job, if any. */
    public Optional<OpenIncident> openIncidentFor(String jobId) {
        return jdbc.query("""
                SELECT incident_number, incident_sys_id FROM sn_incident
                WHERE job_id = ? AND status = 'CREATED' AND resolved_at IS NULL
                ORDER BY created_at DESC LIMIT 1""",
                (rs, k) -> new OpenIncident(rs.getString(1), rs.getString(2)), jobId).stream().findFirst();
    }

    public void record(IncidentCandidate c, String status, String number, String sysId, String link,
                       String error, int attempts) {
        jdbc.update("DELETE FROM sn_incident WHERE run_id = ?", c.runId());
        jdbc.update("""
                INSERT INTO sn_incident (run_id, job_id, job_name, status, incident_number, incident_sys_id,
                    link, error, attempts, resolve_attempts, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
                c.runId(), c.jobId(), c.jobName(), status, number, sysId, cut(link, 8000), cut(error, 2000),
                attempts, Timestamp.from(Instant.now()));
    }

    public record Resolvable(String runId, String jobName, String number, String sysId, int resolveAttempts,
                             String okRunName, Instant okEndedAt) {}

    /** Open incidents whose job has since run successfully. */
    public List<Resolvable> resolvable() {
        Map<String, Resolvable> first = new LinkedHashMap<>();
        jdbc.query("""
                SELECT i.run_id, i.job_name, i.incident_number, i.incident_sys_id, i.resolve_attempts,
                       ok.run_name, ok.stopped_at
                FROM sn_incident i
                JOIN run_status f  ON f.run_id = i.run_id
                JOIN run_status ok ON ok.job_id = i.job_id AND ok.status = 'SUCCEEDED'
                                  AND ok.started_at > COALESCE(f.stopped_at, f.observed_at)
                WHERE i.status = 'CREATED' AND i.resolved_at IS NULL AND i.resolve_attempts < ?
                ORDER BY ok.stopped_at""",
                rs -> {
                    first.putIfAbsent(rs.getString(1), new Resolvable(rs.getString(1), rs.getString(2), rs.getString(3),
                            rs.getString(4), rs.getInt(5), rs.getString(6), inst(rs.getObject(7, OffsetDateTime.class))));
                }, MAX_ATTEMPTS);
        return List.copyOf(first.values());
    }

    /** Marks the incident (and any runs recorded as duplicates of it) resolved. */
    public void markResolved(String number) {
        jdbc.update("UPDATE sn_incident SET resolved_at = ? WHERE incident_number = ?", Timestamp.from(Instant.now()), number);
    }

    public void resolveFailed(String runId, String error) {
        jdbc.update("UPDATE sn_incident SET resolve_attempts = resolve_attempts + 1, error = ? WHERE run_id = ?",
                cut(error, 2000), runId);
    }

    private static String cut(String s, int max) {
        return s == null || s.length() <= max ? s : s.substring(0, max);
    }

    private static Instant inst(OffsetDateTime o) {
        return o == null ? null : o.toInstant();
    }
}
