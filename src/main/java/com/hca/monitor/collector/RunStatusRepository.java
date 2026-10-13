package com.hca.monitor.collector;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Stores the latest known state of each run. Events can repeat or arrive out of order,
 * so an update only applies when it moves the run forward:
 *   - a higher redriveCount always wins (a redrive is a new attempt), otherwise
 *   - a terminal status is never overwritten by RUNNING / PENDING_REDRIVE.
 */
@Repository
public class RunStatusRepository {

    private final JdbcTemplate jdbc;

    public RunStatusRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public enum Outcome { INSERTED, UPDATED, UNCHANGED, IGNORED_STALE }

    @Transactional
    public Outcome upsert(RunStatus r) {
        List<Object[]> existing = jdbc.query(
                "SELECT status, redrive_count, stopped_at, error_code FROM run_status WHERE run_id = ? FOR UPDATE",
                (rs, n) -> new Object[]{rs.getString(1), rs.getInt(2),
                        toInstant(rs.getObject(3, OffsetDateTime.class)), rs.getString(4)},
                r.runId());

        if (existing.isEmpty()) {
            jdbc.update("""
                    INSERT INTO run_status (run_id, source, job_id, job_name, run_name, status,
                        started_at, stopped_at, error_code, error_cause, redrive_count, event_time, observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    r.runId(), r.source(), r.jobId(), r.jobName(), r.runName(), r.status().name(),
                    ts(r.startedAt()), ts(r.stoppedAt()), r.errorCode(), r.errorCause(),
                    r.redriveCount(), ts(r.eventTime()), ts(r.observedAt()));
            return Outcome.INSERTED;
        }

        RunStatus.Status currentStatus = RunStatus.Status.valueOf((String) existing.get(0)[0]);
        int currentRedrive = (Integer) existing.get(0)[1];
        Instant currentStoppedAt = (Instant) existing.get(0)[2];
        String currentErrorCode = (String) existing.get(0)[3];

        // Pollers (Informatica) re-read the same rows every cycle; skip writes when nothing changed.
        if (currentStatus == r.status() && currentRedrive == r.redriveCount()
                && Objects.equals(currentStoppedAt, r.stoppedAt())
                && Objects.equals(currentErrorCode, r.errorCode())) {
            return Outcome.UNCHANGED;
        }

        if (!movesForward(currentStatus, currentRedrive, r)) {
            return Outcome.IGNORED_STALE;
        }

        jdbc.update("""
                UPDATE run_status SET status = ?, started_at = COALESCE(?, started_at), stopped_at = ?,
                    error_code = ?, error_cause = ?, redrive_count = ?, event_time = ?, observed_at = ?
                WHERE run_id = ?""",
                r.status().name(), ts(r.startedAt()), ts(r.stoppedAt()), r.errorCode(), r.errorCause(),
                r.redriveCount(), ts(r.eventTime()), ts(r.observedAt()), r.runId());
        return Outcome.UPDATED;
    }

    static boolean movesForward(RunStatus.Status current, int currentRedrive, RunStatus incoming) {
        if (incoming.redriveCount() != currentRedrive) {
            return incoming.redriveCount() > currentRedrive;
        }
        return !current.isTerminal() || incoming.status().isTerminal();
    }


    /** Records which step/task failed; fills error_cause only if the run-level message was empty. */
    public void setFailedStep(String runId, String failedStep, String cause) {
        jdbc.update("""
                UPDATE run_status SET failed_step = ?, error_cause = COALESCE(error_cause, ?)
                WHERE run_id = ?""", failedStep, cause, runId);
    }

    public Optional<String> findStatus(String runId) {
        return jdbc.query("SELECT status FROM run_status WHERE run_id = ?",
                (rs, n) -> rs.getString(1), runId).stream().findFirst();
    }

    private static Instant toInstant(OffsetDateTime o) {
        return o == null ? null : o.toInstant();
    }

    private static Timestamp ts(Instant i) {
        return i == null ? null : Timestamp.from(i);
    }
}
