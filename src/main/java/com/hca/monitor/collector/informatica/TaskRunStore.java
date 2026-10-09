package com.hca.monitor.collector.informatica;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/** Stores task-level detail for Informatica runs in the engine's local database. */
@Repository
public class TaskRunStore {

    private final JdbcTemplate jdbc;

    public TaskRunStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Replaces all task rows for one workflow run. */
    @Transactional
    public void replace(String runId, List<TaskRun> tasks, Instant observedAt) {
        jdbc.update("DELETE FROM infa_task_run WHERE run_id = ?", runId);
        jdbc.batchUpdate("""
                INSERT INTO infa_task_run (run_id, instance_id, worklet_run_id, instance_name, task_type, status,
                    started_at, stopped_at, error_code, error_msg, mapping_name,
                    src_success_rows, src_failed_rows, targ_success_rows, targ_failed_rows,
                    first_error_code, first_error_msg, observed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                tasks, 100, (ps, t) -> {
                    ps.setString(1, runId);
                    ps.setLong(2, t.instanceId());
                    ps.setLong(3, t.workletRunId());
                    ps.setString(4, t.instanceName());
                    ps.setString(5, t.taskType());
                    ps.setString(6, t.status());
                    ps.setTimestamp(7, ts(t.startedAt()));
                    ps.setTimestamp(8, ts(t.stoppedAt()));
                    ps.setString(9, t.errorCode());
                    ps.setString(10, t.errorMsg());
                    ps.setString(11, t.mappingName());
                    ps.setObject(12, t.srcSuccessRows());
                    ps.setObject(13, t.srcFailedRows());
                    ps.setObject(14, t.targSuccessRows());
                    ps.setObject(15, t.targFailedRows());
                    ps.setString(16, t.firstErrorCode());
                    ps.setString(17, t.firstErrorMsg());
                    ps.setTimestamp(18, ts(observedAt));
                });
    }

    private static Timestamp ts(Instant i) {
        return i == null ? null : Timestamp.from(i);
    }
}
