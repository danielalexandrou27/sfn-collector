package com.hca.monitor.dashboard;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

@Repository
public class DashboardStore {

    private final JdbcTemplate jdbc;

    public DashboardStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Runs that started in [from, to), with failure detail and incident where present. */
    public List<RunView> runsStartedBetween(Instant from, Instant to) {
        return jdbc.query("""
                SELECT s.run_id, s.source, s.job_id, s.job_name, s.run_name, s.status, s.started_at, s.stopped_at,
                       s.error_code, s.error_cause, s.failed_step, d.summary, i.incident_number, i.incident_sys_id
                FROM run_status s
                LEFT JOIN run_failure_detail d ON d.run_id = s.run_id
                LEFT JOIN sn_incident i ON i.run_id = s.run_id
                WHERE s.started_at >= ? AND s.started_at < ?""",
                (rs, n) -> new RunView(
                        rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                        rs.getString(6), inst(rs.getObject(7, OffsetDateTime.class)), inst(rs.getObject(8, OffsetDateTime.class)),
                        rs.getString(9), rs.getString(10), rs.getString(11), rs.getString(12), rs.getString(13),
                        rs.getString(14)),
                Timestamp.from(from), Timestamp.from(to));
    }

    private static Instant inst(OffsetDateTime o) {
        return o == null ? null : o.toInstant();
    }
}
