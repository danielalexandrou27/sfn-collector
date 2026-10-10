package com.hca.monitor.pipeline;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

/** Queries over run_status for the linker, and storage for run_link / pipeline_alert. */
@Repository
public class PipelineStore {

    private final JdbcTemplate jdbc;

    public PipelineStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<LinkClassifier.Run> RUN = (rs, n) -> new LinkClassifier.Run(
            rs.getString(1), rs.getString(2),
            instant(rs.getObject(3, OffsetDateTime.class)), instant(rs.getObject(4, OffsetDateTime.class)));

    /** Step Functions are matched by state machine name, Informatica by "folder/workflow". */
    private static String matchColumn(PipelineGraph.NodeRef n) {
        return PipelineGraph.SFN.equals(n.source()) ? "job_name" : "job_id";
    }

    /** Latest run of {@code node} that started before {@code before} (and after {@code notBefore}). */
    public Optional<LinkClassifier.Run> latestRunStartedBefore(PipelineGraph.NodeRef node, Instant before, Instant notBefore) {
        return jdbc.query("""
                SELECT run_id, status, started_at, stopped_at FROM run_status
                WHERE source = ? AND %s = ? AND started_at < ? AND started_at >= ?
                ORDER BY started_at DESC LIMIT 1""".formatted(matchColumn(node)),
                RUN, node.source(), node.matchValue(), ts(before), ts(notBefore)).stream().findFirst();
    }

    /** Successful runs of {@code node} that finished in [from, to]. */
    public List<LinkClassifier.Run> succeededRunsEndedBetween(PipelineGraph.NodeRef node, Instant from, Instant to) {
        return jdbc.query("""
                SELECT run_id, status, started_at, stopped_at FROM run_status
                WHERE source = ? AND %s = ? AND status = 'SUCCEEDED' AND stopped_at BETWEEN ? AND ?""".formatted(matchColumn(node)),
                RUN, node.source(), node.matchValue(), ts(from), ts(to));
    }

    public boolean anyRunStartedAtOrAfter(PipelineGraph.NodeRef node, Instant after) {
        Integer c = jdbc.queryForObject("""
                SELECT COUNT(*) FROM run_status WHERE source = ? AND %s = ? AND started_at >= ?""".formatted(matchColumn(node)),
                Integer.class, node.source(), node.matchValue(), ts(after));
        return c != null && c > 0;
    }

    public void saveLink(String pipeline, String downstreamNode, String downstreamRunId, Instant downstreamStart,
                         String upstreamNode, String upstreamRunId, LinkClassifier.State state, Duration lag) {
        jdbc.update("DELETE FROM run_link WHERE downstream_run_id = ? AND pipeline = ? AND upstream_node = ?",
                downstreamRunId, pipeline, upstreamNode);
        jdbc.update("""
                INSERT INTO run_link (pipeline, downstream_node, downstream_run_id, upstream_node, upstream_run_id,
                    state, lag_seconds, downstream_start, alerted, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, FALSE, ?)""",
                pipeline, downstreamNode, downstreamRunId, upstreamNode, upstreamRunId, state.name(),
                lag == null ? null : lag.toSeconds(), ts(downstreamStart), ts(Instant.now()));
    }

    public record PendingLink(String pipeline, String downstreamNode, String downstreamRunId,
                              String upstreamNode, Instant downstreamStart) {}

    /** Non-OK links older than {@code olderThan} that have not been alerted yet. */
    public List<PendingLink> pendingLinks(Instant olderThan) {
        return jdbc.query("""
                SELECT pipeline, downstream_node, downstream_run_id, upstream_node, downstream_start
                FROM run_link WHERE state <> 'OK' AND alerted = FALSE AND created_at < ?""",
                (rs, n) -> new PendingLink(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        instant(rs.getObject(5, OffsetDateTime.class))),
                ts(olderThan));
    }

    public void resolveLink(PendingLink p, String upstreamRunId, LinkClassifier.State state, Duration lag) {
        jdbc.update("""
                UPDATE run_link SET upstream_run_id = ?, state = ?, lag_seconds = ?, alerted = ?
                WHERE downstream_run_id = ? AND pipeline = ? AND upstream_node = ?""",
                upstreamRunId, state.name(), lag == null ? null : lag.toSeconds(), state != LinkClassifier.State.OK,
                p.downstreamRunId(), p.pipeline(), p.upstreamNode());
    }

    /** @return true if the alert is new; false if the same alert was already raised. */
    public boolean raiseAlert(String type, String pipeline, String nodeId, String runId, String detail) {
        try {
            jdbc.update("""
                    INSERT INTO pipeline_alert (alert_type, pipeline, node_id, run_id, detail, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)""", type, pipeline, nodeId, runId, detail, ts(Instant.now()));
            return true;
        } catch (DataIntegrityViolationException duplicate) {
            return false;
        }
    }

    /** Recent runs of both sources, for edge discovery. */
    public List<EdgeDiscovery.Run> runsSince(Instant since) {
        return jdbc.query("""
                SELECT source, job_id, job_name, status, started_at, stopped_at FROM run_status
                WHERE started_at >= ?""",
                (rs, n) -> new EdgeDiscovery.Run(
                        rs.getString(1),
                        PipelineGraph.SFN.equals(rs.getString(1)) ? rs.getString(3) : rs.getString(2),
                        rs.getString(4),
                        instant(rs.getObject(5, OffsetDateTime.class)),
                        instant(rs.getObject(6, OffsetDateTime.class))),
                ts(since));
    }

    private static Instant instant(OffsetDateTime o) { return o == null ? null : o.toInstant(); }

    private static Timestamp ts(Instant i) { return i == null ? null : Timestamp.from(i); }
}
