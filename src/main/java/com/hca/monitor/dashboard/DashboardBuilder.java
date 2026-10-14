package com.hca.monitor.dashboard;

import com.hca.monitor.dashboard.DashboardModel.Counts;
import com.hca.monitor.dashboard.DashboardModel.Dashboard;
import com.hca.monitor.dashboard.DashboardModel.Incident;
import com.hca.monitor.dashboard.DashboardModel.Job;
import com.hca.monitor.dashboard.DashboardModel.Row;
import com.hca.monitor.pipeline.PipelineGraph;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns the pipeline registry plus one batch day's runs into dashboard rows. Pure logic.
 *
 * Every step function in pipelines.yml gets a row per upstream link (or one row if it has no
 * upstream), even if it hasn't run: NOT_RUN, or LATE once its expected_by time has passed.
 * Each job shows its latest run of the day and how many runs there were.
 */
public final class DashboardBuilder {

    private DashboardBuilder() {}

    /** Where links in the rows point. serviceNowUrl may be null (no incident links). */
    public record Links(String serviceNowUrl, String awsRegion) {
        String incident(String sysId) {
            return serviceNowUrl == null || serviceNowUrl.contains("<") || sysId == null ? null
                    : trim(serviceNowUrl) + "/incident.do?sys_id=" + sysId;
        }
        String console(String executionArn) {
            return executionArn == null || awsRegion == null ? null
                    : "https://" + awsRegion + ".console.aws.amazon.com/states/home?region=" + awsRegion
                      + "#/v2/executions/details/" + URLEncoder.encode(executionArn, StandardCharsets.UTF_8);
        }
        private static String trim(String u) { return u.endsWith("/") ? u.substring(0, u.length() - 1) : u; }
    }

    public static Dashboard build(PipelineGraph graph, List<RunView> runsOfDay, LocalDate date, ZoneId zone,
                                  Instant now, Links links) {
        Map<String, List<RunView>> byKey = new LinkedHashMap<>();
        runsOfDay.stream()
                .sorted(Comparator.comparing(RunView::startedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .forEach(r -> byKey.computeIfAbsent(r.key(), k -> new ArrayList<>()).add(r));

        List<Row> rows = new ArrayList<>();
        Set<String> registered = new HashSet<>();
        Map<String, String> sfStatus = new LinkedHashMap<>();   // pipeline/node -> status, for counts

        for (PipelineGraph.NodeRef n : graph.nodes()) {
            if (!PipelineGraph.SFN.equals(n.source())) continue;
            registered.add(n.key());
            LocalTime expected = graph.expectedBy(n);
            Job sf = job(n.source(), n.matchValue(), byKey.get(n.key()), expected, date, zone, now, links);
            sfStatus.put(n.pipeline() + "/" + n.nodeId(), sf.status());
            String exp = expected == null ? null : expected.toString();

            List<PipelineGraph.Edge> ups = graph.upstreamOf(n);
            if (ups.isEmpty()) {
                rows.add(new Row(n.pipeline(), null, sf, exp));
            }
            for (PipelineGraph.Edge e : ups) {
                PipelineGraph.NodeRef u = e.upstream();
                Job up = job(u.source(), u.matchValue(), byKey.get(u.key()), null, date, zone, now, links);
                rows.add(new Row(n.pipeline(), up, sf, exp));
            }
        }

        List<Row> unregistered = new ArrayList<>();
        for (var e : byKey.entrySet()) {
            if (!e.getKey().startsWith(PipelineGraph.SFN + "|") || registered.contains(e.getKey())) continue;
            String name = e.getKey().substring(PipelineGraph.SFN.length() + 1);
            unregistered.add(new Row(null, null, job(PipelineGraph.SFN, name, e.getValue(), null, date, zone, now, links), null));
        }

        Comparator<Row> order = Comparator
                .comparingInt((Row r) -> severity(r.stepFunction().status()))
                .thenComparing(r -> r.pipeline() == null ? "" : r.pipeline())
                .thenComparing(r -> r.stepFunction().name())
                .thenComparing(r -> r.upstream() == null ? "" : r.upstream().name());
        rows.sort(order);
        unregistered.sort(order);

        return new Dashboard(date, zone.getId(), now, counts(sfStatus.values(), unregistered.size()), rows, unregistered);
    }

    /** Builds a job cell from its runs of the day (newest first). */
    static Job job(String type, String matchValue, List<RunView> runs, LocalTime expectedBy, LocalDate date,
                   ZoneId zone, Instant now, Links links) {
        String name = matchValue, folder = null;
        if (PipelineGraph.INFA.equals(type)) {
            int i = matchValue.indexOf('/');
            folder = i > 0 ? matchValue.substring(0, i) : null;
            name = i > 0 ? matchValue.substring(i + 1) : matchValue;
        }
        if (runs == null || runs.isEmpty()) {
            boolean late = expectedBy != null && now.isAfter(date.atTime(expectedBy).atZone(zone).toInstant());
            return new Job(type, name, folder, late ? "LATE" : "NOT_RUN", null, null, null, null, null, 0,
                    null, null, null);
        }
        RunView r = runs.get(0);
        return toJob(type, name, folder, r, runs.size(), now, links);
    }

    /** One attempt (also used by the attempts endpoint). */
    static Job toJob(String type, String name, String folder, RunView r, int runCount, Instant now, Links links) {
        boolean running = "RUNNING".equals(r.status()) || "PENDING_REDRIVE".equals(r.status());
        Long duration = r.startedAt() == null ? null
                : Duration.between(r.startedAt(), r.stoppedAt() != null ? r.stoppedAt() : running ? now : r.startedAt()).toSeconds();
        String error = failed(r.status()) ? errorText(r) : null;
        Incident inc = r.incidentNumber() == null ? null : new Incident(r.incidentNumber(), links.incident(r.incidentSysId()));
        return new Job(type, name, folder, r.status(), r.startedAt(), r.stoppedAt(), duration, error,
                failed(r.status()) ? r.failedStep() : null, runCount, r.runId(),
                PipelineGraph.SFN.equals(type) ? links.console(r.runId()) : null, inc);
    }

    static String errorText(RunView r) {
        if (r.detailSummary() != null && !r.detailSummary().isBlank()) return r.detailSummary();
        String cause = r.errorCause() == null ? null : r.errorCause().replaceAll("\\s+", " ").trim();
        if (cause != null && cause.length() > 500) cause = cause.substring(0, 497) + "...";
        if (r.errorCode() != null && cause != null) return r.errorCode() + ": " + cause;
        return r.errorCode() != null ? r.errorCode() : cause;
    }

    static boolean failed(String status) {
        return switch (status) {
            case "FAILED", "TIMED_OUT", "ABORTED", "SUSPENDED" -> true;
            default -> false;
        };
    }

    /** Sort order: problems first. */
    static int severity(String status) {
        return switch (status) {
            case "FAILED", "TIMED_OUT", "SUSPENDED" -> 0;
            case "LATE" -> 1;
            case "ABORTED" -> 2;
            case "RUNNING", "PENDING_REDRIVE" -> 3;
            case "NOT_RUN" -> 4;
            default -> 5; // SUCCEEDED
        };
    }

    static Counts counts(Iterable<String> statuses, int unregistered) {
        int total = 0, failed = 0, late = 0, running = 0, notRun = 0, ok = 0;
        for (String s : statuses) {
            total++;
            if (failed(s)) failed++;
            else if ("LATE".equals(s)) late++;
            else if ("RUNNING".equals(s) || "PENDING_REDRIVE".equals(s)) running++;
            else if ("NOT_RUN".equals(s)) notRun++;
            else ok++;
        }
        return new Counts(total, failed, late, running, notRun, ok, unregistered);
    }
}
