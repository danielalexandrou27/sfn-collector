package com.hca.monitor.dashboard;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** JSON shapes returned by the dashboard API. */
public final class DashboardModel {

    private DashboardModel() {}

    public record Dashboard(
            LocalDate date,
            String timeZone,
            Instant generatedAt,
            Counts counts,
            List<Row> rows,            // registered pipelines: one row per workflow -> step function link
            List<Row> unregistered) {} // step functions that ran but are not in pipelines.yml

    /** Counted per step function (not per row), registered pipelines only. */
    public record Counts(int total, int failed, int late, int running, int notRun, int succeeded, int unregistered) {}

    public record Row(
            String pipeline,
            Job upstream,              // null when the step function has no upstream in the registry
            Job stepFunction,
            String expectedBy) {}      // "HH:mm" or null

    public record Job(
            String type,               // STEP_FUNCTIONS | INFORMATICA
            String name,
            String folder,             // Informatica only
            String status,             // SUCCEEDED, FAILED, TIMED_OUT, ABORTED, SUSPENDED, RUNNING, PENDING_REDRIVE, NOT_RUN, LATE
            Instant startedAt,
            Instant endedAt,
            Long durationSeconds,      // running: elapsed so far
            String error,
            String failedStep,
            int runCount,              // runs this batch day
            String runId,
            String consoleUrl,
            Incident incident) {}

    public record Incident(String number, String url) {}
}
