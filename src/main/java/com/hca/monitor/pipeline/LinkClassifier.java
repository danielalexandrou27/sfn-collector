package com.hca.monitor.pipeline;

import java.time.Duration;
import java.time.Instant;

/** Decides whether a downstream run started on good upstream data. Pure logic. */
public final class LinkClassifier {

    private LinkClassifier() {}

    public enum State {
        /** Upstream succeeded and finished before the downstream started. */
        OK,
        /** Downstream started while upstream was still running (or suspended). */
        UPSTREAM_STILL_RUNNING,
        /** The latest upstream run before the downstream start failed, aborted or timed out. */
        UPSTREAM_FAILED,
        /** No upstream run in the look-back window before the downstream start. */
        NO_UPSTREAM_RUN
    }

    /** Minimal view of a stored run. */
    public record Run(String runId, String status, Instant startedAt, Instant stoppedAt) {}

    public record Result(State state, Duration lag) {}

    public static Result classify(Run upstream, Instant downstreamStart) {
        if (upstream == null) {
            return new Result(State.NO_UPSTREAM_RUN, null);
        }
        switch (upstream.status()) {
            case "SUCCEEDED" -> {
                if (upstream.stoppedAt() != null && !upstream.stoppedAt().isAfter(downstreamStart)) {
                    return new Result(State.OK, Duration.between(upstream.stoppedAt(), downstreamStart));
                }
                return new Result(State.UPSTREAM_STILL_RUNNING, null); // finished after downstream started
            }
            case "RUNNING", "SUSPENDED", "PENDING_REDRIVE" -> {
                return new Result(State.UPSTREAM_STILL_RUNNING, null);
            }
            default -> {
                return new Result(State.UPSTREAM_FAILED, null); // FAILED, ABORTED, TIMED_OUT
            }
        }
    }

    public static boolean isFailure(String status) {
        return switch (status) {
            case "FAILED", "ABORTED", "TIMED_OUT", "SUSPENDED" -> true;
            default -> false;
        };
    }
}
