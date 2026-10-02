package com.hca.monitor.collector;

import java.time.Instant;

/** Normalized run record every collector emits (Step Functions, Informatica, ...). */
public record RunStatus(
        String source,
        String jobId,
        String jobName,
        String runId,
        String runName,
        Status status,
        Instant startedAt,
        Instant stoppedAt,
        String errorCode,
        String errorCause,
        int redriveCount,
        Instant eventTime,
        Instant observedAt) {

    public enum Status {
        RUNNING, SUCCEEDED, FAILED, TIMED_OUT, ABORTED, PENDING_REDRIVE;

        public boolean isTerminal() {
            return this != RUNNING && this != PENDING_REDRIVE;
        }
    }
}
