package com.hca.monitor.incident;

import java.time.Instant;

/** A failed run that needs a ticket, with whatever detail the engine has gathered. */
public record IncidentCandidate(
        String runId,
        String source,          // STEP_FUNCTIONS | INFORMATICA
        String jobId,
        String jobName,
        String runName,
        String status,
        Instant startedAt,
        Instant stoppedAt,
        Instant observedAt,
        String errorCode,
        String errorCause,
        String failedStep,
        String detailSummary,   // Step Functions: from run_failure_detail
        String detailMessage,
        Integer attempts,
        String downstreamImpact, // from pipeline_alert UPSTREAM_FAILED, if the run is in a registered pipeline
        int previousAttempts,
        String previousNumber) { // INC number from an earlier attempt where Submit was clicked
}
