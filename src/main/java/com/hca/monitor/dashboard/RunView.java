package com.hca.monitor.dashboard;

import java.time.Instant;

/** One run as the dashboard sees it: run_status joined with failure detail and its ServiceNow incident. */
public record RunView(
        String runId,
        String source,        // STEP_FUNCTIONS | INFORMATICA
        String jobId,         // state machine ARN, or "folder/workflow"
        String jobName,
        String runName,
        String status,
        Instant startedAt,
        Instant stoppedAt,
        String errorCode,
        String errorCause,
        String failedStep,
        String detailSummary,
        String incidentNumber,
        String incidentSysId) {

    /** Same key the pipeline registry uses: state machines by name, workflows by "folder/workflow". */
    public String key() {
        return source + "|" + ("STEP_FUNCTIONS".equals(source) ? jobName : jobId);
    }
}
