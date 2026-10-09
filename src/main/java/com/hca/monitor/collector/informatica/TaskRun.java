package com.hca.monitor.collector.informatica;

import java.time.Instant;

/**
 * One task inside an Informatica workflow run (session, command, worklet, ...), from
 * OPB_TASK_INST_RUN + OPB_OBJECT_TYPE, with session stats from OPB_SESS_TASK_LOG when present.
 */
public record TaskRun(
        String runId,            // parent run_status.run_id
        long instanceId,
        long workletRunId,
        String instanceName,
        String taskType,
        String status,
        Instant startedAt,
        Instant stoppedAt,
        String errorCode,
        String errorMsg,
        String mappingName,
        Long srcSuccessRows,
        Long srcFailedRows,
        Long targSuccessRows,
        Long targFailedRows,
        String firstErrorCode,
        String firstErrorMsg) {

    public boolean failed() {
        return "FAILED".equals(status) || "ABORTED".equals(status) || "SUSPENDED".equals(status);
    }
}
