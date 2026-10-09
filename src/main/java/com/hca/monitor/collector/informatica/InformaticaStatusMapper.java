package com.hca.monitor.collector.informatica;

import com.hca.monitor.collector.RunStatus;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

/** Maps one REP_WFLOW_RUN row to a RunStatus. Pure logic, no database access. */
public final class InformaticaStatusMapper {

    public static final String SOURCE = "INFORMATICA";
    static final int MAX_MSG_CHARS = 4000;

    private InformaticaStatusMapper() {}

    /** One row of REP_WFLOW_RUN. Times are in the Informatica server's local time. */
    public record Row(
            String folder,
            String workflowName,
            long workflowRunId,
            LocalDateTime startTime,
            LocalDateTime endTime,
            int runStatusCode,
            String runErrCode,
            String runErrMsg) {}

    /**
     * PowerCenter RUN_STATUS_CODE values:
     * 1 Succeeded, 2 Disabled, 3 Failed, 4 Stopped, 5 Aborted, 6 Running, 7 Suspending,
     * 8 Suspended, 9 Stopping, 10 Aborting, 11 Waiting, 12 Scheduled, 13 Unscheduled,
     * 14 Unknown, 15 Terminated.
     *
     * @return empty for codes that do not describe an actual run (disabled, waiting, scheduled...).
     */
    public static Optional<RunStatus.Status> status(int code) {
        return Optional.ofNullable(switch (code) {
            case 1 -> RunStatus.Status.SUCCEEDED;
            case 3, 15 -> RunStatus.Status.FAILED;
            case 4, 5 -> RunStatus.Status.ABORTED;
            case 6, 9, 10 -> RunStatus.Status.RUNNING;   // stopping/aborting are still in flight
            case 7, 8 -> RunStatus.Status.SUSPENDED;
            default -> null;                               // 2, 11, 12, 13, 14, unknown codes
        });
    }

    public static Optional<RunStatus> map(Row row, ZoneId serverZone, Instant observedAt) {
        return status(row.runStatusCode()).map(status -> {
            boolean finished = status != RunStatus.Status.RUNNING && status != RunStatus.Status.SUSPENDED;
            boolean hasError = row.runErrCode() != null && !row.runErrCode().isBlank()
                    && !"0".equals(row.runErrCode().trim());
            String jobId = row.folder() + "/" + row.workflowName();
            return new RunStatus(
                    SOURCE,
                    jobId,
                    row.workflowName(),
                    "infa:" + jobId + "/" + row.workflowRunId(),
                    String.valueOf(row.workflowRunId()),
                    status,
                    toInstant(row.startTime(), serverZone),
                    finished ? toInstant(row.endTime(), serverZone) : null,
                    hasError ? row.runErrCode().trim() : null,
                    hasError ? truncate(row.runErrMsg()) : null,
                    0,
                    null,
                    observedAt);
        });
    }

    private static Instant toInstant(LocalDateTime t, ZoneId zone) {
        return t == null ? null : t.atZone(zone).toInstant();
    }

    private static String truncate(String s) {
        if (s == null) return null;
        s = s.trim();
        return s.length() <= MAX_MSG_CHARS ? s : s.substring(0, MAX_MSG_CHARS);
    }
}
