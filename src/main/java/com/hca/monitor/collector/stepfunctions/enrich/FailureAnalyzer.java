package com.hca.monitor.collector.stepfunctions.enrich;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Finds the root failure in a Step Functions execution history. Pure logic.
 *
 * The top-level ExecutionFailed often only says "States.TaskFailed", or carries a custom error
 * from a Fail state reached through a Catch. The real cause is the most recent task-level
 * failure, so that is preferred; the execution-level error is kept alongside it.
 */
public final class FailureAnalyzer {

    private FailureAnalyzer() {}

    static final Set<String> TASK_FAILURES = Set.of(
            "TaskFailed", "TaskTimedOut", "TaskStartFailed", "TaskSubmitFailed",
            "LambdaFunctionFailed", "LambdaFunctionTimedOut", "LambdaFunctionStartFailed",
            "ActivityFailed", "ActivityTimedOut", "MapRunFailed");

    static final Set<String> EXECUTION_FAILURES = Set.of("ExecutionFailed", "ExecutionTimedOut");

    public record Analysis(
            String failedState,
            String failureEvent,     // e.g. TaskFailed
            String resourceType,     // e.g. glue
            String resource,         // e.g. startJobRun.sync
            String error,            // task-level error, e.g. States.TaskFailed, Lambda.Unknown
            String cause,            // task-level cause (often JSON from the service)
            int attempts,            // task-level failures seen for this state (retries + 1)
            String executionError,
            String executionCause) {}

    /** @param newestFirst history in reverse order (as requested with reverseOrder=true). */
    public static Optional<Analysis> analyze(List<HistoryItem> newestFirst) {
        if (newestFirst.isEmpty()) return Optional.empty();
        Map<Long, HistoryItem> byId = new HashMap<>();
        for (HistoryItem h : newestFirst) byId.put(h.id(), h);

        HistoryItem exec = newestFirst.stream().filter(h -> EXECUTION_FAILURES.contains(h.type())).findFirst().orElse(null);
        HistoryItem task = null;
        for (int i = 0; i < newestFirst.size() && task == null; i++) {
            HistoryItem h = newestFirst.get(i);
            if (TASK_FAILURES.contains(h.type()) && !recoveredLater(newestFirst, byId, i)) task = h;
        }
        HistoryItem root = task != null ? task : exec;
        if (root == null) return Optional.empty();

        int rootIdx = newestFirst.indexOf(root);
        String state = stateNameFor(root, byId, newestFirst, rootIdx);

        int attempts = 0;
        if (task != null) {
            for (int i = 0; i < newestFirst.size(); i++) {
                HistoryItem h = newestFirst.get(i);
                if (TASK_FAILURES.contains(h.type()) && java.util.Objects.equals(state, stateNameFor(h, byId, newestFirst, i))) {
                    attempts++;
                }
            }
        }

        return Optional.of(new Analysis(
                state,
                root.type(),
                task != null ? task.resourceType() : null,
                task != null ? task.resource() : null,
                root.error(),
                root.cause(),
                Math.max(attempts, 1),
                exec != null ? exec.error() : null,
                exec != null ? exec.cause() : null));
    }

    static final Set<String> TASK_SUCCESSES = Set.of("TaskSucceeded", "LambdaFunctionSucceeded", "ActivitySucceeded");

    /** True if the same state succeeded after this failure (i.e. a Retry worked), so it is not the root cause. */
    private static boolean recoveredLater(List<HistoryItem> newestFirst, Map<Long, HistoryItem> byId, int failureIdx) {
        String state = stateNameFor(newestFirst.get(failureIdx), byId, newestFirst, failureIdx);
        if (state == null) return false;
        for (int i = 0; i < failureIdx; i++) {               // newer events
            HistoryItem h = newestFirst.get(i);
            if (TASK_SUCCESSES.contains(h.type()) && state.equals(stateNameFor(h, byId, newestFirst, i))) return true;
        }
        return false;
    }

    /** Follows previousEventId back to the *StateEntered event; falls back to the next older one in the list. */
    static String stateNameFor(HistoryItem h, Map<Long, HistoryItem> byId, List<HistoryItem> newestFirst, int idx) {
        HistoryItem cur = h;
        for (int hops = 0; cur != null && hops < 200; hops++) {
            if (cur.type() != null && cur.type().endsWith("StateEntered")) return cur.stateName();
            cur = byId.get(cur.previousEventId());
        }
        for (int i = idx + 1; i < newestFirst.size(); i++) {
            HistoryItem o = newestFirst.get(i);
            if (o.type() != null && o.type().endsWith("StateEntered")) return o.stateName();
        }
        return null;
    }
}
