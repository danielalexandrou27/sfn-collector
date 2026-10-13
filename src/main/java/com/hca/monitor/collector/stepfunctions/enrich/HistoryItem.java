package com.hca.monitor.collector.stepfunctions.enrich;

/**
 * The parts of a Step Functions history event the enricher needs, copied out of the SDK type
 * so the analysis logic can be tested without AWS.
 */
public record HistoryItem(
        long id,
        long previousEventId,
        String type,          // e.g. TaskFailed, TaskStateEntered, ExecutionFailed
        String stateName,     // set on *StateEntered events
        String resourceType,  // e.g. glue, lambda (task events)
        String resource,      // e.g. startJobRun.sync
        String error,
        String cause) {
}
