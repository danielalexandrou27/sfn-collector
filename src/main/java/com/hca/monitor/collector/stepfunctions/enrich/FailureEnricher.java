package com.hca.monitor.collector.stepfunctions.enrich;

import com.hca.monitor.collector.RunStatus;
import com.hca.monitor.pipeline.RunChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.GetExecutionHistoryResponse;
import software.amazon.awssdk.services.sfn.model.HistoryEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * When a Step Functions execution fails or times out, reads its execution history to find the
 * failed state and the underlying service error (Glue, Lambda, ...). Runs on its own thread so the
 * SQS consumer is never slowed down. Results go to run_failure_detail and run_status.failed_step.
 *
 * Needs states:GetExecutionHistory.
 */
@Component
public class FailureEnricher implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(FailureEnricher.class);
    private static final Set<RunStatus.Status> ENRICH = Set.of(RunStatus.Status.FAILED, RunStatus.Status.TIMED_OUT);

    private final SfnClient sfn;
    private final FailureDetailStore store;
    private final boolean enabled;
    private final int maxEvents;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "sfn-failure-enricher");
        t.setDaemon(true);
        return t;
    });

    public FailureEnricher(SfnClient sfn, FailureDetailStore store,
                           @Value("${monitor.sfn-enricher.enabled:true}") boolean enabled,
                           @Value("${monitor.sfn-enricher.max-history-events:1000}") int maxEvents) {
        this.sfn = sfn;
        this.store = store;
        this.enabled = enabled;
        this.maxEvents = maxEvents;
    }

    @EventListener
    public void onRunChanged(RunChangedEvent event) {
        RunStatus r = event.run();
        if (!enabled || !"STEP_FUNCTIONS".equals(r.source()) || !ENRICH.contains(r.status())) return;
        worker.submit(() -> enrich(r));
    }

    void enrich(RunStatus r) {
        try {
            if (store.exists(r.runId())) return;

            List<HistoryItem> items = new ArrayList<>();
            String token = null;
            do {
                final String t = token;
                GetExecutionHistoryResponse page = sfn.getExecutionHistory(b -> b
                        .executionArn(r.runId()).reverseOrder(true).maxResults(Math.min(1000, maxEvents)).nextToken(t));
                for (HistoryEvent e : page.events()) items.add(toItem(e));
                token = page.nextToken();
                // Newest events come first; stop once a task-level failure is in hand.
                if (items.stream().anyMatch(h -> FailureAnalyzer.TASK_FAILURES.contains(h.type()))) break;
            } while (token != null && items.size() < maxEvents);

            var analysis = FailureAnalyzer.analyze(items);
            if (analysis.isEmpty()) {
                log.info("No failure event found in history of {} {}", r.jobName(), r.runName());
                return;
            }
            var a = analysis.get();
            var parsed = CauseParser.parse(a.resourceType(), a.cause() != null ? a.cause() : a.executionCause());
            String summary = parsed.summary(a.error());
            store.save(r.runId(), a, parsed, summary);

            log.warn("FAILURE_DETAIL {} {}: state '{}'{} -> {}",
                    r.jobName(), r.runName(), a.failedState(),
                    a.attempts() > 1 ? " (after " + a.attempts() + " attempts)" : "", summary);
        } catch (Exception e) {
            log.warn("Could not read execution history for {} {}: {} (needs states:GetExecutionHistory)",
                    r.jobName(), r.runName(), e.toString());
        }
    }

    static HistoryItem toItem(HistoryEvent e) {
        String type = e.typeAsString();
        String state = e.stateEnteredEventDetails() != null ? e.stateEnteredEventDetails().name() : null;
        String resourceType = null, resource = null, error = null, cause = null;

        if (e.taskFailedEventDetails() != null) {
            var d = e.taskFailedEventDetails();
            resourceType = d.resourceType(); resource = d.resource(); error = d.error(); cause = d.cause();
        } else if (e.taskTimedOutEventDetails() != null) {
            var d = e.taskTimedOutEventDetails();
            resourceType = d.resourceType(); resource = d.resource(); error = d.error(); cause = d.cause();
        } else if (e.taskStartFailedEventDetails() != null) {
            var d = e.taskStartFailedEventDetails();
            resourceType = d.resourceType(); resource = d.resource(); error = d.error(); cause = d.cause();
        } else if (e.taskSubmitFailedEventDetails() != null) {
            var d = e.taskSubmitFailedEventDetails();
            resourceType = d.resourceType(); resource = d.resource(); error = d.error(); cause = d.cause();
        } else if (e.lambdaFunctionFailedEventDetails() != null) {
            var d = e.lambdaFunctionFailedEventDetails();
            resourceType = "lambda"; error = d.error(); cause = d.cause();
        } else if (e.lambdaFunctionTimedOutEventDetails() != null) {
            var d = e.lambdaFunctionTimedOutEventDetails();
            resourceType = "lambda"; error = d.error(); cause = d.cause();
        } else if (e.lambdaFunctionStartFailedEventDetails() != null) {
            var d = e.lambdaFunctionStartFailedEventDetails();
            resourceType = "lambda"; error = d.error(); cause = d.cause();
        } else if (e.activityFailedEventDetails() != null) {
            var d = e.activityFailedEventDetails();
            resourceType = "activity"; error = d.error(); cause = d.cause();
        } else if (e.activityTimedOutEventDetails() != null) {
            var d = e.activityTimedOutEventDetails();
            resourceType = "activity"; error = d.error(); cause = d.cause();
        } else if (e.mapRunFailedEventDetails() != null) {
            var d = e.mapRunFailedEventDetails();
            resourceType = "map"; error = d.error(); cause = d.cause();
        } else if (e.executionFailedEventDetails() != null) {
            error = e.executionFailedEventDetails().error(); cause = e.executionFailedEventDetails().cause();
        } else if (e.executionTimedOutEventDetails() != null) {
            error = e.executionTimedOutEventDetails().error(); cause = e.executionTimedOutEventDetails().cause();
        }
        return new HistoryItem(e.id(), e.previousEventId() == null ? 0 : e.previousEventId(),
                type, state, resourceType, resource, error, cause);
    }

    @Override
    public void destroy() {
        worker.shutdownNow();
    }
}
