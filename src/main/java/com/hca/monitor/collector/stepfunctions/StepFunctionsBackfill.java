package com.hca.monitor.collector.stepfunctions;

import com.hca.monitor.collector.RunStatus;
import com.hca.monitor.collector.RunStatusRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.ExecutionListItem;
import software.amazon.awssdk.services.sfn.model.ListExecutionsResponse;
import software.amazon.awssdk.services.sfn.model.ListStateMachinesResponse;
import software.amazon.awssdk.services.sfn.model.StateMachineListItem;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * One-off history load: lists every state machine (optionally filtered by name prefix) and inserts
 * its executions from the last N days into run_status. Insert-only, so it never overwrites data
 * from live events. Paced to stay well under the ListExecutions quota shared with other users.
 *
 * Needed so edge discovery has enough history to work with. Turn it off after the first run.
 */
@Component
@ConditionalOnProperty(prefix = "monitor.sfn-backfill", name = "enabled", havingValue = "true")
public class StepFunctionsBackfill {

    private static final Logger log = LoggerFactory.getLogger(StepFunctionsBackfill.class);

    private final SfnClient sfn;
    private final RunStatusRepository repository;
    private final int days;
    private final String namePrefix;
    private final long pauseMs;

    public StepFunctionsBackfill(SfnClient sfn, RunStatusRepository repository,
                                 @Value("${monitor.sfn-backfill.days:30}") int days,
                                 @Value("${monitor.sfn-backfill.name-prefix:}") String namePrefix,
                                 @Value("${monitor.sfn-backfill.calls-per-second:2}") double callsPerSecond) {
        this.sfn = sfn;
        this.repository = repository;
        this.days = days;
        this.namePrefix = namePrefix == null ? "" : namePrefix;
        this.pauseMs = (long) (1000 / Math.max(0.1, callsPerSecond));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Thread.ofPlatform().name("sfn-backfill").daemon(true).start(this::run);
    }

    void run() {
        Instant cutoff = Instant.now().minus(Duration.ofDays(days));
        try {
            List<StateMachineListItem> machines = listStateMachines();
            log.info("SFN_BACKFILL: loading {} days of executions for {} state machines{}",
                    days, machines.size(), namePrefix.isEmpty() ? "" : " (prefix '" + namePrefix + "')");
            long inserted = 0, seen = 0;
            int done = 0;
            for (StateMachineListItem sm : machines) {
                String token = null;
                outer:
                do {
                    pause();
                    final String t = token;
                    ListExecutionsResponse page = sfn.listExecutions(b -> b
                            .stateMachineArn(sm.stateMachineArn()).maxResults(1000).nextToken(t));
                    for (ExecutionListItem e : page.executions()) {   // newest first
                        if (e.startDate() != null && e.startDate().isBefore(cutoff)) break outer;
                        seen++;
                        RunStatus r = toRunStatus(sm, e);
                        if (r != null && repository.insertIfAbsent(r)) inserted++;
                    }
                    token = page.nextToken();
                } while (token != null);

                if (++done % 25 == 0 || done == machines.size()) {
                    log.info("SFN_BACKFILL: {}/{} state machines, {} executions seen, {} new",
                            done, machines.size(), seen, inserted);
                }
            }
            log.info("SFN_BACKFILL_DONE: {} executions seen, {} inserted. You can set monitor.sfn-backfill.enabled=false now.",
                    seen, inserted);
        } catch (Exception e) {
            log.error("SFN_BACKFILL_FAILED: {} (needs states:ListStateMachines and states:ListExecutions)", e.toString());
        }
    }

    private List<StateMachineListItem> listStateMachines() {
        List<StateMachineListItem> all = new ArrayList<>();
        String token = null;
        do {
            pause();
            final String t = token;
            ListStateMachinesResponse page = sfn.listStateMachines(b -> b.maxResults(1000).nextToken(t));
            for (StateMachineListItem sm : page.stateMachines()) {
                if (sm.name().startsWith(namePrefix)) all.add(sm);
            }
            token = page.nextToken();
        } while (token != null);
        return all;
    }

    private static RunStatus toRunStatus(StateMachineListItem sm, ExecutionListItem e) {
        RunStatus.Status status;
        try {
            status = RunStatus.Status.valueOf(e.statusAsString());
        } catch (Exception unknown) {
            return null;
        }
        return new RunStatus("STEP_FUNCTIONS", sm.stateMachineArn(), sm.name(), e.executionArn(), e.name(),
                status, e.startDate(), e.stopDate(), null, null,
                e.redriveCount() == null ? 0 : e.redriveCount(), null, Instant.now());
    }

    private void pause() {
        try {
            Thread.sleep(pauseMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
