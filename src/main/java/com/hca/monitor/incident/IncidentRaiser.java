package com.hca.monitor.incident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Raises one ServiceNow incident per failed run without the ServiceNow API:
 *   1. builds a prefilled new-incident link (incident.do?sys_id=-1&sysparm_query=...)
 *   2. DRY_RUN: writes the link to the outbox so you can open it yourself
 *      BROWSER: opens it signed in as you, checks every field prefilled, clicks Submit, records the INC number
 *   3. when the same job later runs successfully, resolves the incident (if resolve is enabled)
 *
 * One open incident per job: a repeat failure while its incident is still open is recorded against it
 * instead of raising another. Only failures that end after the engine starts are ticketed.
 * Runs on its own thread so a slow browser or a sign-in wait never holds up the collectors.
 */
@Component
@ConditionalOnProperty(prefix = "monitor.incidents", name = "enabled", havingValue = "true")
public class IncidentRaiser implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(IncidentRaiser.class);
    private static final DateTimeFormatter FILE_TS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final IncidentProperties props;
    private final IncidentStore store;
    private final Instant startedAt = Instant.now();
    private final ScheduledExecutorService thread = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "servicenow-incidents");
        t.setDaemon(true);
        return t;
    });
    private IncidentProperties.Mode mode;
    private boolean resolveEnabled;
    private ServiceNowBrowser browser;

    public IncidentRaiser(IncidentProperties props, IncidentStore store) {
        this.props = props;
        this.store = store;
        this.mode = props.mode();

        List<String> unfilled = ServiceNowLinks.unfilled(props.fields());
        if (mode == IncidentProperties.Mode.BROWSER && (!unfilled.isEmpty() || props.instanceUrl().contains("<"))) {
            log.error("INCIDENT_CONFIG_ERROR: monitor.incidents still has placeholder values for {} - "
                    + "staying in DRY_RUN until they are filled in", unfilled.isEmpty() ? List.of("instance-url") : unfilled);
            mode = IncidentProperties.Mode.DRY_RUN;
        }
        resolveEnabled = mode == IncidentProperties.Mode.BROWSER && props.resolve() != null && props.resolve().enabled();
        if (resolveEnabled && !ServiceNowLinks.unfilled(props.resolve().fields()).isEmpty()) {
            log.error("INCIDENT_CONFIG_ERROR: monitor.incidents.resolve.fields still has placeholders for {} - auto-resolve is off",
                    ServiceNowLinks.unfilled(props.resolve().fields()));
            resolveEnabled = false;
        }
        if (mode == IncidentProperties.Mode.BROWSER) {
            browser = new ServiceNowBrowser(props.browser(), props.instanceUrl());
        }
        log.info("ServiceNow incidents enabled: mode {}, statuses {}, auto-resolve {}", mode, props.statuses(), resolveEnabled);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        thread.scheduleWithFixedDelay(this::cycle, 60, 60, TimeUnit.SECONDS);
    }

    void cycle() {
        try {
            raiseNew();
            if (resolveEnabled) {
                resolveRecovered();
            }
        } catch (Exception e) {
            log.error("Incident cycle failed: {}", e.toString());
        }
    }

    private void raiseNew() {
        Instant since = startedAt.minus(Duration.ofMinutes(5));
        var candidates = store.candidates(props.statuses(), since, props.maxPerPoll());
        for (IncidentCandidate c : candidates) {
            // Step Functions: give the failure enricher time to find the failed state and service error.
            boolean waiting = "STEP_FUNCTIONS".equals(c.source()) && c.detailSummary() == null
                    && c.observedAt() != null
                    && c.observedAt().isAfter(Instant.now().minus(Duration.ofMinutes(props.waitForDetailMinutes())));
            if (waiting) continue;

            var open = store.openIncidentFor(c.jobId());
            if (open.isPresent() && mode == IncidentProperties.Mode.BROWSER && closedInServiceNow(open.get())) {
                store.markResolved(open.get().number());
                log.info("{} was closed in ServiceNow - raising a new incident for {}", open.get().number(), c.jobName());
                open = java.util.Optional.empty();
            }
            if (open.isPresent()) {
                store.record(c, "DUPLICATE", open.get().number(), open.get().sysId(), null, null, 1);
                log.warn("INCIDENT_ALREADY_OPEN {} {} failed again - recorded against {}", c.jobName(), c.runName(),
                        open.get().number());
                continue;
            }

            Map<String, String> fields = IncidentFormatter.incidentFields(c, props);
            String link = ServiceNowLinks.newIncident(props.instanceUrl(), fields);
            int attempt = c.previousAttempts() + 1;

            if (mode == IncidentProperties.Mode.DRY_RUN) {
                Path file = writeOutbox(c, fields, link);
                store.record(c, "DRY_RUN", null, null, link, null, attempt);
                log.warn("INCIDENT_DRY_RUN {} {}: '{}' - open this link to raise it: {} (details in {})",
                        c.jobName(), c.runName(), fields.get("short_description"), link, file);
                continue;
            }

            try {
                // An earlier attempt clicked Submit before failing: check that incident exists before submitting again.
                if (c.previousNumber() != null) {
                    var existing = browser.findIncident(c.previousNumber());
                    if (existing != null) {
                        store.record(c, "CREATED", existing.number(), existing.sysId(), link, null, attempt);
                        log.warn("INCIDENT_CREATED {} for {} {} (found from the earlier attempt; not submitted again)",
                                existing.number(), c.jobName(), c.runName());
                        continue;
                    }
                }
                var created = browser.createIncident(link, fields);
                store.record(c, "CREATED", created.number(), created.sysId(), link, null, attempt);
                log.warn("INCIDENT_CREATED {} for {} {}: {}", created.number(), c.jobName(), c.runName(),
                        fields.get("short_description"));
            } catch (Exception e) {
                String submitted = e instanceof ServiceNowBrowser.IncidentException ie ? ie.submittedNumber : null;
                store.record(c, "FAILED", submitted, null, link, e.getMessage(), attempt);
                log.error("INCIDENT_FAILED {} {} (attempt {}/{}): {}\n  Raise it by hand with this link: {}",
                        c.jobName(), c.runName(), attempt, IncidentStore.MAX_ATTEMPTS, e.getMessage(), link);
            }
        }
        if (candidates.size() >= props.maxPerPoll()) {
            log.warn("INCIDENT_THROTTLED: {} failures this minute (max-per-poll); the rest follow next minute", candidates.size());
        }
    }

    /** True if ServiceNow shows the incident as Resolved/Closed/Canceled (e.g. closed by hand). */
    private boolean closedInServiceNow(IncidentStore.OpenIncident open) {
        try {
            String state = browser.incidentState(open.sysId());
            return props.closedStates() != null && props.closedStates().contains(state);
        } catch (Exception e) {
            log.warn("Could not check state of {} ({}); treating it as still open", open.number(), e.getMessage());
            return false;
        }
    }

    private void resolveRecovered() {
        ZoneId zone = ZoneId.of(props.timeZone());
        DateTimeFormatter t = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm z");
        for (var r : store.resolvable()) {
            String resolution = "Resolved automatically by the pipeline monitoring engine: " + r.jobName() + " run "
                    + r.okRunName() + " succeeded" + (r.okEndedAt() != null ? " at " + t.format(r.okEndedAt().atZone(zone)) : "") + ".";
            Map<String, String> fields = new LinkedHashMap<>();
            props.resolve().fields().forEach((k, v) -> fields.put(k, v.replace("{{resolution}}", resolution)));
            try {
                browser.resolveIncident(r.sysId(), r.number(), fields);
                store.markResolved(r.number());
                log.warn("INCIDENT_RESOLVED {} - {} run {} succeeded", r.number(), r.jobName(), r.okRunName());
            } catch (Exception e) {
                store.resolveFailed(r.runId(), e.getMessage());
                log.error("INCIDENT_RESOLVE_FAILED {} (attempt {}/{}): {}", r.number(), r.resolveAttempts() + 1,
                        IncidentStore.MAX_ATTEMPTS, e.getMessage());
            }
        }
    }

    private Path writeOutbox(IncidentCandidate c, Map<String, String> fields, String link) {
        try {
            Path dir = Path.of(props.outboxDir());
            Files.createDirectories(dir);
            Path file = dir.resolve(FILE_TS.format(Instant.now().atZone(ZoneId.of(props.timeZone()))) + "_"
                    + c.jobName().replaceAll("[^A-Za-z0-9_.-]", "_") + ".txt");
            StringBuilder s = new StringBuilder("Open this link to raise the incident:\n").append(link).append("\n\nFields:\n");
            fields.forEach((k, v) -> s.append(k).append(": ").append(v.replace("\n", "\n    ")).append('\n'));
            Files.writeString(file, s.toString(), StandardCharsets.UTF_8);
            return file;
        } catch (Exception e) {
            return Path.of("(outbox write failed: " + e.getMessage() + ")");
        }
    }

    @Override
    public void destroy() {
        thread.shutdownNow();
        if (browser != null) browser.close();
    }
}
