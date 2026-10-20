package com.hca.monitor.notify;

import com.hca.monitor.collector.RunStatus;
import com.hca.monitor.pipeline.PipelineRegistry;
import com.hca.monitor.pipeline.RunChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Turns run status changes into desktop pop-ups (every failure) and WhatsApp messages (jobs listed in
 * config/notifications.yml). Each run notifies at most once per kind (started / succeeded / failed), and only
 * runs that change after the engine starts are notified, so a restart doesn't replay the day.
 * Sending happens on its own thread: a slow provider never holds up the collectors.
 */
@Component
public class NotificationDispatcher implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(NotificationDispatcher.class);

    private final NotifyProperties props;
    private final NotificationRulesLoader rules;
    private final PipelineRegistry registry;
    private final JdbcTemplate jdbc;
    private final ZoneId zone;
    private final DateTimeFormatter clock;
    private final Instant engineStart = Instant.now();
    private final Set<String> sent = Collections.newSetFromMap(new LinkedHashMap<>() {
        @Override protected boolean removeEldestEntry(Map.Entry<String, Boolean> e) { return size() > 20_000; }
    });
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "notifications");
        t.setDaemon(true);
        return t;
    });
    private final DesktopNotifier desktop;
    private final WhatsAppSender whatsapp;

    public NotificationDispatcher(NotifyProperties props, NotificationRulesLoader rules, PipelineRegistry registry,
                                  JdbcTemplate jdbc) {
        this.props = props;
        this.rules = rules;
        this.registry = registry;
        this.jdbc = jdbc;
        this.zone = ZoneId.of(props.timeZone() == null ? "America/Los_Angeles" : props.timeZone());
        this.clock = DateTimeFormatter.ofPattern("h:mm a").withZone(zone);
        this.desktop = props.desktopEnabled() ? new DesktopNotifier() : null;
        this.whatsapp = props.whatsapp() == null ? null : new WhatsAppSender(props.whatsapp());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (whatsapp != null && whatsapp.configProblem() != null) {
            log.error("WHATSAPP_CONFIG_ERROR: {} - WhatsApp messages will fail until it's set", whatsapp.configProblem());
        } else if (whatsapp != null && whatsapp.provider() == NotifyProperties.Provider.WAHA
                && rules.rules() != null && rules.rules().whatsapp() != null && rules.rules().whatsapp().on()) {
            worker.execute(this::reportWaha);
        }
        if (desktop != null) {
            worker.execute(() -> desktop.show("Pipeline monitor", "Running. Failures will pop up here.", false, props.dashboardUrl()));
        }
        var r = rules.rules();
        if (r != null && r.whatsapp() != null && Boolean.TRUE.equals(r.whatsapp().sendTestOnStartup())) {
            worker.execute(() -> sendWhatsApp(planTest(r), "startup test"));
        }
    }

    @EventListener
    public void onRunChanged(RunChangedEvent event) {
        RunStatus run = event.run();
        NotificationRules.Kind kind = NotificationRules.Kind.of(run.status());
        if (kind == null) return;
        if (kind == NotificationRules.Kind.STARTED && !event.inserted()) return;     // only the first sighting
        Instant when = run.stoppedAt() != null ? run.stoppedAt() : run.startedAt() != null ? run.startedAt() : run.eventTime();
        if (when != null && when.isBefore(engineStart.minus(Duration.ofMinutes(5)))) return;   // history, not news
        synchronized (sent) {
            if (!sent.add(run.runId() + "|" + kind)) return;
        }
        // Step Functions failures: give the failure enricher a moment to find the failed step and real error.
        long delay = kind == NotificationRules.Kind.FAILED && "STEP_FUNCTIONS".equals(run.source()) ? 20 : 0;
        worker.schedule(() -> deliver(run, kind, 0), delay, TimeUnit.SECONDS);
    }

    private void deliver(RunStatus run, NotificationRules.Kind kind, int waitedSeconds) {
        try {
            Map<String, String> vars = vars(run, kind);
            if (kind == NotificationRules.Kind.FAILED && "STEP_FUNCTIONS".equals(run.source())
                    && vars.get("failed_step").isEmpty() && waitedSeconds < props.failureDetailWaitSeconds()) {
                worker.schedule(() -> deliver(run, kind, waitedSeconds + 15), 15, TimeUnit.SECONDS);
                return;
            }
            NotificationRules.File r = rules.rules();
            desktopNotify(run, kind, vars, r);
            sendWhatsApp(NotificationRules.plan(r, run, kind, vars), run.jobName() + " " + kind.key);
        } catch (Exception e) {
            log.error("Notification for {} {} failed: {}", run.jobName(), kind.key, e.toString());
        }
    }

    private void desktopNotify(RunStatus run, NotificationRules.Kind kind, Map<String, String> vars, NotificationRules.File r) {
        if (desktop == null || kind == NotificationRules.Kind.STARTED) return;
        NotificationRules.Desktop d = r == null ? new NotificationRules.Desktop(true, false, false) : r.desktopOrDefault();
        if (kind == NotificationRules.Kind.FAILED && !d.failures()) return;
        if (kind == NotificationRules.Kind.SUCCEEDED && !d.successes()) return;
        if (d.registeredOnly() && registry.graph().nodesFor(run).isEmpty()) return;

        boolean failed = kind == NotificationRules.Kind.FAILED;
        String title = vars.get("job") + " " + vars.get("status_word");
        String text = failed
                ? (vars.get("failed_step").isEmpty() ? "" : "Step: " + vars.get("failed_step") + "\n")
                    + (vars.get("error").isEmpty() ? vars.get("type") + " at " + vars.get("ended") : vars.get("error"))
                : vars.get("type") + " finished at " + vars.get("ended") + " (" + vars.get("duration") + ")";
        String link = !vars.get("link").isEmpty() ? vars.get("link") : props.dashboardUrl();
        desktop.show(title, text.length() > 250 ? text.substring(0, 247) + "..." : text, failed, link);
    }

    private void sendWhatsApp(List<NotificationRules.Planned> planned, String what) {
        if (planned.isEmpty() || whatsapp == null) return;
        for (NotificationRules.Planned p : planned) {
            for (int attempt = 1; attempt <= 2; attempt++) {
                try {
                    whatsapp.send(p.recipient(), p.text());
                    log.info("WHATSAPP_SENT to {} [{}] ({}): {}", p.recipientName(), p.recipient().describe(), what, oneLine(p.text()));
                    break;
                } catch (Exception e) {
                    if (attempt == 2) {
                        log.error("WHATSAPP_FAILED to {} ({}): {}", p.recipientName(), what, e.getMessage());
                    } else {
                        sleep(10_000);
                    }
                }
            }
            sleep(2_000);   // providers rate-limit; CallMeBot especially
        }
    }

    /** Startup check of the WAHA gateway, plus the group ids you can paste into notifications.yml. */
    private void reportWaha() {
        String status = whatsapp.wahaStatus();
        if (!"WORKING".equals(status)) {
            log.error("WHATSAPP_GATEWAY_NOT_READY: WAHA session is {} - open the WAHA dashboard, start the session and "
                    + "scan the QR code with the WhatsApp number (Linked devices). Messages fail until then.", status);
            return;
        }
        try {
            var groups = whatsapp.wahaGroups();
            StringBuilder b = new StringBuilder();
            groups.forEach((name, id) -> b.append("\n  ").append(name).append("  ->  chat_id: \"").append(id).append('"'));
            log.info("WhatsApp gateway ready. Groups this number is in ({}):{}", groups.size(), b);
        } catch (Exception e) {
            log.info("WhatsApp gateway ready (could not list groups: {})", e.getMessage());
        }
    }

    private List<NotificationRules.Planned> planTest(NotificationRules.File r) {
        var w = r.whatsapp();
        if (w.defaultTo() == null || w.recipients() == null) return List.of();
        return w.defaultTo().stream()
                .filter(n -> w.recipients().containsKey(n))
                .map(n -> new NotificationRules.Planned(n, w.recipients().get(n),
                        "Pipeline monitor started at " + clock.format(Instant.now()) + ". WhatsApp alerts are working."))
                .toList();
    }

    /** The {{placeholders}} for one run. */
    Map<String, String> vars(RunStatus run, NotificationRules.Kind kind) {
        Map<String, String> v = NotificationRules.emptyVars();
        boolean sfn = "STEP_FUNCTIONS".equals(run.source());
        String folder = "";
        if (!sfn && run.jobId() != null && run.jobId().contains("/")) folder = run.jobId().substring(0, run.jobId().indexOf('/'));
        v.put("job", run.jobName());
        v.put("type", sfn ? "Step function" : "Informatica workflow");
        v.put("folder", folder);
        v.put("status", run.status().name());
        v.put("status_word", switch (run.status()) {
            case RUNNING -> "started";
            case SUCCEEDED -> "succeeded";
            case FAILED -> "failed";
            case TIMED_OUT -> "timed out";
            case ABORTED -> "was aborted";
            case SUSPENDED -> "is suspended";
            default -> run.status().name().toLowerCase();
        });
        v.put("run", run.runName() == null ? "" : run.runName());
        v.put("started", run.startedAt() == null ? "" : clock.format(run.startedAt()));
        v.put("ended", run.stoppedAt() == null ? clock.format(Instant.now()) : clock.format(run.stoppedAt()));
        if (run.startedAt() != null) {
            Duration d = Duration.between(run.startedAt(), run.stoppedAt() != null ? run.stoppedAt() : Instant.now());
            long m = d.toMinutes();
            v.put("duration", m < 1 ? d.toSeconds() + "s" : m < 60 ? m + "m" : (m / 60) + "h " + (m % 60) + "m");
        }
        v.put("date", DateTimeFormatter.ofPattern("MMM d").withZone(zone).format(Instant.now()));
        v.put("dashboard", props.dashboardUrl() == null ? "" : props.dashboardUrl());
        if (sfn) {
            String region = props.awsRegion() == null ? "us-west-2" : props.awsRegion();
            v.put("link", "https://" + region + ".console.aws.amazon.com/states/home?region=" + region
                    + "#/v2/executions/details/" + URLEncoder.encode(run.runId(), StandardCharsets.UTF_8));
        }
        if (kind == NotificationRules.Kind.FAILED) {
            Map<String, String> detail = detail(run.runId());
            String step = first(detail.get("failed_state"), detail.get("failed_step"));
            v.put("failed_step", step == null ? "" : step);
            String error = first(detail.get("summary"),
                    join(run.errorCode(), run.errorCause()), join(detail.get("error_code"), detail.get("error_cause")));
            if (error != null) error = error.replaceAll("\\s+", " ").trim();
            v.put("error", error == null ? "" : error.length() > 400 ? error.substring(0, 397) + "..." : error);
        }
        return v;
    }

    private Map<String, String> detail(String runId) {
        try {
            return jdbc.query("""
                    SELECT s.failed_step, s.error_code, s.error_cause, d.summary, d.failed_state
                    FROM run_status s LEFT JOIN run_failure_detail d ON d.run_id = s.run_id
                    WHERE s.run_id = ?""",
                    rs -> {
                        Map<String, String> m = new java.util.HashMap<>();
                        if (rs.next()) {
                            m.put("failed_step", rs.getString(1));
                            m.put("error_code", rs.getString(2));
                            m.put("error_cause", rs.getString(3));
                            m.put("summary", rs.getString(4));
                            m.put("failed_state", rs.getString(5));
                        }
                        return m;
                    }, runId);
        } catch (Exception e) {
            return Map.of();
        }
    }

    private static String first(String... values) {
        for (String s : values) if (s != null && !s.isBlank()) return s;
        return null;
    }

    private static String join(String code, String cause) {
        if (code == null && cause == null) return null;
        if (cause == null || cause.isBlank()) return code;
        return code == null ? cause : code + ": " + cause;
    }

    private static String oneLine(String s) {
        String t = s.replaceAll("\\s+", " ");
        return t.length() <= 120 ? t : t.substring(0, 117) + "...";
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    @Override
    public void destroy() {
        worker.shutdownNow();
        if (desktop != null) desktop.close();
    }
}
