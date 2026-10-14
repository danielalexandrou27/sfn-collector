package com.hca.monitor.incident;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds the incident's field values from the templates in application.yml. {{name}}
 * placeholders are replaced ({{ }} rather than ${ }, which Spring would try to resolve).
 * Pure logic.
 */
public final class IncidentFormatter {

    private IncidentFormatter() {}

    /** ServiceNow's short_description is 160 characters. */
    static final int SHORT_DESCRIPTION_MAX = 160;
    /** Keeps the prefilled link well under URL length limits. */
    static final int DESCRIPTION_MAX = 1800;
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");

    /** All incident fields for this failure: the fixed ones from config plus short_description and description. */
    public static Map<String, String> incidentFields(IncidentCandidate c, IncidentProperties p) {
        Map<String, String> v = values(c, p);
        Map<String, String> f = new LinkedHashMap<>();
        if (p.fields() != null) f.putAll(p.fields());
        // "^" separates fields in a ServiceNow encoded query; Spark errors often contain "---^^^" pointer lines.
        f.put("short_description", truncate(oneLine(noCaret(fill(p.shortDescriptionTemplate(), v))), SHORT_DESCRIPTION_MAX));
        f.put("description", truncate(noCaret(fill(p.descriptionTemplate(), v)).strip(), DESCRIPTION_MAX));
        return f;
    }

    static Map<String, String> values(IncidentCandidate c, IncidentProperties p) {
        ZoneId zone = ZoneId.of(p.timeZone());
        boolean sfn = "STEP_FUNCTIONS".equals(c.source());
        String summary = firstNonBlank(c.detailSummary(),
                c.errorCode() != null && c.errorCause() != null ? c.errorCode() + ": " + oneLine(c.errorCause()) : null,
                c.errorCode(), c.errorCause(), "No error message was reported");
        String detail = firstNonBlank(c.detailMessage(), c.errorCause(), "(none)");

        Map<String, String> v = new LinkedHashMap<>();
        v.put("job", c.jobName());
        v.put("source_label", sfn ? "AWS Step Function" : "Informatica workflow");
        v.put("folder", sfn ? "n/a" : folderOf(c.jobId()));
        v.put("run", c.runName());
        v.put("run_id", c.runId());
        v.put("status", c.status());
        v.put("status_word", "TIMED_OUT".equals(c.status()) ? "timed out"
                : "SUSPENDED".equals(c.status()) ? "suspended" : "failed");
        v.put("started", c.startedAt() == null ? "unknown" : TIME.format(c.startedAt().atZone(zone)));
        v.put("ended", c.stoppedAt() == null ? "still open (" + c.status().toLowerCase() + ")" : TIME.format(c.stoppedAt().atZone(zone)));
        v.put("duration", duration(c.startedAt(), c.stoppedAt()));
        v.put("failed_step", firstNonBlank(c.failedStep(), "unknown"));
        v.put("attempts", c.attempts() == null ? "1" : String.valueOf(c.attempts()));
        v.put("summary", summary);
        v.put("detail", detail);
        v.put("downstream", firstNonBlank(c.downstreamImpact(), "none recorded"));
        v.put("link", sfn ? consoleLink(c.runId(), p.awsRegion()) : "Workflow Monitor: folder " + folderOf(c.jobId()) + ", run id " + c.runName());
        return v;
    }

    static String fill(String template, Map<String, String> v) {
        String out = template == null ? "" : template;
        for (var e : v.entrySet()) out = out.replace("{{" + e.getKey() + "}}", e.getValue() == null ? "" : e.getValue());
        return out;
    }

    static String consoleLink(String executionArn, String region) {
        return "https://" + region + ".console.aws.amazon.com/states/home?region=" + region
                + "#/v2/executions/details/" + URLEncoder.encode(executionArn, StandardCharsets.UTF_8);
    }

    private static String folderOf(String jobId) {
        int i = jobId == null ? -1 : jobId.indexOf('/');
        return i > 0 ? jobId.substring(0, i) : "";
    }

    private static String duration(Instant a, Instant b) {
        if (a == null || b == null) return "n/a";
        Duration d = Duration.between(a, b);
        return (d.toHours() > 0 ? d.toHours() + "h " : "") + d.toMinutesPart() + "m " + d.toSecondsPart() + "s";
    }

    private static String firstNonBlank(String... s) {
        for (String x : s) if (x != null && !x.isBlank()) return x.trim();
        return null;
    }

    static String noCaret(String s) {
        return s.replace("^", "");
    }

    static String oneLine(String s) {
        return s == null ? "" : s.replaceAll("\\s+", " ").trim();
    }

    static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 3) + "...";
    }
}
