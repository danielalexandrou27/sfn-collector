package com.hca.monitor.collector.stepfunctions.enrich;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Pulls a readable message out of a task's "cause", which services fill differently:
 *   Glue   (glue:startJobRun.sync)  {"JobName":..,"Id":"jr_..","JobRunState":"FAILED","ErrorMessage":..}
 *   Lambda (lambda:invoke)          {"errorType":..,"errorMessage":..,"stackTrace":[..]}
 *   Others (ECS, Batch, EMR, SDK)   a JSON object with one of the message fields below, or plain text
 * Pure logic.
 */
public final class CauseParser {

    private CauseParser() {}

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String[] MESSAGE_FIELDS = {
            "ErrorMessage", "errorMessage", "Message", "message", "StatusReason", "StoppedReason",
            "StateChangeReason", "Cause", "cause"};

    public record Parsed(
            String service,       // glue, lambda, ecs, batch, ... (from the task's resourceType)
            String serviceJob,    // Glue job name / Lambda function, when the cause carries it
            String serviceRunId,  // Glue job run id, Lambda request id
            String errorType,     // Lambda errorType, exception class if present
            String message) {     // the readable error

        /** One line for logs, tickets and the LLM, e.g. "glue hca_loans_job: AnalysisException: Table not found". */
        public String summary(String fallbackError) {
            StringBuilder s = new StringBuilder();
            if (service != null) s.append(service);
            if (serviceJob != null) s.append(s.isEmpty() ? "" : " ").append(serviceJob);
            if (!s.isEmpty()) s.append(": ");
            if (errorType != null && (message == null || !message.startsWith(errorType))) s.append(errorType).append(": ");
            s.append(message != null ? message : fallbackError != null ? fallbackError : "unknown error");
            return oneLine(s.toString(), 500);
        }
    }

    public static Parsed parse(String resourceType, String cause) {
        String service = resourceType;
        if (cause == null || cause.isBlank()) return new Parsed(service, null, null, null, null);

        JsonNode node = null;
        String trimmed = cause.trim();
        if (trimmed.startsWith("{")) {
            try { node = JSON.readTree(trimmed); } catch (Exception ignored) { }
        }
        if (node == null || !node.isObject()) {
            return new Parsed(service, null, null, null, oneLine(trimmed, 4000));
        }

        String job = null, runId = null, type = null;
        if ("glue".equals(service)) {
            job = text(node, "JobName");
            runId = text(node, "Id");
        } else if ("lambda".equals(service)) {
            type = text(node, "errorType");
            runId = text(node, "requestId");
        }
        if (type == null) type = text(node, "errorType");

        String msg = null;
        for (String f : MESSAGE_FIELDS) {
            msg = text(node, f);
            if (msg != null) break;
        }
        // Some SDK integrations nest the real error one level down, e.g. {"Error":{"Message":..}}
        if (msg == null && node.has("Error") && node.get("Error").isObject()) {
            msg = text(node.get("Error"), "Message");
        }
        return new Parsed(service, job, runId, type, msg == null ? oneLine(trimmed, 4000) : oneLine(msg, 4000));
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return null;
        String s = v.isTextual() ? v.asText() : v.toString();
        return s.isBlank() ? null : s.trim();
    }

    static String oneLine(String s, int max) {
        String t = s.replaceAll("\\s+", " ").trim();
        return t.length() <= max ? t : t.substring(0, max - 3) + "...";
    }
}
