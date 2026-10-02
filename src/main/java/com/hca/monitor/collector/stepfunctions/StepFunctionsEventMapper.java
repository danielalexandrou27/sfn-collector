package com.hca.monitor.collector.stepfunctions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hca.monitor.collector.RunStatus;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/** Maps an EventBridge "Step Functions Execution Status Change" message body to a RunStatus. */
@Component
public class StepFunctionsEventMapper {

    static final String SOURCE = "aws.states";
    static final String DETAIL_TYPE = "Step Functions Execution Status Change";
    static final int MAX_CAUSE_CHARS = 4000;

    private final ObjectMapper objectMapper;
    private final Clock clock;

    public StepFunctionsEventMapper(ObjectMapper objectMapper) {
        this(objectMapper, Clock.systemUTC());
    }

    StepFunctionsEventMapper(ObjectMapper objectMapper, Clock clock) {
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /**
     * @return the mapped status, or empty if the message is valid JSON but not a Step Functions
     *         status event (safe to delete).
     * @throws MalformedEventException if the body cannot be parsed (left on the queue so it ends
     *         up in the DLQ for inspection).
     */
    public Optional<RunStatus> map(String body) {
        JsonNode root;
        try {
            root = objectMapper.readTree(body);
        } catch (Exception e) {
            throw new MalformedEventException("Body is not valid JSON", e);
        }

        if (!SOURCE.equals(text(root, "source")) || !DETAIL_TYPE.equals(text(root, "detail-type"))) {
            return Optional.empty();
        }

        JsonNode d = root.path("detail");
        String executionArn = text(d, "executionArn");
        String stateMachineArn = text(d, "stateMachineArn");
        String statusText = text(d, "status");
        if (executionArn == null || stateMachineArn == null || statusText == null) {
            throw new MalformedEventException("Missing executionArn, stateMachineArn or status");
        }

        RunStatus.Status status;
        try {
            status = RunStatus.Status.valueOf(statusText);
        } catch (IllegalArgumentException e) {
            throw new MalformedEventException("Unknown status: " + statusText, e);
        }

        return Optional.of(new RunStatus(
                "STEP_FUNCTIONS",
                stateMachineArn,
                stateMachineName(stateMachineArn),
                executionArn,
                text(d, "name"),
                status,
                epochMillis(d, "startDate"),
                epochMillis(d, "stopDate"),
                text(d, "error"),
                truncate(text(d, "cause")),
                d.path("redriveCount").asInt(0),
                parseInstant(text(root, "time")),
                Instant.now(clock)));
    }

    /** arn:aws:states:us-west-2:123:stateMachine:my-machine -> my-machine */
    static String stateMachineName(String arn) {
        int i = arn.lastIndexOf(':');
        return i >= 0 ? arn.substring(i + 1) : arn;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static Instant epochMillis(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull() || !v.canConvertToLong()) ? null : Instant.ofEpochMilli(v.asLong());
    }

    private static Instant parseInstant(String s) {
        try {
            return s == null ? null : Instant.parse(s);
        } catch (Exception e) {
            return null;
        }
    }

    private static String truncate(String s) {
        return (s == null || s.length() <= MAX_CAUSE_CHARS) ? s : s.substring(0, MAX_CAUSE_CHARS);
    }

    public static class MalformedEventException extends RuntimeException {
        public MalformedEventException(String message) { super(message); }
        public MalformedEventException(String message, Throwable cause) { super(message, cause); }
    }
}
