package com.hca.monitor.collector.stepfunctions;

import com.hca.monitor.collector.RunStatus;
import com.hca.monitor.collector.RunStatusRepository;
import com.hca.monitor.pipeline.RunChangedEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Long-polls the status queue, stores each event, and deletes a message only after the
 * database write has committed. Unparseable messages are left alone so SQS moves them to
 * the DLQ after maxReceiveCount attempts.
 */
@Component
public class SqsStatusConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SqsStatusConsumer.class);

    private static final Set<String> CREDENTIAL_ERROR_CODES = Set.of(
            "ExpiredToken", "ExpiredTokenException", "UnrecognizedClientException",
            "InvalidClientTokenId", "InvalidSignatureException");

    /** Wrong queue URL / region / account, or missing permissions. Not fixed by logging in again. */
    private static final Set<String> CONFIG_ERROR_CODES = Set.of(
            "AccessDenied", "AccessDeniedException", "AWS.SimpleQueueService.NonExistentQueue",
            "QueueDoesNotExist");

    private final SqsClient sqs;
    private final SqsCollectorProperties props;
    private final StepFunctionsEventMapper mapper;
    private final RunStatusRepository repository;

    private volatile boolean running;
    private volatile Thread worker;

    // Simple health state; later exposed to the detector as "collector unhealthy".
    private volatile Instant lastSuccessfulPoll;
    private volatile String lastError;
    private boolean inCredentialFailure;

    private final ApplicationEventPublisher events;

    public SqsStatusConsumer(SqsClient sqs, SqsCollectorProperties props, StepFunctionsEventMapper mapper,
                             RunStatusRepository repository, ApplicationEventPublisher events) {
        this.sqs = sqs;
        this.props = props;
        this.mapper = mapper;
        this.repository = repository;
        this.events = events;
    }

    @Override
    public void start() {
        if (!props.enabled()) {
            log.info("SQS status consumer disabled (monitor.sqs.enabled=false)");
            return;
        }
        running = true;
        // Non-daemon platform thread: keeps the JVM alive (there is no web server holding it open).
        worker = Thread.ofPlatform().name("sqs-status-consumer").daemon(false).start(this::loop);
        log.info("SQS status consumer started on {}", props.queueUrl());
    }

    @Override
    public void stop() {
        running = false;
        Thread t = worker;
        if (t != null) {
            t.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        while (running) {
            try {
                pollOnce();
                markHealthy();
            } catch (SdkClientException e) {
                if (!running) break;
                handleFailure("AWS client error (credentials or network): " + e.getMessage(), true);
            } catch (AwsServiceException e) {
                String code = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "unknown";
                if (CONFIG_ERROR_CODES.contains(code)) {
                    handleConfigError("SQS error " + code + ": " + e.getMessage());
                } else {
                    handleFailure("SQS error " + code + ": " + e.getMessage(),
                            CREDENTIAL_ERROR_CODES.contains(code));
                }
            } catch (Exception e) {
                if (!running) break;
                handleFailure("Unexpected error: " + e, false);
            }
        }
        log.info("SQS status consumer stopped");
    }

    /** One receive / process / delete cycle. Package-private for tests. */
    int pollOnce() {
        List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(props.queueUrl())
                .waitTimeSeconds(props.waitTimeSeconds())
                .maxNumberOfMessages(props.maxMessages())
                .visibilityTimeout(props.visibilityTimeoutSeconds())
                .build()).messages();

        if (messages.isEmpty()) {
            return 0;
        }

        List<DeleteMessageBatchRequestEntry> toDelete = new ArrayList<>();
        for (Message m : messages) {
            if (process(m)) {
                toDelete.add(DeleteMessageBatchRequestEntry.builder()
                        .id(m.messageId())
                        .receiptHandle(m.receiptHandle())
                        .build());
            }
        }

        if (!toDelete.isEmpty()) {
            var result = sqs.deleteMessageBatch(b -> b.queueUrl(props.queueUrl()).entries(toDelete));
            result.failed().forEach(f ->
                    log.warn("Delete failed for message {}: {} {}", f.id(), f.code(), f.message()));
        }
        return toDelete.size();
    }

    /** @return true if the message is handled and should be deleted. */
    private boolean process(Message m) {
        Optional<RunStatus> mapped;
        try {
            mapped = mapper.map(m.body());
        } catch (StepFunctionsEventMapper.MalformedEventException e) {
            log.error("Malformed message {} left for DLQ: {}", m.messageId(), e.getMessage());
            return false;
        }

        if (mapped.isEmpty()) {
            log.debug("Ignoring non-Step-Functions message {}", m.messageId());
            return true;
        }

        RunStatus r = mapped.get();
        try {
            RunStatusRepository.Outcome outcome = repository.upsert(r);
            log.info("{} {} {} -> {}{}", outcome, r.jobName(), r.runName(), r.status(),
                    r.errorCode() != null ? " (" + r.errorCode() + ")" : "");
            if (outcome == RunStatusRepository.Outcome.INSERTED || outcome == RunStatusRepository.Outcome.UPDATED) {
                events.publishEvent(new RunChangedEvent(r, outcome == RunStatusRepository.Outcome.INSERTED));
            }
            return true;
        } catch (Exception e) {
            // DB problem: keep the message; it becomes visible again and is retried.
            log.error("Failed to store run {}; message will be retried", r.runId(), e);
            return false;
        }
    }

    private void markHealthy() {
        lastSuccessfulPoll = Instant.now();
        if (inCredentialFailure) {
            log.info("COLLECTOR_RECOVERED: SQS polling working again");
            inCredentialFailure = false;
        }
        lastError = null;
    }

    private void handleConfigError(String message) {
        if (!message.equals(lastError)) {
            log.error("COLLECTOR_CONFIG_ERROR: {}. Check monitor.sqs.queue-url, AWS_REGION, and that the "
                    + "credentials belong to the queue's account and allow sqs:ReceiveMessage. Retrying every {}s.",
                    message, props.credentialErrorBackoffSeconds());
        }
        lastError = message;
        inCredentialFailure = true; // so recovery is logged
        sleep(Duration.ofSeconds(props.credentialErrorBackoffSeconds()));
    }

    private void handleFailure(String message, boolean credentialProblem) {
        lastError = message;
        if (credentialProblem) {
            if (!inCredentialFailure) {
                log.error("COLLECTOR_CREDENTIALS_FAILED: {}. Refresh the keys in %USERPROFILE%\\.aws\\credentials "
                        + "from the AWS access portal (Access keys -> Option 2), then restart if this repeats. Retrying every {}s.",
                        message, props.credentialErrorBackoffSeconds());
                inCredentialFailure = true;
            }
            sleep(Duration.ofSeconds(props.credentialErrorBackoffSeconds()));
        } else {
            log.error("COLLECTOR_ERROR: {}", message);
            sleep(Duration.ofSeconds(5));
        }
    }

    private void sleep(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Instant lastSuccessfulPoll() { return lastSuccessfulPoll; }
    public String lastError() { return lastError; }
}
