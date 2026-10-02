package com.hca.monitor.collector.stepfunctions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hca.monitor.collector.RunStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StepFunctionsEventMapperTest {

    private final StepFunctionsEventMapper mapper = new StepFunctionsEventMapper(new ObjectMapper());

    static String event(String status, String stopDate, String error, String cause) {
        return """
            {
              "version": "0",
              "id": "315c1398-40ff-a850-213b-158f73e60175",
              "detail-type": "Step Functions Execution Status Change",
              "source": "aws.states",
              "account": "060703039313",
              "time": "2026-09-29T19:42:21Z",
              "region": "us-west-2",
              "resources": ["arn:aws:states:us-west-2:060703039313:execution:test-machine:exec-1"],
              "detail": {
                "executionArn": "arn:aws:states:us-west-2:060703039313:execution:test-machine:exec-1",
                "stateMachineArn": "arn:aws:states:us-west-2:060703039313:stateMachine:test-machine",
                "name": "exec-1",
                "status": "%s",
                "startDate": 1759174941000,
                "stopDate": %s,
                "input": "{}",
                "inputDetails": {"included": true},
                "output": null,
                "outputDetails": null,
                "redriveCount": 0,
                "error": %s,
                "cause": %s
              }
            }""".formatted(status, stopDate, error, cause);
    }

    @Test
    void mapsRunningEvent() {
        RunStatus r = mapper.map(event("RUNNING", "null", "null", "null")).orElseThrow();

        assertThat(r.source()).isEqualTo("STEP_FUNCTIONS");
        assertThat(r.jobName()).isEqualTo("test-machine");
        assertThat(r.runName()).isEqualTo("exec-1");
        assertThat(r.status()).isEqualTo(RunStatus.Status.RUNNING);
        assertThat(r.startedAt()).isEqualTo(Instant.ofEpochMilli(1759174941000L));
        assertThat(r.stoppedAt()).isNull();
        assertThat(r.errorCode()).isNull();
        assertThat(r.eventTime()).isEqualTo(Instant.parse("2026-09-29T19:42:21Z"));
    }

    @Test
    void mapsFailedEventWithError() {
        RunStatus r = mapper.map(event("FAILED", "1759175000000",
                "\"Glue.AWSGlueException\"", "\"Job run failed\"")).orElseThrow();

        assertThat(r.status()).isEqualTo(RunStatus.Status.FAILED);
        assertThat(r.stoppedAt()).isEqualTo(Instant.ofEpochMilli(1759175000000L));
        assertThat(r.errorCode()).isEqualTo("Glue.AWSGlueException");
        assertThat(r.errorCause()).isEqualTo("Job run failed");
    }

    @Test
    void ignoresOtherEventTypes() {
        assertThat(mapper.map("{\"source\":\"aws.s3\",\"detail-type\":\"Object Created\"}")).isEmpty();
    }

    @Test
    void rejectsGarbage() {
        assertThatThrownBy(() -> mapper.map("not json"))
                .isInstanceOf(StepFunctionsEventMapper.MalformedEventException.class);
    }
}
