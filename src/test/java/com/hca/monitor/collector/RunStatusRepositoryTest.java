package com.hca.monitor.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hca.monitor.collector.stepfunctions.StepFunctionsEventMapper;
import com.hca.monitor.collector.stepfunctions.StepFunctionsEventMapperTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "monitor.sqs.enabled=false",
        "spring.datasource.url=jdbc:h2:mem:test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
})
class RunStatusRepositoryTest {

    @Autowired RunStatusRepository repository;
    @Autowired org.springframework.jdbc.core.JdbcTemplate jdbc;

    @org.junit.jupiter.api.BeforeEach
    void clean() {
        jdbc.update("DELETE FROM run_status");
    }

    private final StepFunctionsEventMapper mapper = new StepFunctionsEventMapper(new ObjectMapper());

    private RunStatus ev(String status, String stop) {
        return mapper.map(StepFunctionsEventMapperTest.event(status, stop, "null", "null")).orElseThrow();
    }

    @Test
    void terminalStatusIsNotOverwrittenByLateRunningEvent() {
        assertThat(repository.upsert(ev("SUCCEEDED", "1759175000000")))
                .isIn(RunStatusRepository.Outcome.INSERTED, RunStatusRepository.Outcome.UPDATED);

        // RUNNING arrives late (EventBridge does not guarantee order)
        assertThat(repository.upsert(ev("RUNNING", "null")))
                .isEqualTo(RunStatusRepository.Outcome.IGNORED_STALE);

        assertThat(repository.findStatus("arn:aws:states:us-west-2:060703039313:execution:test-machine:exec-1"))
                .contains("SUCCEEDED");
    }

    @Test
    void repeatedIdenticalRowIsUnchanged() {
        repository.upsert(ev("RUNNING", "null"));
        assertThat(repository.upsert(ev("RUNNING", "null")))
                .isEqualTo(RunStatusRepository.Outcome.UNCHANGED);
    }

    @Test
    void redriveMovesFailedRunForward() {
        RunStatus failed = ev("FAILED", "1759175000000");
        repository.upsert(failed);

        RunStatus redriven = new RunStatus(failed.source(), failed.jobId(), failed.jobName(), failed.runId(),
                failed.runName(), RunStatus.Status.RUNNING, failed.startedAt(), null, null, null,
                1, failed.eventTime(), failed.observedAt());

        assertThat(repository.upsert(redriven)).isEqualTo(RunStatusRepository.Outcome.UPDATED);
        assertThat(repository.findStatus(failed.runId())).contains("RUNNING");
    }
}
