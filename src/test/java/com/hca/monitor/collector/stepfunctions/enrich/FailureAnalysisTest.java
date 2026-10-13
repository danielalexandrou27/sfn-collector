package com.hca.monitor.collector.stepfunctions.enrich;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FailureAnalysisTest {

    static final String GLUE_CAUSE = """
            {"AllocatedCapacity":10,"Attempt":0,"ErrorMessage":"AnalysisException: Table or view not found: stg.loans_daily; line 1 pos 14",
             "Id":"jr_5f2b1c","JobName":"hca_loans_load_job","JobRunState":"FAILED","ExecutionTime":95}""";

    /** Glue state retried twice (3 failures), caught, routed to a notify task, then a Fail state. */
    static List<HistoryItem> glueFailure() {
        List<HistoryItem> h = new ArrayList<>();
        h.add(new HistoryItem(1, 0, "ExecutionStarted", null, null, null, null, null));
        h.add(new HistoryItem(2, 1, "TaskStateEntered", "Load Loans", null, null, null, null));
        long prev = 2;
        long id = 3;
        for (int attempt = 0; attempt < 3; attempt++) {
            h.add(new HistoryItem(id, prev, "TaskScheduled", null, "glue", "startJobRun.sync", null, null)); prev = id++;
            h.add(new HistoryItem(id, prev, "TaskStarted", null, "glue", "startJobRun.sync", null, null)); prev = id++;
            h.add(new HistoryItem(id, prev, "TaskFailed", null, "glue", "startJobRun.sync", "States.TaskFailed", GLUE_CAUSE)); prev = id++;
        }
        h.add(new HistoryItem(id, prev, "TaskStateExited", "Load Loans", null, null, null, null)); prev = id++;
        h.add(new HistoryItem(id, prev, "TaskStateEntered", "Notify Failure", null, null, null, null)); prev = id++;
        h.add(new HistoryItem(id, prev, "TaskSucceeded", null, "sns", "publish", null, null)); prev = id++;
        h.add(new HistoryItem(id, prev, "FailStateEntered", "Fail", null, null, null, null)); prev = id++;
        h.add(new HistoryItem(id, prev, "ExecutionFailed", null, null, null, "LoadFailed", "Loans load failed, see Load Loans"));
        Collections.reverse(h);
        return h;
    }

    @Test
    void findsGlueStateBehindCatchAndFailState() {
        var a = FailureAnalyzer.analyze(glueFailure()).orElseThrow();
        assertThat(a.failedState()).isEqualTo("Load Loans");
        assertThat(a.failureEvent()).isEqualTo("TaskFailed");
        assertThat(a.resourceType()).isEqualTo("glue");
        assertThat(a.attempts()).isEqualTo(3);
        assertThat(a.executionError()).isEqualTo("LoadFailed");

        var p = CauseParser.parse(a.resourceType(), a.cause());
        assertThat(p.serviceJob()).isEqualTo("hca_loans_load_job");
        assertThat(p.serviceRunId()).isEqualTo("jr_5f2b1c");
        assertThat(p.summary(a.error()))
                .isEqualTo("glue hca_loans_load_job: AnalysisException: Table or view not found: stg.loans_daily; line 1 pos 14");
    }

    @Test
    void ignoresTaskThatRecoveredOnRetry() {
        List<HistoryItem> h = new ArrayList<>(List.of(
                new HistoryItem(1, 0, "ExecutionStarted", null, null, null, null, null),
                new HistoryItem(2, 1, "TaskStateEntered", "Get Batch Date", null, null, null, null),
                new HistoryItem(3, 2, "TaskFailed", null, "lambda", "invoke", "Lambda.ServiceException", "{\"errorMessage\":\"throttled\"}"),
                new HistoryItem(4, 3, "TaskSucceeded", null, "lambda", "invoke", null, null),
                new HistoryItem(5, 4, "TaskStateExited", "Get Batch Date", null, null, null, null),
                new HistoryItem(6, 5, "ChoiceStateEntered", "Has Data?", null, null, null, null),
                new HistoryItem(7, 6, "ExecutionFailed", null, null, null, "States.Runtime",
                        "An error occurred while executing the state 'Has Data?'. Invalid path '$.rows'")));
        Collections.reverse(h);

        var a = FailureAnalyzer.analyze(h).orElseThrow();
        assertThat(a.failureEvent()).isEqualTo("ExecutionFailed");
        assertThat(a.failedState()).isEqualTo("Has Data?");
        assertThat(CauseParser.parse(a.resourceType(), a.cause()).summary(a.error()))
                .startsWith("An error occurred while executing the state 'Has Data?'");
    }

    @Test
    void parsesLambdaCause() {
        var p = CauseParser.parse("lambda", """
                {"errorType":"KeyError","errorMessage":"'batch_date'","requestId":"abc-123","stackTrace":["  File ..."]}""");
        assertThat(p.errorType()).isEqualTo("KeyError");
        assertThat(p.serviceRunId()).isEqualTo("abc-123");
        assertThat(p.summary(null)).isEqualTo("lambda: KeyError: 'batch_date'");
    }

    @Test
    void plainTextCauseIsKept() {
        var p = CauseParser.parse("ecs", "Essential container in task exited");
        assertThat(p.summary("States.TaskFailed")).isEqualTo("ecs: Essential container in task exited");
    }
}
