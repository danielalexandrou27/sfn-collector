package com.hca.monitor.dashboard;

import com.hca.monitor.pipeline.PipelineConfig;
import com.hca.monitor.pipeline.PipelineGraph;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DashboardBuilderTest {

    static final ZoneId PT = ZoneId.of("America/Los_Angeles");
    static final LocalDate DAY = LocalDate.of(2026, 10, 14);
    static final DashboardBuilder.Links LINKS = new DashboardBuilder.Links("https://hcadev.service-now.com", "us-west-2");

    static Instant at(int h, int m) {
        return LocalDateTime.of(DAY, java.time.LocalTime.of(h, m)).atZone(PT).toInstant();
    }

    static PipelineGraph graph() {
        return PipelineGraph.build(new PipelineConfig.File(List.of(
                new PipelineConfig.Pipeline("auto_loans_daily", List.of(
                        new PipelineConfig.Node("extract", new PipelineConfig.InformaticaRef("LOANS", "wf_LOANS_EXTRACT"), null, null, null, null),
                        new PipelineConfig.Node("dims", new PipelineConfig.InformaticaRef("LOANS", "wf_LOANS_DIMS"), null, null, null, null),
                        new PipelineConfig.Node("load", null, "hca-loans-load", List.of("extract", "dims"), 30, "04:30"))),
                new PipelineConfig.Pipeline("gl_daily", List.of(
                        new PipelineConfig.Node("gl", null, "hca-gl-load", null, null, "06:00"))),
                new PipelineConfig.Pipeline("dealer_daily", List.of(
                        new PipelineConfig.Node("dealer", null, "hca-dealer-load", null, null, "23:00"))))));
    }

    static RunView sfn(String name, String run, String status, Instant start, Instant stop, String summary) {
        return new RunView("arn:aws:states:us-west-2:1:execution:" + name + ":" + run, "STEP_FUNCTIONS",
                "arn:aws:states:us-west-2:1:stateMachine:" + name, name, run, status, start, stop,
                summary != null ? "States.TaskFailed" : null, null, summary != null ? "Load Loans" : null, summary,
                summary != null ? "INC0012345" : null, summary != null ? "abc123" : null);
    }

    static RunView wf(String folder, String wf, long id, String status, Instant start, Instant stop) {
        return new RunView("infa:" + folder + "/" + wf + "/" + id, "INFORMATICA", folder + "/" + wf, wf, String.valueOf(id),
                status, start, stop, null, null, null, null, null, null);
    }

    @Test
    void oneRowPerLinkWithLatestRunAndCounts() {
        var runs = List.of(
                wf("LOANS", "wf_LOANS_EXTRACT", 1, "SUCCEEDED", at(1, 0), at(1, 40)),
                wf("LOANS", "wf_LOANS_DIMS", 2, "SUCCEEDED", at(1, 5), at(1, 20)),
                sfn("hca-loans-load", "exec-1", "FAILED", at(1, 45), at(1, 52), "glue hca_loans_load_job: AnalysisException: Table not found"),
                sfn("hca-loans-load", "exec-2", "RUNNING", at(3, 0), null, null),
                sfn("hca-misc-job", "exec-9", "SUCCEEDED", at(2, 0), at(2, 3), null));

        var d = DashboardBuilder.build(graph(), runs, DAY, PT, at(7, 0), LINKS);

        // hca-loans-load: 2 rows (one per upstream workflow), latest run RUNNING, 2 runs today
        var load = d.rows().stream().filter(r -> r.stepFunction().name().equals("hca-loans-load")).toList();
        assertThat(load).hasSize(2);
        assertThat(load).extracting(r -> r.upstream().name()).containsExactlyInAnyOrder("wf_LOANS_EXTRACT", "wf_LOANS_DIMS");
        assertThat(load.get(0).stepFunction().status()).isEqualTo("RUNNING");
        assertThat(load.get(0).stepFunction().runCount()).isEqualTo(2);
        assertThat(load.get(0).stepFunction().durationSeconds()).isEqualTo(4 * 3600L);   // elapsed while running
        assertThat(load.get(0).upstream().folder()).isEqualTo("LOANS");

        // hca-gl-load: never ran, expected by 06:00, now 07:00 -> LATE; hca-dealer-load expected 23:00 -> NOT_RUN
        var gl = d.rows().stream().filter(r -> r.stepFunction().name().equals("hca-gl-load")).findFirst().orElseThrow();
        assertThat(gl.stepFunction().status()).isEqualTo("LATE");
        assertThat(gl.upstream()).isNull();
        var dealer = d.rows().stream().filter(r -> r.stepFunction().name().equals("hca-dealer-load")).findFirst().orElseThrow();
        assertThat(dealer.stepFunction().status()).isEqualTo("NOT_RUN");

        // Not in the registry
        assertThat(d.unregistered()).extracting(r -> r.stepFunction().name()).containsExactly("hca-misc-job");

        // Counts per step function, not per row
        assertThat(d.counts().total()).isEqualTo(3);
        assertThat(d.counts().running()).isEqualTo(1);
        assertThat(d.counts().late()).isEqualTo(1);
        assertThat(d.counts().notRun()).isEqualTo(1);
        assertThat(d.counts().unregistered()).isEqualTo(1);

        // LATE sorts before RUNNING before NOT_RUN
        assertThat(d.rows().get(0).stepFunction().name()).isEqualTo("hca-gl-load");
    }

    @Test
    void failedRunCarriesErrorIncidentAndConsoleLink() {
        var runs = List.of(sfn("hca-loans-load", "exec-1", "FAILED", at(1, 45), at(1, 52),
                "glue hca_loans_load_job: AnalysisException: Table not found"));
        var d = DashboardBuilder.build(graph(), runs, DAY, PT, at(7, 0), LINKS);
        var sf = d.rows().stream().filter(r -> r.stepFunction().name().equals("hca-loans-load")).findFirst().orElseThrow().stepFunction();
        assertThat(sf.status()).isEqualTo("FAILED");
        assertThat(sf.error()).isEqualTo("glue hca_loans_load_job: AnalysisException: Table not found");
        assertThat(sf.failedStep()).isEqualTo("Load Loans");
        assertThat(sf.incident().number()).isEqualTo("INC0012345");
        assertThat(sf.incident().url()).isEqualTo("https://hcadev.service-now.com/incident.do?sys_id=abc123");
        assertThat(sf.consoleUrl()).startsWith("https://us-west-2.console.aws.amazon.com/states/home");
        assertThat(sf.durationSeconds()).isEqualTo(7 * 60L);
        assertThat(d.counts().failed()).isEqualTo(1);
    }

    @Test
    void stepFunctionUpstreamFromAnotherPipelineGetsItsOwnRow() {
        var g = PipelineGraph.build(new PipelineConfig.File(List.of(
                new PipelineConfig.Pipeline("HCA_EDW_DAYBREAK", List.of(
                        new PipelineConfig.Node("prod-Application", null, "prod-Application", null, null, null))),
                new PipelineConfig.Pipeline("ADW_KSIOP", List.of(
                        new PipelineConfig.Node("prod-ksiop_report", null, "prod-ksiop_report",
                                List.of("prod-Application"), null, "05:00"))))));
        var runs = List.of(
                sfn("prod-Application", "a1", "FAILED", at(2, 0), at(2, 10), "glue app_job: boom"));

        var d = DashboardBuilder.build(g, runs, DAY, PT, at(6, 0), LINKS);

        var row = d.rows().stream().filter(r -> r.stepFunction().name().equals("prod-ksiop_report")).findFirst().orElseThrow();
        assertThat(row.pipeline()).isEqualTo("ADW_KSIOP");
        assertThat(row.upstream().type()).isEqualTo("STEP_FUNCTIONS");
        assertThat(row.upstream().name()).isEqualTo("prod-Application");
        assertThat(row.upstream().folder()).isNull();
        assertThat(row.upstream().status()).isEqualTo("FAILED");
        assertThat(row.upstream().consoleUrl()).contains("prod-Application");
        assertThat(row.stepFunction().status()).isEqualTo("LATE");            // waited on, past 05:00
        // Both step functions are counted once each.
        assertThat(d.counts().total()).isEqualTo(2);
        assertThat(d.counts().failed()).isEqualTo(1);
        assertThat(d.counts().late()).isEqualTo(1);
    }
}
