package com.hca.monitor.pipeline;

import com.hca.monitor.collector.RunStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineLogicTest {

    static PipelineConfig.Node infa(String id, String folder, String wf) {
        return new PipelineConfig.Node(id, new PipelineConfig.InformaticaRef(folder, wf), null, null, null, null);
    }

    static PipelineConfig.Node sfn(String id, String name, Integer lag, String... deps) {
        return new PipelineConfig.Node(id, null, name, List.of(deps), lag, null);
    }

    static PipelineGraph sampleGraph() {
        return PipelineGraph.build(new PipelineConfig.File(List.of(new PipelineConfig.Pipeline("loans", List.of(
                infa("extract", "LOANS", "wf_EXTRACT"),
                sfn("load", "hca-loans-load", 30, "extract"),
                sfn("agg", "hca-loans-agg", 15, "load"))))));
    }

    @Test
    void graphMapsRunsToNodesAndFindsDownstream() {
        PipelineGraph g = sampleGraph();
        RunStatus wf = new RunStatus("INFORMATICA", "LOANS/wf_EXTRACT", "wf_EXTRACT", "infa:1", "1",
                RunStatus.Status.FAILED, Instant.now(), Instant.now(), null, null, 0, null, Instant.now());
        var nodes = g.nodesFor(wf);
        assertThat(nodes).extracting(PipelineGraph.NodeRef::nodeId).containsExactly("extract");
        assertThat(g.allDownstream(nodes.get(0))).extracting(PipelineGraph.NodeRef::nodeId)
                .containsExactly("load", "agg");

        RunStatus sm = new RunStatus("STEP_FUNCTIONS", "arn:aws:states:us-west-2:1:stateMachine:hca-loans-load",
                "hca-loans-load", "arn:x", "x", RunStatus.Status.RUNNING, Instant.now(), null, null, null, 0, null, Instant.now());
        assertThat(g.nodesFor(sm)).extracting(PipelineGraph.NodeRef::nodeId).containsExactly("load");
    }

    @Test
    void graphRejectsBadConfigWithAllProblemsListed() {
        var bad = new PipelineConfig.File(List.of(new PipelineConfig.Pipeline("p", List.of(
                sfn("a", "sm-a", null, "b"),
                sfn("b", "sm-b", null, "a"),
                sfn("c", "sm-c", null, "missing"),
                new PipelineConfig.Node("d", null, null, null, null, null)))));
        assertThatThrownBy(() -> PipelineGraph.build(bad))
                .hasMessageContaining("cycle")
                .hasMessageContaining("depends_on 'missing' doesn't match any node")
                .hasMessageContaining("exactly one of");
    }

    /** Folder-per-pipeline layout: a step function waits on a step function registered in another pipeline. */
    static PipelineGraph crossPipelineGraph() {
        return PipelineGraph.build(new PipelineConfig.File(List.of(
                new PipelineConfig.Pipeline("HCA_EDW_DAYBREAK", List.of(
                        infa("wf_APPLICATION", "HCA_EDW_DAYBREAK", "wf_APPLICATION"),
                        sfn("prod-Application", "prod-Application", null, "wf_APPLICATION"))),
                new PipelineConfig.Pipeline("ADW_KSIOP", List.of(
                        infa("wf_POOL", "ADW_KSIOP", "wf_POOL"),
                        sfn("prod-ksiopcommon", "prod-ksiopcommon", null, "wf_POOL"),
                        // by bare step function name, by pipeline/id, and two upstreams of different kinds
                        sfn("prod-ksiop_report", "prod-ksiop_report", 20, "prod-Application"),
                        sfn("prod-ksiop_rollup", "prod-ksiop_rollup", null, "HCA_EDW_DAYBREAK/prod-Application", "prod-ksiopcommon"))))));
    }

    @Test
    void stepFunctionCanDependOnStepFunctionInAnotherPipeline() {
        PipelineGraph g = crossPipelineGraph();
        var report = g.node("ADW_KSIOP", "prod-ksiop_report").orElseThrow();
        assertThat(g.upstreamOf(report)).singleElement().satisfies(e -> {
            assertThat(e.upstream().pipeline()).isEqualTo("HCA_EDW_DAYBREAK");
            assertThat(e.upstream().source()).isEqualTo(PipelineGraph.SFN);
            assertThat(e.upstream().matchValue()).isEqualTo("prod-Application");
            assertThat(e.maxStartLag()).isEqualTo(Duration.ofMinutes(20));
        });
        var rollup = g.node("ADW_KSIOP", "prod-ksiop_rollup").orElseThrow();
        assertThat(g.upstreamOf(rollup)).extracting(e -> e.upstream().matchValue())
                .containsExactly("prod-Application", "prod-ksiopcommon");

        // A failure in the workflow reaches everything downstream, across pipelines.
        var wfApp = g.node("HCA_EDW_DAYBREAK", "wf_APPLICATION").orElseThrow();
        assertThat(g.allDownstream(wfApp)).extracting(PipelineGraph.NodeRef::nodeId)
                .containsExactlyInAnyOrder("prod-Application", "prod-ksiop_report", "prod-ksiop_rollup");

        // The upstream step function's own runs are still matched to its single node.
        RunStatus app = new RunStatus("STEP_FUNCTIONS", "arn:aws:states:us-west-2:1:stateMachine:prod-Application",
                "prod-Application", "arn:y", "y", RunStatus.Status.SUCCEEDED, Instant.now(), Instant.now(), null, null, 0, null, Instant.now());
        assertThat(g.nodesFor(app)).extracting(PipelineGraph.NodeRef::pipeline).containsExactly("HCA_EDW_DAYBREAK");
    }

    @Test
    void crossPipelineNamesMustBeUnambiguousAndAcyclic() {
        var ambiguous = new PipelineConfig.File(List.of(
                new PipelineConfig.Pipeline("a", List.of(sfn("shared", "sm-a", null))),
                new PipelineConfig.Pipeline("b", List.of(sfn("shared", "sm-b", null))),
                new PipelineConfig.Pipeline("c", List.of(sfn("down", "sm-c", null, "shared")))));
        assertThatThrownBy(() -> PipelineGraph.build(ambiguous))
                .hasMessageContaining("matches more than one node (a/shared, b/shared)")
                .hasMessageContaining("pipeline/node_id");

        var loop = new PipelineConfig.File(List.of(
                new PipelineConfig.Pipeline("a", List.of(sfn("x", "sm-x", null, "b/y"))),
                new PipelineConfig.Pipeline("b", List.of(sfn("y", "sm-y", null, "sm-x")))));
        assertThatThrownBy(() -> PipelineGraph.build(loop)).hasMessageContaining("dependency cycle");
    }

    @Test
    void classifierCoversEachCase() {
        Instant start = Instant.parse("2026-10-09T10:00:00Z");
        var ok = LinkClassifier.classify(new LinkClassifier.Run("u", "SUCCEEDED",
                start.minusSeconds(3600), start.minusSeconds(300)), start);
        assertThat(ok.state()).isEqualTo(LinkClassifier.State.OK);
        assertThat(ok.lag()).isEqualTo(Duration.ofMinutes(5));

        assertThat(LinkClassifier.classify(new LinkClassifier.Run("u", "RUNNING", start.minusSeconds(60), null), start).state())
                .isEqualTo(LinkClassifier.State.UPSTREAM_STILL_RUNNING);
        assertThat(LinkClassifier.classify(new LinkClassifier.Run("u", "SUCCEEDED", start.minusSeconds(60),
                start.plusSeconds(60)), start).state()).isEqualTo(LinkClassifier.State.UPSTREAM_STILL_RUNNING);
        assertThat(LinkClassifier.classify(new LinkClassifier.Run("u", "FAILED", start.minusSeconds(60),
                start.minusSeconds(30)), start).state()).isEqualTo(LinkClassifier.State.UPSTREAM_FAILED);
        assertThat(LinkClassifier.classify(null, start).state()).isEqualTo(LinkClassifier.State.NO_UPSTREAM_RUN);
    }

}
