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
        return new PipelineConfig.Node(id, new PipelineConfig.InformaticaRef(folder, wf), null, null, null);
    }

    static PipelineConfig.Node sfn(String id, String name, Integer lag, String... deps) {
        return new PipelineConfig.Node(id, null, name, List.of(deps), lag);
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
                new PipelineConfig.Node("d", null, null, null, null)))));
        assertThatThrownBy(() -> PipelineGraph.build(bad))
                .hasMessageContaining("cycle")
                .hasMessageContaining("unknown node 'missing'")
                .hasMessageContaining("exactly one of");
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
