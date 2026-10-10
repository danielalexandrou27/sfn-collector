package com.hca.monitor.pipeline;

import com.hca.monitor.collector.RunStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

    @Test
    void discoveryFindsDailyUpstreamAndIgnoresHourlyNoise() {
        ZoneId pt = ZoneId.of("America/Los_Angeles");
        List<EdgeDiscovery.Run> runs = new ArrayList<>();
        Instant day0 = Instant.parse("2026-09-01T09:00:00Z"); // 02:00 PDT
        for (int d = 0; d < 10; d++) {
            Instant base = day0.plus(Duration.ofDays(d));
            // Daily workflow ends at 02:30, state machine starts at 02:35
            runs.add(new EdgeDiscovery.Run("INFORMATICA", "LOANS/wf_EXTRACT", "SUCCEEDED", base, base.plusSeconds(1800)));
            runs.add(new EdgeDiscovery.Run("STEP_FUNCTIONS", "hca-loans-load", "SUCCEEDED", base.plusSeconds(2100), base.plusSeconds(2400)));
            // Hourly workflow: finishes before the state machine every day, but only 1 of 24 runs is followed
            for (int h = 0; h < 24; h++) {
                Instant hs = base.minusSeconds(7200).plus(Duration.ofHours(h));
                runs.add(new EdgeDiscovery.Run("INFORMATICA", "OPS/wf_HOURLY", "SUCCEEDED", hs, hs.plusSeconds(600)));
            }
            // Unrelated state machine at 14:00 PDT with nothing before it
            runs.add(new EdgeDiscovery.Run("STEP_FUNCTIONS", "hca-other", "SUCCEEDED", base.plusSeconds(43200), null));
        }
        var result = EdgeDiscovery.discover(runs, EdgeDiscovery.Settings.defaults(pt), Set.of());

        assertThat(result.byDownstream()).containsOnlyKeys("hca-loans-load");
        var c = result.byDownstream().get("hca-loans-load").get(0);
        assertThat(c.upstream()).isEqualTo("LOANS/wf_EXTRACT");
        assertThat(c.daysTogether()).isEqualTo(10);
        assertThat(c.medianLag()).isEqualTo(Duration.ofMinutes(5));
        assertThat(result.downstreamWithoutCandidate()).contains("hca-other");

        String yaml = EdgeCandidatesWriter.render(result, EdgeDiscovery.Settings.defaults(pt), 30, Instant.now());
        assertThat(yaml).contains("step_function: \"hca-loans-load\"")
                .contains("depends_on: [wf__LOANS_wf_EXTRACT]")
                .doesNotContain("wf_HOURLY\" }");
    }
}
