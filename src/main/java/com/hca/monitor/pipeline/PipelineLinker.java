package com.hca.monitor.pipeline;

import com.hca.monitor.collector.RunStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Applies the pipeline registry to runs as they arrive:
 *  - when a downstream run starts: was each upstream finished and successful? (run_link)
 *  - when an upstream run fails: which downstream nodes are now blocked? (UPSTREAM_FAILED alert)
 *  - every 5 min: did a downstream fail to start after its upstream succeeded? (MISSING_TRIGGER alert)
 *
 * Alerts are logged and stored in pipeline_alert; the detector/LLM layer picks them up later.
 */
@Component
public class PipelineLinker {

    private static final Logger log = LoggerFactory.getLogger(PipelineLinker.class);

    private final PipelineRegistry registry;
    private final PipelineStore store;
    private final Duration linkWindow;
    private final Duration pendingGrace;

    public PipelineLinker(PipelineRegistry registry, PipelineStore store,
                          @Value("${monitor.pipelines.link-window-hours:36}") long linkWindowHours,
                          @Value("${monitor.pipelines.pending-grace-minutes:3}") long pendingGraceMinutes) {
        this.registry = registry;
        this.store = store;
        this.linkWindow = Duration.ofHours(linkWindowHours);
        this.pendingGrace = Duration.ofMinutes(pendingGraceMinutes);
    }

    @EventListener
    public void onRunChanged(RunChangedEvent event) {
        try {
            PipelineGraph g = registry.graph();
            RunStatus r = event.run();
            for (PipelineGraph.NodeRef node : g.nodesFor(r)) {
                if (event.inserted() && r.startedAt() != null) {
                    checkUpstream(g, node, r);
                }
                if (LinkClassifier.isFailure(r.status().name())) {
                    blockDownstream(g, node, r);
                }
            }
        } catch (Exception e) {
            // Never let a linking problem break collection.
            log.warn("Pipeline linking failed for {}: {}", event.run().runId(), e.toString());
        }
    }

    /**
     * Records the link immediately. A non-OK result is only provisional: Informatica is polled every
     * 60 s, so a workflow that just finished may still look RUNNING here. confirmPendingLinks()
     * re-checks it a few minutes later before raising an alert.
     */
    private void checkUpstream(PipelineGraph g, PipelineGraph.NodeRef node, RunStatus r) {
        for (PipelineGraph.Edge e : g.upstreamOf(node)) {
            Instant start = r.startedAt();
            var upstream = store.latestRunStartedBefore(e.upstream(), start, start.minus(linkWindow)).orElse(null);
            var result = LinkClassifier.classify(upstream, start);
            store.saveLink(node.pipeline(), node.nodeId(), r.runId(), start, e.upstream().nodeId(),
                    upstream == null ? null : upstream.runId(), result.state(), result.lag());

            if (result.state() == LinkClassifier.State.OK) {
                log.info("LINKED {}: {} <- {} (started {} after upstream finished)",
                        node.pipeline(), node.nodeId(), e.upstream().nodeId(), Durations.human(result.lag()));
            } else {
                log.debug("Link pending {}: {} <- {} = {}", node.pipeline(), node.nodeId(),
                        e.upstream().nodeId(), result.state());
            }
        }
    }

    @Scheduled(fixedDelay = 60_000, initialDelay = 90_000)
    public void confirmPendingLinks() {
        PipelineGraph g = registry.graph();
        for (var p : store.pendingLinks(Instant.now().minus(pendingGrace))) {
            try {
                var down = g.node(p.pipeline(), p.downstreamNode()).orElse(null);
                var up = g.node(p.pipeline(), p.upstreamNode()).orElse(null);
                if (down == null || up == null) {            // edge removed from the registry since
                    store.resolveLink(p, null, LinkClassifier.State.OK, null);
                    continue;
                }
                Instant start = p.downstreamStart();
                var upstream = store.latestRunStartedBefore(up, start, start.minus(linkWindow)).orElse(null);
                var result = LinkClassifier.classify(upstream, start);
                store.resolveLink(p, upstream == null ? null : upstream.runId(), result.state(), result.lag());

                if (result.state() == LinkClassifier.State.OK) {
                    log.info("LINKED {}: {} <- {} (started {} after upstream finished)",
                            p.pipeline(), p.downstreamNode(), p.upstreamNode(), Durations.human(result.lag()));
                    continue;
                }
                String detail = switch (result.state()) {
                    case UPSTREAM_STILL_RUNNING -> p.downstreamNode() + " started while " + p.upstreamNode()
                            + " was still running (may have loaded incomplete data)";
                    case UPSTREAM_FAILED -> p.downstreamNode() + " started after " + p.upstreamNode()
                            + " " + upstream.status();
                    case NO_UPSTREAM_RUN -> p.downstreamNode() + " started but " + p.upstreamNode()
                            + " has not run in the " + linkWindow.toHours() + "h before it";
                    default -> result.state().name();
                };
                String type = "RAN_ON_" + result.state().name();
                if (store.raiseAlert(type, p.pipeline(), p.downstreamNode(), p.downstreamRunId(), detail)) {
                    log.warn("PIPELINE_ALERT {} [{}]: {}", p.pipeline(), type, detail);
                }
            } catch (Exception ex) {
                log.warn("Could not confirm link {} <- {}: {}", p.downstreamNode(), p.upstreamNode(), ex.toString());
            }
        }
    }

    private void blockDownstream(PipelineGraph g, PipelineGraph.NodeRef node, RunStatus r) {
        List<PipelineGraph.NodeRef> blocked = g.allDownstream(node);
        if (blocked.isEmpty()) return;
        String names = blocked.stream().map(PipelineGraph.NodeRef::nodeId).collect(Collectors.joining(", "));
        String detail = node.nodeId() + " " + r.status() + (r.errorCode() != null ? " (" + r.errorCode() + ")" : "")
                + " - blocks " + blocked.size() + " downstream: " + names;
        if (store.raiseAlert("UPSTREAM_FAILED", node.pipeline(), node.nodeId(), r.runId(), detail)) {
            log.warn("PIPELINE_ALERT {} [UPSTREAM_FAILED]: {}", node.pipeline(), detail);
        }
    }

    /** Upstream succeeded more than max_start_lag ago, but the downstream never started. */
    @Scheduled(fixedDelay = 300_000, initialDelay = 120_000)
    public void checkMissingTriggers() {
        PipelineGraph g = registry.graph();
        Instant now = Instant.now();
        for (PipelineGraph.Edge e : g.edges()) {
            if (e.maxStartLag() == null) continue;
            try {
                var finished = store.succeededRunsEndedBetween(e.upstream(),
                        now.minus(Duration.ofHours(24)), now.minus(e.maxStartLag()));
                for (var up : finished) {
                    if (store.anyRunStartedAtOrAfter(e.downstream(), up.stoppedAt())) continue;
                    String detail = e.downstream().nodeId() + " has not started " + Durations.human(Duration.between(up.stoppedAt(), now))
                            + " after " + e.upstream().nodeId() + " succeeded (expected within "
                            + Durations.human(e.maxStartLag()) + ")";
                    if (store.raiseAlert("MISSING_TRIGGER", e.downstream().pipeline(), e.downstream().nodeId(),
                            up.runId(), detail)) {
                        log.warn("PIPELINE_ALERT {} [MISSING_TRIGGER]: {}", e.downstream().pipeline(), detail);
                    }
                }
            } catch (Exception ex) {
                log.warn("Missing-trigger check failed for {} -> {}: {}",
                        e.upstream().nodeId(), e.downstream().nodeId(), ex.toString());
            }
        }
    }

}
