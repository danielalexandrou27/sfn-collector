package com.hca.monitor.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

/** Runs edge discovery over stored history and writes a draft registry to data/edge-candidates.yml. */
@Component
public class EdgeDiscoveryJob {

    private static final Logger log = LoggerFactory.getLogger(EdgeDiscoveryJob.class);

    private final PipelineStore store;
    private final PipelineRegistry registry;
    private final Path output;
    private final int historyDays;
    private final EdgeDiscovery.Settings settings;

    public EdgeDiscoveryJob(PipelineStore store, PipelineRegistry registry,
                            @Value("${monitor.pipelines.discovery.output:./data/edge-candidates.yml}") String output,
                            @Value("${monitor.pipelines.discovery.history-days:30}") int historyDays,
                            @Value("${monitor.pipelines.discovery.window-minutes:120}") long windowMinutes,
                            @Value("${monitor.pipelines.discovery.min-days:3}") int minDays,
                            @Value("${monitor.pipelines.discovery.min-support:0.8}") double minSupport,
                            @Value("${monitor.pipelines.discovery.min-precision:0.5}") double minPrecision,
                            @Value("${monitor.informatica.server-time-zone:America/Los_Angeles}") String zone) {
        this.store = store;
        this.registry = registry;
        this.output = Path.of(output);
        this.historyDays = historyDays;
        this.settings = new EdgeDiscovery.Settings(Duration.ofMinutes(windowMinutes), ZoneId.of(zone),
                minDays, minSupport, minPrecision);
    }

    /** First run 10 min after startup (after backfills), then every 6 h. */
    @Scheduled(initialDelay = 600_000, fixedDelay = 21_600_000)
    public void run() {
        try {
            long t0 = System.currentTimeMillis();
            var runs = store.runsSince(Instant.now().minus(Duration.ofDays(historyDays)));
            var result = EdgeDiscovery.discover(runs, settings, registry.graph().configuredEdgeKeys());
            Files.createDirectories(output.toAbsolutePath().getParent());
            Files.writeString(output, EdgeCandidatesWriter.render(result, settings, historyDays, Instant.now()),
                    StandardCharsets.UTF_8);
            log.info("EDGE_DISCOVERY: {} of {} state machines have a likely upstream workflow ({} runs analysed in {} ms) -> {}",
                    result.byDownstream().size(), result.downstreamCount(), runs.size(),
                    System.currentTimeMillis() - t0, output.toAbsolutePath());
        } catch (Exception e) {
            log.warn("Edge discovery failed: {}", e.toString());
        }
    }
}
