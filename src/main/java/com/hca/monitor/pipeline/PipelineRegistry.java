package com.hca.monitor.pipeline;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

/**
 * Loads pipelines.yml and reloads it when the file changes (checked every 30 s), so edges can be
 * added without restarting. An invalid edit is rejected and the previous version stays active.
 */
@Component
public class PipelineRegistry {

    private static final Logger log = LoggerFactory.getLogger(PipelineRegistry.class);

    private final Path file;
    private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    private volatile PipelineGraph graph = PipelineGraph.empty();
    private volatile FileTime loadedVersion;
    private volatile boolean missingLogged;

    public PipelineRegistry(@Value("${monitor.pipelines.file:./config/pipelines.yml}") String file) {
        this.file = Path.of(file);
        reloadIfChanged();
    }

    public PipelineGraph graph() {
        return graph;
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void reloadIfChanged() {
        try {
            if (!Files.exists(file)) {
                if (!missingLogged) {
                    missingLogged = true;
                    log.info("No pipeline registry at {} - dependency checks are off until it exists. "
                            + "See config/pipelines.example.yml.", file.toAbsolutePath());
                }
                loadedVersion = null;
                graph = PipelineGraph.empty();
                return;
            }
            missingLogged = false;
            FileTime modified = Files.getLastModifiedTime(file);
            if (modified.equals(loadedVersion)) return;

            PipelineConfig.File parsed = yaml.readValue(file.toFile(), PipelineConfig.File.class);
            PipelineGraph g = PipelineGraph.build(parsed);
            graph = g;
            loadedVersion = modified;
            log.info("PIPELINES_LOADED: {} pipelines, {} nodes, {} edges from {}",
                    g.pipelineCount(), g.nodeCount(), g.edges().size(), file.toAbsolutePath());
        } catch (Exception e) {
            try { loadedVersion = Files.getLastModifiedTime(file); } catch (Exception ignored) { }
            log.error("PIPELINES_INVALID: {} - keeping the previous version ({} pipelines).\n{}",
                    file.toAbsolutePath(), graph.pipelineCount(), e.getMessage());
        }
    }
}
