package com.hca.monitor.pipeline;

import java.util.List;

/** Shape of pipelines.yml (snake_case keys). */
public final class PipelineConfig {

    private PipelineConfig() {}

    public record File(List<Pipeline> pipelines) {}

    public record Pipeline(String name, List<Node> nodes) {}

    /**
     * Exactly one of {@code informatica} or {@code stepFunction} is set.
     * {@code stepFunction} is the state machine name (not the ARN).
     */
    public record Node(
            String id,
            InformaticaRef informatica,
            String stepFunction,
            List<String> dependsOn,
            Integer maxStartLagMinutes) {}

    public record InformaticaRef(String folder, String workflow) {}
}
