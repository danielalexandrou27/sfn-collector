package com.hca.monitor.pipeline;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Renders discovery results as a draft pipelines.yml that a person reviews and copies from. Pure logic. */
public final class EdgeCandidatesWriter {

    private EdgeCandidatesWriter() {}

    static final int MAX_UPSTREAMS = 3;

    public static String render(EdgeDiscovery.Result result, EdgeDiscovery.Settings s, int historyDays, Instant generatedAt) {
        StringBuilder y = new StringBuilder();
        y.append("# DRAFT pipeline registry, generated ").append(generatedAt).append('\n')
         .append("# From ").append(historyDays).append(" days of history: a workflow is suggested as upstream of a\n")
         .append("# state machine when it finished successfully within ").append(Durations.human(s.window()))
         .append(" before it started on at least\n")
         .append("# ").append(pct(s.minSupport())).append(" of the days it ran (min ").append(s.minDays())
         .append(" days), and at least ").append(pct(s.minPrecision()))
         .append(" of the workflow's runs were followed by it.\n")
         .append("# Timing is evidence, not proof. Review each block, then copy the ones you agree with\n")
         .append("# into config/pipelines.yml.\n#\n")
         .append("# ").append(result.byDownstream().size()).append(" of ").append(result.downstreamCount())
         .append(" state machines have a likely upstream; ")
         .append(result.upstreamCount()).append(" workflows considered.\n\n")
         .append("pipelines:\n");

        for (Map.Entry<String, List<EdgeDiscovery.Candidate>> e : result.byDownstream().entrySet()) {
            String sm = e.getKey();
            List<EdgeDiscovery.Candidate> cands = e.getValue();
            List<EdgeDiscovery.Candidate> top = cands.subList(0, Math.min(MAX_UPSTREAMS, cands.size()));
            String smId = "sfn__" + id(sm);

            y.append("\n  - name: ").append(q(sm)).append('\n')
             .append("    nodes:\n");
            for (EdgeDiscovery.Candidate c : top) {
                int slash = c.upstream().indexOf('/');
                y.append("      - id: ").append(upId(c.upstream())).append('\n')
                 .append("        informatica: { folder: ").append(q(c.upstream().substring(0, slash)))
                 .append(", workflow: ").append(q(c.upstream().substring(slash + 1))).append(" }\n");
            }
            y.append("      - id: ").append(smId).append('\n')
             .append("        step_function: ").append(q(sm)).append('\n')
             .append("        depends_on: [")
             .append(String.join(", ", top.stream().map(c -> upId(c.upstream())).toList())).append("]\n")
             .append("        max_start_lag_minutes: ").append(suggestedLag(top)).append('\n');
            for (EdgeDiscovery.Candidate c : top) {
                y.append("        # ").append(c.upstream()).append(": ").append(c.daysTogether()).append('/')
                 .append(c.downstreamDays()).append(" days, follows ").append(c.upstreamRunsFollowed()).append('/')
                 .append(c.upstreamRuns()).append(" of its runs, lag median ")
                 .append(Durations.human(c.medianLag())).append(" max ").append(Durations.human(c.maxLag()))
                 .append('\n');
            }
            for (EdgeDiscovery.Candidate c : cands.subList(top.size(), cands.size())) {
                y.append("        # also possible: ").append(c.upstream()).append(" (").append(c.daysTogether())
                 .append('/').append(c.downstreamDays()).append(" days)\n");
            }
        }

        if (!result.downstreamWithoutCandidate().isEmpty()) {
            y.append("\n# No consistent upstream workflow found (scheduled independently, fed by another state\n")
             .append("# machine, or not enough history yet): ").append(result.downstreamWithoutCandidate().size())
             .append(" state machines\n");
            for (String sm : result.downstreamWithoutCandidate()) y.append("#   ").append(sm).append('\n');
        }
        return y.toString();
    }

    /** Max observed lag + 15 min buffer, rounded up to 5 min, capped at 4 h. */
    static int suggestedLag(List<EdgeDiscovery.Candidate> top) {
        long max = top.stream().map(EdgeDiscovery.Candidate::maxLag).mapToLong(Duration::toMinutes).max().orElse(0);
        long m = Math.min(240, max + 15);
        return (int) (((m + 4) / 5) * 5);
    }

    static String upId(String folderSlashWorkflow) {
        return "wf__" + id(folderSlashWorkflow.replace('/', '_'));
    }

    static String id(String s) {
        return s.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    static String q(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String pct(double d) {
        return String.format(Locale.ROOT, "%.0f%%", d * 100);
    }
}
