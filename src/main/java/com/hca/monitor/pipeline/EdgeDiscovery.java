package com.hca.monitor.pipeline;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Suggests "workflow -> state machine" dependencies from run timing. Pure logic.
 *
 * A workflow W is a likely upstream of state machine S when, over the history:
 *   - support:   on most days S ran, a successful run of W finished within {@code window} before S started
 *   - precision: most of W's successful runs are followed by S within the window
 *                (filters out hourly workflows that happen to finish before everything)
 *   - at least {@code minDays} distinct days of evidence.
 * Timing only suggests an edge; a person confirms it.
 */
public final class EdgeDiscovery {

    private EdgeDiscovery() {}

    /** {@code key} is "folder/workflow" for Informatica, the state machine name for Step Functions. */
    public record Run(String source, String key, String status, Instant startedAt, Instant stoppedAt) {}

    public record Settings(Duration window, ZoneId zone, int minDays, double minSupport, double minPrecision) {
        public static Settings defaults(ZoneId zone) {
            return new Settings(Duration.ofMinutes(120), zone, 3, 0.8, 0.5);
        }
    }

    public record Candidate(
            String upstream, String downstream,
            int daysTogether, int downstreamDays, double support,
            int upstreamRunsFollowed, int upstreamRuns, double precision,
            Duration medianLag, Duration maxLag) {}

    public record Result(Map<String, List<Candidate>> byDownstream, Set<String> downstreamWithoutCandidate,
                         int downstreamCount, int upstreamCount) {}

    public static Result discover(List<Run> runs, Settings s, Set<String> alreadyConfigured) {
        List<Run> ups = runs.stream()
                .filter(r -> PipelineGraph.INFA.equals(r.source()) && "SUCCEEDED".equals(r.status()) && r.stoppedAt() != null)
                .sorted(Comparator.comparing(Run::stoppedAt))
                .toList();
        List<Run> downs = runs.stream()
                .filter(r -> PipelineGraph.SFN.equals(r.source()) && r.startedAt() != null)
                .toList();

        Map<String, Integer> upRunCount = new HashMap<>();
        for (Run u : ups) upRunCount.merge(u.key(), 1, Integer::sum);

        Instant[] upEnds = ups.stream().map(Run::stoppedAt).toArray(Instant[]::new);
        Map<String, Set<LocalDate>> downDays = new HashMap<>();
        Map<String, Set<LocalDate>> pairDays = new HashMap<>();
        Map<String, List<Long>> pairLags = new HashMap<>();
        Map<String, Set<Integer>> pairUpRuns = new HashMap<>();

        for (Run d : downs) {
            LocalDate day = d.startedAt().atZone(s.zone()).toLocalDate();
            downDays.computeIfAbsent(d.key(), k -> new HashSet<>()).add(day);

            Instant from = d.startedAt().minus(s.window());
            Map<String, Integer> nearestPerUpstream = new HashMap<>(); // up key -> index of latest-ending run
            for (int i = firstAtOrAfter(upEnds, from); i < upEnds.length && !upEnds[i].isAfter(d.startedAt()); i++) {
                nearestPerUpstream.put(ups.get(i).key(), i); // ascending, so the last one wins = nearest
            }
            for (var e : nearestPerUpstream.entrySet()) {
                String pair = e.getKey() + ">" + d.key();
                pairDays.computeIfAbsent(pair, k -> new HashSet<>()).add(day);
                pairLags.computeIfAbsent(pair, k -> new ArrayList<>())
                        .add(Duration.between(upEnds[e.getValue()], d.startedAt()).toSeconds());
                pairUpRuns.computeIfAbsent(pair, k -> new HashSet<>()).add(e.getValue());
            }
        }

        Map<String, List<Candidate>> byDown = new TreeMap<>();
        for (var e : pairDays.entrySet()) {
            String pair = e.getKey();
            int sep = pair.indexOf('>');
            String up = pair.substring(0, sep), down = pair.substring(sep + 1);
            if (alreadyConfigured.contains(PipelineGraph.INFA + "|" + up + ">" + PipelineGraph.SFN + "|" + down)) continue;

            int together = e.getValue().size();
            int dDays = downDays.get(down).size();
            int followed = pairUpRuns.get(pair).size();
            int upRuns = upRunCount.getOrDefault(up, 1);
            double support = (double) together / dDays;
            double precision = (double) followed / upRuns;
            if (together < s.minDays() || support < s.minSupport() || precision < s.minPrecision()) continue;

            List<Long> lags = pairLags.get(pair);
            lags.sort(null);
            byDown.computeIfAbsent(down, k -> new ArrayList<>()).add(new Candidate(
                    up, down, together, dDays, support, followed, upRuns, precision,
                    Duration.ofSeconds(lags.get(lags.size() / 2)), Duration.ofSeconds(lags.get(lags.size() - 1))));
        }
        for (List<Candidate> list : byDown.values()) {
            list.sort(Comparator.comparingDouble(Candidate::support).reversed()
                    .thenComparing(Comparator.comparingDouble(Candidate::precision).reversed())
                    .thenComparing(Candidate::medianLag));
        }

        Set<String> without = new java.util.TreeSet<>(downDays.keySet());
        without.removeAll(byDown.keySet());
        return new Result(byDown, without, downDays.size(), upRunCount.size());
    }

    private static int firstAtOrAfter(Instant[] sorted, Instant t) {
        int lo = 0, hi = sorted.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (sorted[mid].isBefore(t)) lo = mid + 1; else hi = mid;
        }
        return lo;
    }
}
