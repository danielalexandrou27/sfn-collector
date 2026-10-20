package com.hca.monitor.pipeline;

import com.hca.monitor.collector.RunStatus;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validated, in-memory form of pipelines.yml. Pure logic: no Spring, no database.
 * A job (workflow or state machine) may appear in several pipelines.
 */
public final class PipelineGraph {

    public static final String SFN = "STEP_FUNCTIONS";
    public static final String INFA = "INFORMATICA";

    /** One node in one pipeline. {@code matchValue} is a workflow's "folder/name" or a state machine name. */
    public record NodeRef(String pipeline, String nodeId, String source, String matchValue) {
        public String key() { return key(source, matchValue); }
        static String key(String source, String value) { return source + "|" + value; }
    }

    public record Edge(NodeRef upstream, NodeRef downstream, Duration maxStartLag) {}

    private final Map<String, List<NodeRef>> byJobKey = new HashMap<>();
    private final Map<String, NodeRef> byId = new HashMap<>();
    private final List<NodeRef> nodes = new ArrayList<>();
    private final Map<NodeRef, java.time.LocalTime> expectedBy = new HashMap<>();
    private final Map<NodeRef, List<Edge>> upstreamEdges = new HashMap<>();
    private final Map<NodeRef, List<Edge>> downstreamEdges = new HashMap<>();
    private final List<Edge> edges = new ArrayList<>();
    private final int pipelineCount;

    private PipelineGraph(int pipelineCount) {
        this.pipelineCount = pipelineCount;
    }

    public static PipelineGraph empty() {
        return new PipelineGraph(0);
    }

    /**
     * Builds and validates the graph; throws IllegalArgumentException listing every problem found.
     *
     * A depends_on entry can name, in this order of preference:
     *   1. a node id in the same pipeline                       depends_on: [loans_extract]
     *   2. a node in another pipeline as "pipeline/node_id"     depends_on: [HCA_EDW_DAYBREAK/prod-Application]
     *   3. a node id or step function name used anywhere else   depends_on: [prod-Application]
     *      in the file, as long as it identifies exactly one node.
     * So a step function can wait on another step function (or workflow) registered under any pipeline.
     */
    public static PipelineGraph build(PipelineConfig.File file) {
        List<String> errors = new ArrayList<>();
        List<PipelineConfig.Pipeline> pipelines = file == null || file.pipelines() == null ? List.of() : file.pipelines();
        PipelineGraph g = new PipelineGraph(pipelines.size());
        Set<String> pipelineNames = new HashSet<>();

        // Pass 1: every node in every pipeline.
        Map<String, Map<String, NodeRef>> refsByPipeline = new LinkedHashMap<>();
        Map<String, List<NodeRef>> byBareName = new HashMap<>();     // node id or step function name -> nodes
        for (PipelineConfig.Pipeline p : pipelines) {
            String pName = p.name();
            if (pName == null || pName.isBlank()) { errors.add("A pipeline has no name"); continue; }
            if (!pipelineNames.add(pName)) { errors.add("Duplicate pipeline name: " + pName); continue; }
            Map<String, NodeRef> refs = new LinkedHashMap<>();
            refsByPipeline.put(pName, refs);
            for (PipelineConfig.Node n : p.nodes() == null ? List.<PipelineConfig.Node>of() : p.nodes()) {
                String where = pName + "/" + n.id();
                if (n.id() == null || n.id().isBlank()) { errors.add(pName + ": a node has no id"); continue; }
                if (refs.containsKey(n.id())) { errors.add(where + ": duplicate node id"); continue; }
                boolean hasInfa = n.informatica() != null;
                boolean hasSfn = n.stepFunction() != null && !n.stepFunction().isBlank();
                if (hasInfa == hasSfn) {
                    errors.add(where + ": set exactly one of 'informatica' or 'step_function'");
                    continue;
                }
                NodeRef ref;
                if (hasInfa) {
                    var i = n.informatica();
                    if (blank(i.folder()) || blank(i.workflow())) {
                        errors.add(where + ": informatica needs both 'folder' and 'workflow'");
                        continue;
                    }
                    ref = new NodeRef(pName, n.id(), INFA, i.folder() + "/" + i.workflow());
                } else {
                    String sm = n.stepFunction().trim();
                    if (sm.startsWith("arn:")) sm = sm.substring(sm.lastIndexOf(':') + 1); // accept ARNs too
                    ref = new NodeRef(pName, n.id(), SFN, sm);
                }
                refs.put(n.id(), ref);
                byBareName.computeIfAbsent(n.id(), k -> new ArrayList<>()).add(ref);
                if (SFN.equals(ref.source()) && !ref.matchValue().equals(n.id())) {
                    byBareName.computeIfAbsent(ref.matchValue(), k -> new ArrayList<>()).add(ref);
                }
                if (n.expectedBy() != null && !n.expectedBy().isBlank()) {
                    try {
                        g.expectedBy.put(ref, java.time.LocalTime.parse(n.expectedBy().trim()));
                    } catch (java.time.format.DateTimeParseException bad) {
                        errors.add(where + ": expected_by must be HH:mm, got '" + n.expectedBy() + "'");
                    }
                }
            }
        }

        // Pass 2: dependencies, which may point into other pipelines.
        Map<String, List<String>> deps = new HashMap<>();             // "pipeline/id" -> upstream "pipeline/id"s
        for (PipelineConfig.Pipeline p : pipelines) {
            Map<String, NodeRef> refs = refsByPipeline.get(p.name());
            if (refs == null || p.nodes() == null) continue;
            for (PipelineConfig.Node n : p.nodes()) {
                NodeRef down = refs.get(n.id());
                if (down == null || n.dependsOn() == null) continue;
                String where = p.name() + "/" + n.id();
                Duration lag = n.maxStartLagMinutes() == null ? null : Duration.ofMinutes(n.maxStartLagMinutes());
                Set<NodeRef> seenUp = new HashSet<>();
                for (String raw : n.dependsOn()) {
                    String upName = raw == null ? "" : raw.trim();
                    NodeRef up = resolve(upName, refs, refsByPipeline, byBareName, where, errors);
                    if (up == null) continue;
                    if (up.equals(down)) { errors.add(where + ": depends on itself"); continue; }
                    if (!seenUp.add(up)) continue;                     // listed twice: ignore the repeat
                    deps.computeIfAbsent(id(down), k -> new ArrayList<>()).add(id(up));
                    Edge e = new Edge(up, down, lag);
                    g.edges.add(e);
                    g.upstreamEdges.computeIfAbsent(down, k -> new ArrayList<>()).add(e);
                    g.downstreamEdges.computeIfAbsent(up, k -> new ArrayList<>()).add(e);
                }
            }
        }

        Set<String> allIds = new LinkedHashSet<>();
        refsByPipeline.values().forEach(m -> m.values().forEach(r -> allIds.add(id(r))));
        String cycle = findCycle(allIds, deps);
        if (cycle != null) errors.add("dependency cycle " + cycle);

        for (Map<String, NodeRef> refs : refsByPipeline.values()) {
            for (NodeRef r : refs.values()) {
                g.byJobKey.computeIfAbsent(r.key(), k -> new ArrayList<>()).add(r);
                g.byId.put(id(r), r);
                g.nodes.add(r);
            }
        }

        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("pipelines.yml has " + errors.size() + " problem(s):\n  - "
                    + String.join("\n  - ", errors));
        }
        return g;
    }

    private static String id(NodeRef r) {
        return r.pipeline() + "/" + r.nodeId();
    }

    /** Finds the node a depends_on entry names (see build); adds an error and returns null if it can't. */
    private static NodeRef resolve(String name, Map<String, NodeRef> samePipeline,
                                   Map<String, Map<String, NodeRef>> refsByPipeline,
                                   Map<String, List<NodeRef>> byBareName, String where, List<String> errors) {
        if (name.isEmpty()) { errors.add(where + ": depends_on has an empty entry"); return null; }
        NodeRef local = samePipeline.get(name);
        if (local != null) return local;

        int slash = name.indexOf('/');
        if (slash > 0) {
            Map<String, NodeRef> other = refsByPipeline.get(name.substring(0, slash));
            NodeRef r = other == null ? null : other.get(name.substring(slash + 1));
            if (r != null) return r;
        }

        List<NodeRef> matches = byBareName.getOrDefault(name, List.of()).stream().distinct().toList();
        if (matches.size() == 1) return matches.get(0);
        if (matches.isEmpty()) {
            errors.add(where + ": depends_on '" + name + "' doesn't match any node id or step function in pipelines.yml"
                    + " (add the upstream as its own node first)");
        } else {
            errors.add(where + ": depends_on '" + name + "' matches more than one node ("
                    + String.join(", ", matches.stream().map(PipelineGraph::id).toList())
                    + "); write it as pipeline/node_id");
        }
        return null;
    }

    private static String findCycle(Set<String> ids, Map<String, List<String>> deps) {
        Map<String, Integer> state = new HashMap<>(); // 1 = visiting, 2 = done
        for (String start : ids) {
            Deque<String> path = new ArrayDeque<>();
            String c = dfs(start, deps, state, path);
            if (c != null) return c;
        }
        return null;
    }

    private static String dfs(String id, Map<String, List<String>> deps, Map<String, Integer> state, Deque<String> path) {
        Integer s = state.get(id);
        if (s != null && s == 2) return null;
        if (s != null && s == 1) {
            List<String> loop = new ArrayList<>();
            boolean in = false;
            for (var it = path.descendingIterator(); it.hasNext(); ) {
                String x = it.next();
                if (x.equals(id)) in = true;
                if (in) loop.add(x);
            }
            loop.add(id);
            return String.join(" -> ", loop);
        }
        state.put(id, 1);
        path.push(id);
        for (String up : deps.getOrDefault(id, List.of())) {
            String c = dfs(up, deps, state, path);
            if (c != null) return c;
        }
        path.pop();
        state.put(id, 2);
        return null;
    }

    private static boolean blank(String s) { return s == null || s.isBlank(); }

    /** Nodes (in any pipeline) that this run belongs to. */
    public List<NodeRef> nodesFor(RunStatus r) {
        String value = SFN.equals(r.source()) ? r.jobName() : r.jobId();
        return byJobKey.getOrDefault(NodeRef.key(r.source(), value), List.of());
    }

    /** Every node in file order. */
    public List<NodeRef> nodes() { return nodes; }

    /** expected_by for a node, or null. */
    public java.time.LocalTime expectedBy(NodeRef n) { return expectedBy.get(n); }

    public java.util.Optional<NodeRef> node(String pipeline, String nodeId) {
        return java.util.Optional.ofNullable(byId.get(pipeline + "/" + nodeId));
    }

    public List<Edge> upstreamOf(NodeRef n) { return upstreamEdges.getOrDefault(n, List.of()); }

    public List<Edge> downstreamOf(NodeRef n) { return downstreamEdges.getOrDefault(n, List.of()); }

    /** Every node reachable downstream of {@code n}, nearest first. */
    public List<NodeRef> allDownstream(NodeRef n) {
        Set<NodeRef> seen = new LinkedHashSet<>();
        Deque<NodeRef> queue = new ArrayDeque<>(List.of(n));
        while (!queue.isEmpty()) {
            for (Edge e : downstreamOf(queue.poll())) {
                if (seen.add(e.downstream())) queue.add(e.downstream());
            }
        }
        return new ArrayList<>(seen);
    }

    public List<Edge> edges() { return edges; }

    public Set<String> configuredEdgeKeys() {
        Set<String> s = new HashSet<>();
        for (Edge e : edges) s.add(e.upstream().key() + ">" + e.downstream().key());
        return s;
    }

    public int pipelineCount() { return pipelineCount; }

    public int nodeCount() { return byJobKey.values().stream().mapToInt(List::size).sum(); }
}
