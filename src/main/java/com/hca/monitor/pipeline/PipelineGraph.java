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

    /** Builds and validates the graph; throws IllegalArgumentException listing every problem found. */
    public static PipelineGraph build(PipelineConfig.File file) {
        List<String> errors = new ArrayList<>();
        List<PipelineConfig.Pipeline> pipelines = file == null || file.pipelines() == null ? List.of() : file.pipelines();
        PipelineGraph g = new PipelineGraph(pipelines.size());
        Set<String> pipelineNames = new HashSet<>();

        for (PipelineConfig.Pipeline p : pipelines) {
            String pName = p.name();
            if (pName == null || pName.isBlank()) { errors.add("A pipeline has no name"); continue; }
            if (!pipelineNames.add(pName)) errors.add("Duplicate pipeline name: " + pName);
            List<PipelineConfig.Node> nodes = p.nodes() == null ? List.of() : p.nodes();

            Map<String, NodeRef> refs = new LinkedHashMap<>();
            for (PipelineConfig.Node n : nodes) {
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
            }

            Map<String, List<String>> deps = new HashMap<>();
            for (PipelineConfig.Node n : nodes) {
                NodeRef down = refs.get(n.id());
                if (down == null || n.dependsOn() == null) continue;
                Duration lag = n.maxStartLagMinutes() == null ? null : Duration.ofMinutes(n.maxStartLagMinutes());
                for (String upId : n.dependsOn()) {
                    NodeRef up = refs.get(upId);
                    if (up == null) { errors.add(pName + "/" + n.id() + ": depends_on unknown node '" + upId + "'"); continue; }
                    if (up.equals(down)) { errors.add(pName + "/" + n.id() + ": depends on itself"); continue; }
                    deps.computeIfAbsent(n.id(), k -> new ArrayList<>()).add(upId);
                    Edge e = new Edge(up, down, lag);
                    g.edges.add(e);
                    g.upstreamEdges.computeIfAbsent(down, k -> new ArrayList<>()).add(e);
                    g.downstreamEdges.computeIfAbsent(up, k -> new ArrayList<>()).add(e);
                }
            }
            String cycle = findCycle(refs.keySet(), deps);
            if (cycle != null) errors.add(pName + ": dependency cycle " + cycle);

            for (NodeRef r : refs.values()) {
                g.byJobKey.computeIfAbsent(r.key(), k -> new ArrayList<>()).add(r);
                g.byId.put(pName + "/" + r.nodeId(), r);
            }
        }

        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("pipelines.yml has " + errors.size() + " problem(s):\n  - "
                    + String.join("\n  - ", errors));
        }
        return g;
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
