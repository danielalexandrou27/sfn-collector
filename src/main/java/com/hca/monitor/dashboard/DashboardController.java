package com.hca.monitor.dashboard;

import com.hca.monitor.dashboard.DashboardModel.Dashboard;
import com.hca.monitor.dashboard.DashboardModel.Job;
import com.hca.monitor.pipeline.PipelineGraph;
import com.hca.monitor.pipeline.PipelineRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.security.Principal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Read-only dashboard API. All endpoints require a signed-in session (see SecurityConfig). */
@RestController
public class DashboardController {

    private final DashboardStore store;
    private final PipelineRegistry registry;
    private final DashboardStream stream;
    private final ZoneId zone;
    private final DashboardBuilder.Links links;

    public DashboardController(DashboardStore store, PipelineRegistry registry, DashboardStream stream,
                               @Value("${monitor.dashboard.time-zone:America/Los_Angeles}") String zone,
                               @Value("${monitor.incidents.instance-url:}") String serviceNowUrl,
                               @Value("${AWS_REGION:us-west-2}") String region) {
        this.store = store;
        this.registry = registry;
        this.stream = stream;
        this.zone = ZoneId.of(zone);
        this.links = new DashboardBuilder.Links(serviceNowUrl.isBlank() ? null : serviceNowUrl, region);
    }

    /** One batch day (midnight to midnight in the configured time zone); defaults to today. */
    @GetMapping("/api/dashboard")
    public Dashboard dashboard(@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        LocalDate day = date != null ? date : LocalDate.now(zone);
        return DashboardBuilder.build(registry.graph(), runsOf(day), day, zone, Instant.now(), links);
    }

    /** Every run of one job on a batch day, newest first (for the "N runs" expand). */
    @GetMapping("/api/attempts")
    public List<Job> attempts(@RequestParam String type, @RequestParam String name,
                              @RequestParam(required = false) String folder,
                              @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        LocalDate day = date != null ? date : LocalDate.now(zone);
        String key = type + "|" + (PipelineGraph.INFA.equals(type) ? folder + "/" + name : name);
        Instant now = Instant.now();
        return runsOf(day).stream()
                .filter(r -> r.key().equals(key))
                .sorted(Comparator.comparing(RunView::startedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .map(r -> DashboardBuilder.toJob(type, name, folder, r, 1, now, links))
                .toList();
    }

    @GetMapping("/api/me")
    public Map<String, String> me(Principal principal) {
        return Map.of("user", principal.getName());
    }

    /** Server-sent events: "changed" whenever a run or incident changes; the page then reloads its data. */
    @GetMapping(value = "/api/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return stream.subscribe();
    }

    private List<RunView> runsOf(LocalDate day) {
        return store.runsStartedBetween(day.atStartOfDay(zone).toInstant(), day.plusDays(1).atStartOfDay(zone).toInstant());
    }
}
