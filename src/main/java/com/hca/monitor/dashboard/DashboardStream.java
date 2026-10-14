package com.hca.monitor.dashboard;

import com.hca.monitor.incident.IncidentRecordedEvent;
import com.hca.monitor.pipeline.RunChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Live push to open dashboards. Changes are coalesced (one "changed" event at most every 2 s) so a
 * burst of runs (e.g. the first Informatica poll) doesn't flood browsers. A heartbeat every 25 s
 * keeps proxies from closing idle connections; browsers reconnect on their own if a stream drops.
 */
@Component
public class DashboardStream {

    private static final Logger log = LoggerFactory.getLogger(DashboardStream.class);
    private static final long EMITTER_TIMEOUT_MS = Duration.ofMinutes(30).toMillis();

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "dashboard-stream");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> pending;

    public SseEmitter subscribe() {
        SseEmitter e = new SseEmitter(EMITTER_TIMEOUT_MS);
        emitters.add(e);
        e.onCompletion(() -> emitters.remove(e));
        e.onTimeout(() -> emitters.remove(e));
        e.onError(x -> emitters.remove(e));
        try {
            e.send(SseEmitter.event().name("hello").data("connected").reconnectTime(5_000));
        } catch (Exception x) {
            emitters.remove(e);
        }
        return e;
    }

    @EventListener
    public void onRunChanged(RunChangedEvent event) {
        changed();
    }

    @EventListener
    public void onIncident(IncidentRecordedEvent event) {
        changed();
    }

    private synchronized void changed() {
        if (pending != null && !pending.isDone()) return;
        pending = timer.schedule(() -> broadcast("changed", "refresh"), 2, TimeUnit.SECONDS);
    }

    @Scheduled(fixedDelay = 25_000, initialDelay = 25_000)
    public void heartbeat() {
        broadcast("heartbeat", "ok");
    }

    private void broadcast(String name, String data) {
        for (SseEmitter e : emitters) {
            try {
                e.send(SseEmitter.event().name(name).data(data));
            } catch (Exception gone) {
                emitters.remove(e);
            }
        }
        if ("changed".equals(name) && !emitters.isEmpty()) {
            log.debug("Dashboard update pushed to {} viewer(s)", emitters.size());
        }
    }
}
