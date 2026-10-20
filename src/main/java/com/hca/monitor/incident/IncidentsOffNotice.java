package com.hca.monitor.incident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/** Says so at startup when ServiceNow incidents are switched off, so a quiet engine isn't mistaken for a broken one. */
@Component
@ConditionalOnProperty(prefix = "monitor.incidents", name = "enabled", havingValue = "false", matchIfMissing = true)
class IncidentsOffNotice {

    private static final Logger log = LoggerFactory.getLogger(IncidentsOffNotice.class);

    @EventListener(ApplicationReadyEvent.class)
    void announce() {
        log.warn("ServiceNow incidents are OFF - failures will not raise tickets. Set monitor.incidents.enabled: true "
                + "in config/local.yml (or INCIDENTS_ENABLED=true) to turn them on.");
    }
}
