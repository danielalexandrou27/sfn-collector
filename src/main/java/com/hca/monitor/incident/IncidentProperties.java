package com.hca.monitor.incident;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "monitor.incidents")
public record IncidentProperties(
        boolean enabled,
        /** DRY_RUN writes each prefilled link to the outbox; BROWSER opens it and clicks Submit. */
        Mode mode,
        /** e.g. https://hcadev.service-now.com */
        String instanceUrl,
        /** Which run statuses raise a ticket. ABORTED is usually a manual stop, so it is off by default. */
        List<String> statuses,
        /** Step Functions: wait this long for the failure detail (failed state, Glue/Lambda error) before raising. */
        int waitForDetailMinutes,
        /** Safety valve: at most this many tickets per check. */
        int maxPerPoll,
        String timeZone,
        String awsRegion,
        String outboxDir,
        /** Incident state values that count as no longer open (Resolved, Closed, Canceled). */
        List<String> closedStates,
        /**
         * Fixed incident fields: ServiceNow internal field name -> internal value
         * (sys_ids for reference fields such as caller_id, cmdb_ci, assignment_group, assigned_to).
         */
        Map<String, String> fields,
        String shortDescriptionTemplate,
        String descriptionTemplate,
        Browser browser,
        Resolve resolve) {

    public enum Mode { DRY_RUN, BROWSER }

    public record Browser(
            /** msedge or chrome uses the installed browser (no download); chromium uses Playwright's own. */
            String channel,
            boolean headless,
            /** Separate browser profile so the ServiceNow login is kept between runs. */
            String profileDir,
            String screenshotDir,
            /** Headed mode: how long to wait for you to sign in when the session has expired. */
            int loginWaitMinutes,
            int timeoutSeconds) {}

    public record Resolve(
            boolean enabled,
            /** Fields set on the incident before clicking Resolve, e.g. state, close_code, close_notes. */
            Map<String, String> fields) {}
}
