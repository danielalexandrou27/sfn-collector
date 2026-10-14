package com.hca.monitor.incident;

/** Published when an incident is created, recorded as a duplicate, or resolved, so open dashboards refresh. */
public record IncidentRecordedEvent(String runId, String status) {}
