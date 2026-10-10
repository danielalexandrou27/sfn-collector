package com.hca.monitor.pipeline;

import com.hca.monitor.collector.RunStatus;

/** Published by the collectors whenever a run is inserted or its status changes. */
public record RunChangedEvent(RunStatus run, boolean inserted) {}
