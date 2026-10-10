package com.hca.monitor.pipeline;

import java.time.Duration;

final class Durations {

    private Durations() {}

    /** 45s, 12m, 2h, 3h15m */
    static String human(Duration d) {
        if (d == null) return "?";
        long m = d.toMinutes();
        if (m < 1) return d.toSeconds() + "s";
        if (m < 120) return m + "m";
        return (m / 60) + "h" + (m % 60 == 0 ? "" : (m % 60) + "m");
    }
}
