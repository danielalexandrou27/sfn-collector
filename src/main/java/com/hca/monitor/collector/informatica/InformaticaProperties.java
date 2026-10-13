package com.hca.monitor.collector.informatica;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "monitor.informatica")
public record InformaticaProperties(
        boolean enabled,
        String jdbcUrl,
        String username,
        String password,
        /** Schema that owns the OPB_ tables, e.g. INFA_REPO_PRD. Blank = user's default schema. */
        String repoSchema,
        /** Time zone of the Informatica server; START_TIME/END_TIME are stored in its local time. */
        String serverTimeZone,
        int lookbackHours,
        long pollIntervalMs,
        int queryTimeoutSeconds,
        /** Optional: only these folders (SUBJECT_AREA). Empty = all folders. */
        List<String> folders) {
}
