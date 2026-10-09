package com.hca.monitor.collector.informatica;

import com.hca.monitor.collector.RunStatus;
import com.hca.monitor.collector.RunStatusRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Polls the PowerCenter repository's REP_WFLOW_RUN view every minute and feeds each run into
 * the same run store as Step Functions. Read-only; uses its own small connection pool so the
 * engine's local database is unaffected.
 */
@Component
@ConditionalOnProperty(prefix = "monitor.informatica", name = "enabled", havingValue = "true")
public class InformaticaRepositoryPoller implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(InformaticaRepositoryPoller.class);
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$#]*");

    /** Runs still in flight are returned even if they started before the lookback window. */
    private static final String IN_FLIGHT_CODES = "6, 7, 8, 9, 10";

    private final InformaticaProperties props;
    private final RunStatusRepository repository;
    private final HikariDataSource dataSource;
    private final ZoneId serverZone;
    private final String sql;

    private volatile boolean failing;

    public InformaticaRepositoryPoller(InformaticaProperties props, RunStatusRepository repository) {
        this.props = props;
        this.repository = repository;
        this.serverZone = ZoneId.of(props.serverTimeZone());
        this.sql = buildSql(props);
        this.dataSource = createDataSource(props);
        log.info("Informatica repository poller enabled: {} every {} ms", props.jdbcUrl(), props.pollIntervalMs());
    }

    static String buildSql(InformaticaProperties p) {
        String view = "REP_WFLOW_RUN";
        if (p.repoSchema() != null && !p.repoSchema().isBlank()) {
            if (!IDENTIFIER.matcher(p.repoSchema()).matches()) {
                throw new IllegalArgumentException("Invalid repo-schema: " + p.repoSchema());
            }
            view = p.repoSchema() + "." + view;
        }
        StringBuilder s = new StringBuilder("""
                SELECT SUBJECT_AREA, WORKFLOW_NAME, WORKFLOW_RUN_ID, START_TIME, END_TIME,
                       RUN_STATUS_CODE, RUN_ERR_CODE, RUN_ERR_MSG
                FROM %s
                WHERE (START_TIME >= ? OR RUN_STATUS_CODE IN (%s))""".formatted(view, IN_FLIGHT_CODES));
        List<String> folders = p.folders() == null ? List.of() : p.folders();
        if (!folders.isEmpty()) {
            s.append(" AND SUBJECT_AREA IN (").append("?,".repeat(folders.size() - 1)).append("?)");
        }
        return s.toString();
    }

    private static HikariDataSource createDataSource(InformaticaProperties p) {
        HikariConfig c = new HikariConfig();
        c.setPoolName("informatica-repo");
        c.setJdbcUrl(p.jdbcUrl());
        c.setUsername(p.username());
        c.setPassword(p.password());
        c.setReadOnly(true);
        c.setMaximumPoolSize(2);
        c.setMinimumIdle(0);
        c.setConnectionTimeout(15_000);
        // Don't fail application startup if the repository is unreachable; the poller retries.
        c.setInitializationFailTimeout(-1);
        return new HikariDataSource(c);
    }

    @Scheduled(fixedDelayString = "${monitor.informatica.poll-interval-ms:60000}", initialDelay = 5_000)
    public void poll() {
        Instant observedAt = Instant.now();
        LocalDateTime since = LocalDateTime.now(serverZone).minusHours(props.lookbackHours());
        List<InformaticaStatusMapper.Row> rows = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(props.queryTimeoutSeconds());
            ps.setObject(1, since);
            int i = 2;
            for (String f : props.folders() == null ? List.<String>of() : props.folders()) {
                ps.setString(i++, f);
            }
            long t0 = System.nanoTime();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new InformaticaStatusMapper.Row(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getLong(3),
                            rs.getObject(4, LocalDateTime.class),
                            rs.getObject(5, LocalDateTime.class),
                            rs.getInt(6),
                            rs.getString(7),
                            rs.getString(8)));
                }
            }
            log.debug("Repository query returned {} rows in {} ms", rows.size(), (System.nanoTime() - t0) / 1_000_000);
        } catch (Exception e) {
            if (!failing) {
                log.error("INFORMATICA_COLLECTOR_FAILED: {} (check VPN/network, JDBC URL, credentials and "
                        + "SELECT access on REP_WFLOW_RUN). Retrying every poll.", e.toString());
                failing = true;
            }
            return;
        }

        if (failing) {
            log.info("INFORMATICA_COLLECTOR_RECOVERED: repository query working again");
            failing = false;
        }

        Map<RunStatusRepository.Outcome, Integer> counts = new EnumMap<>(RunStatusRepository.Outcome.class);
        for (InformaticaStatusMapper.Row row : rows) {
            InformaticaStatusMapper.map(row, serverZone, observedAt).ifPresent(r -> {
                RunStatusRepository.Outcome o = repository.upsert(r);
                counts.merge(o, 1, Integer::sum);
                if (o == RunStatusRepository.Outcome.INSERTED || o == RunStatusRepository.Outcome.UPDATED) {
                    log.info("{} {} run {} -> {}{}", o, r.jobId(), r.runName(), r.status(),
                            r.errorCode() != null ? " (" + r.errorCode() + ": " + firstLine(r.errorCause()) + ")" : "");
                }
            });
        }
        log.debug("Informatica poll: {} rows, outcomes {}", rows.size(), counts);
    }

    private static String firstLine(String s) {
        if (s == null) return "";
        int nl = s.indexOf('\n');
        String line = nl >= 0 ? s.substring(0, nl) : s;
        return line.length() > 200 ? line.substring(0, 200) + "..." : line;
    }

    @Override
    public void destroy() {
        dataSource.close();
    }
}
