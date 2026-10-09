package com.hca.monitor.collector.informatica;

import com.hca.monitor.collector.RunStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

class InformaticaStatusMapperTest {

    private static final ZoneId PT = ZoneId.of("America/Los_Angeles");
    private static final Instant NOW = Instant.parse("2026-10-09T17:00:00Z");

    private static InformaticaStatusMapper.Row row(int code, LocalDateTime end, String errCode, String errMsg) {
        return new InformaticaStatusMapper.Row("LOANS", "wf_LOANS_EXTRACT", 777L, 12345L,
                LocalDateTime.of(2026, 10, 9, 2, 0), end, code, errCode, errMsg);
    }

    @Test
    void succeededRunConvertsServerLocalTimeToUtc() {
        RunStatus r = InformaticaStatusMapper.map(
                row(1, LocalDateTime.of(2026, 10, 9, 2, 30), "0", null), PT, NOW).orElseThrow();

        assertThat(r.status()).isEqualTo(RunStatus.Status.SUCCEEDED);
        assertThat(r.jobId()).isEqualTo("LOANS/wf_LOANS_EXTRACT");
        assertThat(r.runId()).isEqualTo("infa:LOANS/wf_LOANS_EXTRACT/12345");
        assertThat(r.startedAt()).isEqualTo(Instant.parse("2026-10-09T09:00:00Z")); // 02:00 PDT
        assertThat(r.stoppedAt()).isEqualTo(Instant.parse("2026-10-09T09:30:00Z"));
        assertThat(r.errorCode()).isNull();
    }

    @Test
    void failedRunKeepsError() {
        RunStatus r = InformaticaStatusMapper.map(
                row(3, LocalDateTime.of(2026, 10, 9, 2, 5), "36331", "Session task instance failed"),
                PT, NOW).orElseThrow();

        assertThat(r.status()).isEqualTo(RunStatus.Status.FAILED);
        assertThat(r.errorCode()).isEqualTo("36331");
        assertThat(r.errorCause()).isEqualTo("Session task instance failed");
    }

    @Test
    void runningRunHasNoStopTime() {
        RunStatus r = InformaticaStatusMapper.map(row(6, null, null, null), PT, NOW).orElseThrow();
        assertThat(r.status()).isEqualTo(RunStatus.Status.RUNNING);
        assertThat(r.stoppedAt()).isNull();
    }

    @Test
    void suspendedIsNotTerminal() {
        RunStatus r = InformaticaStatusMapper.map(row(8, null, null, null), PT, NOW).orElseThrow();
        assertThat(r.status()).isEqualTo(RunStatus.Status.SUSPENDED);
        assertThat(r.status().isTerminal()).isFalse();
    }

    @Test
    void scheduledAndDisabledRowsAreSkipped() {
        assertThat(InformaticaStatusMapper.map(row(2, null, null, null), PT, NOW)).isEmpty();
        assertThat(InformaticaStatusMapper.map(row(12, null, null, null), PT, NOW)).isEmpty();
    }
}
