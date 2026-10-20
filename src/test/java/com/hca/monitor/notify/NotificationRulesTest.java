package com.hca.monitor.notify;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hca.monitor.collector.RunStatus;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationRulesTest {

    static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    static RunStatus sfn(String name, RunStatus.Status s) {
        return new RunStatus("STEP_FUNCTIONS", "arn:sm:" + name, name, "arn:exec:" + name + ":1", "1", s,
                Instant.now(), Instant.now(), null, null, 0, Instant.now(), Instant.now());
    }

    static RunStatus wf(String folder, String wf, RunStatus.Status s) {
        return new RunStatus("INFORMATICA", folder + "/" + wf, wf, "infa:" + folder + "/" + wf + "/1", "1", s,
                Instant.now(), Instant.now(), null, null, 0, Instant.now(), Instant.now());
    }

    @Test
    void exampleFileIsValidAndPlansMessages() throws Exception {
        var f = YAML.readValue(new File("config/notifications.example.yml"), NotificationRules.File.class);
        assertThat(NotificationRules.problems(f)).isEmpty();

        var v = NotificationRules.emptyVars();
        v.put("job", "prod-Application"); v.put("ended", "4:32 AM"); v.put("duration", "12m");
        v.put("status_word", "failed"); v.put("error", "glue x: boom"); v.put("failed_step", "Load");

        assertThat(NotificationRules.plan(f, sfn("prod-Application", RunStatus.Status.SUCCEEDED), NotificationRules.Kind.SUCCEEDED, v))
                .singleElement().satisfies(p -> assertThat(p.text())
                        .isEqualTo("✅ Daybreak Application load done at 4:32 AM (12m). Reports can run."));
        assertThat(NotificationRules.plan(f, sfn("prod-Application", RunStatus.Status.FAILED), NotificationRules.Kind.FAILED, v))
                .singleElement().satisfies(p -> assertThat(p.text()).startsWith("❌ prod-Application failed at 4:32 AM\nStep: Load"));
        assertThat(NotificationRules.plan(f, sfn("prod-Application", RunStatus.Status.RUNNING), NotificationRules.Kind.STARTED, v)).isEmpty();
        assertThat(NotificationRules.plan(f, sfn("prod-collections_ph2_ksiop_crawler_trg", RunStatus.Status.TIMED_OUT), NotificationRules.Kind.FAILED, v)).hasSize(1);
        assertThat(NotificationRules.plan(f, sfn("unlisted", RunStatus.Status.FAILED), NotificationRules.Kind.FAILED, v)).isEmpty();
        assertThat(NotificationRules.plan(f, wf("HCA_CRM_SKIP", "wf_any", RunStatus.Status.FAILED), NotificationRules.Kind.FAILED, v)).hasSize(1);
        assertThat(NotificationRules.plan(f, wf("ADW_KSIOP", "wf_POOL", RunStatus.Status.FAILED), NotificationRules.Kind.FAILED, v)).isEmpty();
    }

    @Test
    void mistakesAreReported() throws Exception {
        var bad = YAML.readValue("""
                whatsapp:
                  recipients: { x: { phone: abc } }
                  default_to: [y]
                jobs:
                  - step_function: a
                    failed: default
                  - step_function: c
                    to: [x]
                """, NotificationRules.File.class);
        assertThat(NotificationRules.problems(bad)).anyMatch(p -> p.contains("phone must be"))
                .anyMatch(p -> p.contains("unknown recipient 'y'"))
                .anyMatch(p -> p.contains("default_messages.failed is not set"))
                .anyMatch(p -> p.contains("would never send anything"));
    }
}
