package com.hca.monitor.incident;

import org.junit.jupiter.api.Test;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IncidentFormatterTest {

    static IncidentProperties props(Map<String, String> fields) {
        return new IncidentProperties(true, IncidentProperties.Mode.DRY_RUN, "https://hcadev.service-now.com/",
                List.of("FAILED", "TIMED_OUT", "SUSPENDED"), 3, 5, "America/Los_Angeles", "us-west-2",
                "./data/incident-outbox", List.of("6", "7", "8"), fields, "{{job}} {{status_word}}",
                "{{summary}}\n\nFailed step: {{failed_step}} (attempts: {{attempts}})\nLink: {{link}}",
                null, null);
    }

    static Map<String, String> fixed() {
        Map<String, String> f = new LinkedHashMap<>();
        f.put("caller_id", "6816f79cc0a8016401c5a33be04be441");
        f.put("category", "application");
        f.put("subcategory", "batch_job");
        f.put("cmdb_ci", "b0c4030ac0a800090152e7a4564ca36c");
        f.put("u_vendor_related", "no");
        f.put("contact_type", "email");
        f.put("assignment_group", "8a5055c9c61122780043563ef53438e3");
        f.put("assigned_to", "6816f79cc0a8016401c5a33be04be441");
        return f;
    }

    static IncidentCandidate sfnFailure() {
        return new IncidentCandidate(
                "arn:aws:states:us-west-2:1:execution:hca-loans-load:exec-1", "STEP_FUNCTIONS",
                "arn:aws:states:us-west-2:1:stateMachine:hca-loans-load", "hca-loans-load", "exec-1", "FAILED",
                Instant.parse("2026-10-13T09:35:00Z"), Instant.parse("2026-10-13T09:41:10Z"), Instant.now(),
                "States.TaskFailed", "{...}", "Load Loans",
                "glue hca_loans_load_job: AnalysisException: Table or view not found: stg.loans^daily",
                "AnalysisException", 3, null, 0, null);
    }

    @Test
    void buildsShortDescriptionAndDescriptionFromTemplates() {
        var f = IncidentFormatter.incidentFields(sfnFailure(), props(fixed()));
        assertThat(f.get("short_description")).isEqualTo("hca-loans-load failed");
        assertThat(f.get("description")).startsWith("glue hca_loans_load_job: AnalysisException: Table or view not found")
                .contains("Failed step: Load Loans (attempts: 3)");
        assertThat(f.get("assignment_group")).isEqualTo("8a5055c9c61122780043563ef53438e3");
    }

    @Test
    void linkPrefillsEveryFieldAndStripsCaretsFromText() {
        var f = IncidentFormatter.incidentFields(sfnFailure(), props(fixed()));
        String link = ServiceNowLinks.newIncident("https://hcadev.service-now.com/", f);
        assertThat(link).startsWith("https://hcadev.service-now.com/incident.do?sys_id=-1&sysparm_query=");
        String query = URLDecoder.decode(link.substring(link.indexOf("sysparm_query=") + 14), StandardCharsets.UTF_8);
        assertThat(query).startsWith("caller_id=6816f79cc0a8016401c5a33be04be441^category=application^subcategory=batch_job")
                .contains("^contact_type=email^assignment_group=8a5055c9c61122780043563ef53438e3^")
                .contains("^short_description=hca-loans-load failed^description=")
                .contains("stg.loansdaily")               // "^" removed from free text
                .doesNotContain("^^");
        assertThat(link).doesNotContain(" ").doesNotContain("+");
    }

    @Test
    void placeholdersAreDetected() {
        Map<String, String> f = new LinkedHashMap<>(fixed());
        f.put("cmdb_ci", "<sys_id of ADW>");
        assertThat(ServiceNowLinks.unfilled(f)).isEqualTo(List.of("cmdb_ci"));
    }

    @Test
    void freeTextComparisonIgnoresWhitespaceButIdsMustMatchExactly() {
        assertThat(ServiceNowBrowser.matches("description", "a\n\nb", "a\r\nb")).isEqualTo(true);
        assertThat(ServiceNowBrowser.matches("assignment_group", "abc", "abd")).isEqualTo(false);
    }
}
