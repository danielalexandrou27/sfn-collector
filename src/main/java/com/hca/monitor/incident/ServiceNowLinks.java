package com.hca.monitor.incident;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Builds ServiceNow form URLs. Pure logic. */
public final class ServiceNowLinks {

    private ServiceNowLinks() {}

    /**
     * New incident form with fields prefilled:
     * {instance}/incident.do?sys_id=-1&sysparm_query=field=value^field=value...
     * In an encoded query "^" separates fields, so a literal "^" in a value is written "^^".
     */
    public static String newIncident(String instanceUrl, Map<String, String> fields) {
        String query = fields.entrySet().stream()
                .filter(e -> e.getValue() != null && !e.getValue().isBlank())
                .map(e -> e.getKey() + "=" + e.getValue().replace("^", "^^"))
                .collect(Collectors.joining("^"));
        return base(instanceUrl) + "/incident.do?sys_id=-1&sysparm_query=" + encode(query);
    }

    public static String incidentBySysId(String instanceUrl, String sysId) {
        return base(instanceUrl) + "/incident.do?sys_id=" + encode(sysId);
    }

    public static String incidentByNumber(String instanceUrl, String number) {
        return base(instanceUrl) + "/incident.do?sysparm_query=" + encode("number=" + number);
    }

    /** Config values still holding the "<...>" placeholders from application.yml. */
    public static List<String> unfilled(Map<String, String> fields) {
        return fields == null ? List.of() : fields.entrySet().stream()
                .filter(e -> e.getValue() == null || e.getValue().isBlank() || e.getValue().trim().startsWith("<"))
                .map(Map.Entry::getKey).toList();
    }

    private static String base(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
