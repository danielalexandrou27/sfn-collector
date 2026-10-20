package com.hca.monitor.notify;

import com.hca.monitor.collector.RunStatus;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * config/notifications.yml: which jobs send WhatsApp messages, to whom, and with what text; plus desktop pop-up
 * settings. Pure logic (no Spring) so it can be tested directly.
 */
public final class NotificationRules {

    private NotificationRules() {}

    // ------------------------------------------------------------------ the file

    public record File(Desktop desktop, WhatsApp whatsapp, List<JobRule> jobs) {
        public Desktop desktopOrDefault() {
            return desktop != null ? desktop : new Desktop(true, false, false);
        }
    }

    /** Windows pop-ups. Defaults: every failure, no successes, registered or not. */
    public record Desktop(Boolean onFailure, Boolean onSuccess, Boolean onlyRegistered) {
        public boolean failures() { return onFailure == null || onFailure; }
        public boolean successes() { return onSuccess != null && onSuccess; }
        public boolean registeredOnly() { return onlyRegistered != null && onlyRegistered; }
    }

    public record WhatsApp(Boolean enabled, Map<String, Recipient> recipients, List<String> defaultTo,
                           Map<String, String> defaultMessages, Boolean sendTestOnStartup) {
        public boolean on() { return enabled == null || enabled; }
    }

    /**
     * Either a phone number in international format (+1...) or, with the WAHA provider, a chat_id: a group
     * ("1203630xxxxxxxx@g.us", listed in the engine log at startup) or a contact ("17145550123@c.us").
     * callmebot_apikey only for the CallMeBot provider.
     */
    public record Recipient(String phone, String chatId, String callmebotApikey) {
        public String describe() { return chatId != null && !chatId.isBlank() ? chatId : phone; }
    }

    /**
     * One job (or a group, with * wildcards) and its messages. For each of started / succeeded / failed:
     * leave it out for no message, write "default" for the default message, or write your own text.
     */
    public record JobRule(String stepFunction, Informatica informatica, List<String> to,
                          String started, String succeeded, String failed) {}

    public record Informatica(String folder, String workflow) {}

    // ------------------------------------------------------------------ events

    public enum Kind {
        STARTED("started"), SUCCEEDED("succeeded"), FAILED("failed");
        public final String key;
        Kind(String key) { this.key = key; }

        /** Which notification a run's new status means, or null for none. */
        public static Kind of(RunStatus.Status s) {
            return switch (s) {
                case RUNNING -> STARTED;
                case SUCCEEDED -> SUCCEEDED;
                case FAILED, TIMED_OUT, ABORTED, SUSPENDED -> FAILED;
                default -> null;
            };
        }
    }

    public record Planned(String recipientName, Recipient recipient, String text) {}

    // ------------------------------------------------------------------ validation

    /** Every problem in the file, or an empty list. */
    public static List<String> problems(File f) {
        List<String> errors = new ArrayList<>();
        if (f == null) return errors;
        WhatsApp w = f.whatsapp();
        Map<String, Recipient> recipients = w == null || w.recipients() == null ? Map.of() : w.recipients();
        recipients.forEach((name, r) -> {
            boolean hasPhone = r != null && r.phone() != null && !r.phone().isBlank();
            boolean hasChat = r != null && r.chatId() != null && !r.chatId().isBlank();
            if (hasPhone == hasChat) {
                errors.add("whatsapp.recipients." + name + ": set either phone or chat_id (a group id ending in @g.us)");
            } else if (hasPhone && !r.phone().trim().matches("\\+?[0-9 ()-]{8,20}")) {
                errors.add("whatsapp.recipients." + name + ": phone must be a number like \"+17145551234\"");
            } else if (hasChat && !r.chatId().trim().matches("[0-9-]+@(g|c)\\.us")) {
                errors.add("whatsapp.recipients." + name + ": chat_id should look like \"120363012345678901@g.us\" "
                        + "(group) or \"17145550123@c.us\" (person)");
            }
        });
        if (w != null && w.defaultTo() != null) {
            for (String n : w.defaultTo()) {
                if (!recipients.containsKey(n)) errors.add("whatsapp.default_to: unknown recipient '" + n + "'");
            }
        }
        Map<String, String> defaults = w == null || w.defaultMessages() == null ? Map.of() : w.defaultMessages();
        for (String k : defaults.keySet()) {
            if (!List.of("started", "succeeded", "failed").contains(k)) {
                errors.add("whatsapp.default_messages: '" + k + "' should be started, succeeded or failed");
            }
        }
        List<JobRule> jobs = f.jobs() == null ? List.of() : f.jobs();
        for (int i = 0; i < jobs.size(); i++) {
            JobRule j = jobs.get(i);
            String where = "jobs[" + (i + 1) + "] " + label(j);
            boolean sfn = j.stepFunction() != null && !j.stepFunction().isBlank();
            boolean infa = j.informatica() != null;
            if (sfn == infa) { errors.add(where + ": set exactly one of step_function or informatica"); continue; }
            if (infa && (blank(j.informatica().folder()) || blank(j.informatica().workflow()))) {
                errors.add(where + ": informatica needs folder and workflow (use \"*\" for any)");
            }
            if (j.started() == null && j.succeeded() == null && j.failed() == null) {
                errors.add(where + ": has no started / succeeded / failed message, so it would never send anything");
            }
            List<String> to = j.to() != null ? j.to() : w == null || w.defaultTo() == null ? List.of() : w.defaultTo();
            if (to.isEmpty()) errors.add(where + ": no 'to' and no whatsapp.default_to");
            for (String n : to) if (!recipients.containsKey(n)) errors.add(where + ": unknown recipient '" + n + "'");
            for (Kind k : Kind.values()) {
                String msg = message(j, k);
                if ("default".equalsIgnoreCase(trim(msg)) && blank(defaults.get(k.key))) {
                    errors.add(where + ": " + k.key + " is 'default' but whatsapp.default_messages." + k.key + " is not set");
                }
            }
        }
        return errors;
    }

    // ------------------------------------------------------------------ planning

    /** The first job rule that matches this run, or null. */
    public static JobRule match(File f, RunStatus run) {
        if (f == null || f.jobs() == null) return null;
        for (JobRule j : f.jobs()) {
            if ("STEP_FUNCTIONS".equals(run.source()) && j.stepFunction() != null
                    && glob(j.stepFunction()).matcher(run.jobName()).matches()) return j;
            if ("INFORMATICA".equals(run.source()) && j.informatica() != null) {
                String id = run.jobId() == null ? "" : run.jobId();
                int slash = id.indexOf('/');
                String folder = slash > 0 ? id.substring(0, slash) : "";
                String wf = slash > 0 ? id.substring(slash + 1) : run.jobName();
                if (glob(j.informatica().folder()).matcher(folder).matches()
                        && glob(j.informatica().workflow()).matcher(wf).matches()) return j;
            }
        }
        return null;
    }

    /** WhatsApp messages to send for this run and event (empty if none). */
    public static List<Planned> plan(File f, RunStatus run, Kind kind, Map<String, String> vars) {
        WhatsApp w = f == null ? null : f.whatsapp();
        if (w == null || !w.on() || kind == null) return List.of();
        JobRule j = match(f, run);
        if (j == null) return List.of();
        String template = trim(message(j, kind));
        if (template == null || template.isEmpty()) return List.of();
        if ("default".equalsIgnoreCase(template)) {
            template = w.defaultMessages() == null ? null : w.defaultMessages().get(kind.key);
            if (blank(template)) return List.of();
        }
        String text = fill(template, vars);
        List<String> to = j.to() != null ? j.to() : w.defaultTo() == null ? List.of() : w.defaultTo();
        List<Planned> out = new ArrayList<>();
        for (String name : new java.util.LinkedHashSet<>(to)) {
            Recipient r = w.recipients() == null ? null : w.recipients().get(name);
            if (r != null) out.add(new Planned(name, r, text));
        }
        return out;
    }

    /** Replaces {{name}} placeholders; unknown ones become empty. Caps the text at WhatsApp's practical length. */
    public static String fill(String template, Map<String, String> vars) {
        StringBuilder out = new StringBuilder();
        var m = Pattern.compile("\\{\\{\\s*([a-z_]+)\\s*}}").matcher(template);
        while (m.find()) {
            String v = vars.get(m.group(1));
            m.appendReplacement(out, java.util.regex.Matcher.quoteReplacement(v == null ? "" : v));
        }
        m.appendTail(out);
        String s = out.toString().strip();
        return s.length() <= 1500 ? s : s.substring(0, 1497) + "...";
    }

    /** Placeholder names, documented in notifications.example.yml. */
    public static Map<String, String> emptyVars() {
        Map<String, String> v = new LinkedHashMap<>();
        for (String k : List.of("job", "type", "folder", "status", "status_word", "run", "started", "ended",
                "duration", "error", "failed_step", "link", "dashboard", "date")) v.put(k, "");
        return v;
    }

    // ------------------------------------------------------------------ helpers

    private static String message(JobRule j, Kind k) {
        return switch (k) {
            case STARTED -> j.started();
            case SUCCEEDED -> j.succeeded();
            case FAILED -> j.failed();
        };
    }

    /** "*" matches anything, "?" one character; everything else literal. Case-sensitive, like the job names. */
    static Pattern glob(String g) {
        StringBuilder re = new StringBuilder();
        for (char c : (g == null ? "" : g.trim()).toCharArray()) {
            if (c == '*') re.append(".*");
            else if (c == '?') re.append('.');
            else re.append(Pattern.quote(String.valueOf(c)));
        }
        return Pattern.compile(re.toString());
    }

    private static String label(JobRule j) {
        if (j.stepFunction() != null) return "(step_function " + j.stepFunction() + ")";
        if (j.informatica() != null) return "(informatica " + j.informatica().folder() + "/" + j.informatica().workflow() + ")";
        return "";
    }

    private static boolean blank(String s) { return s == null || s.isBlank(); }
    private static String trim(String s) { return s == null ? null : s.trim(); }
}
