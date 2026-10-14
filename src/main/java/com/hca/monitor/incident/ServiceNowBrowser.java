package com.hca.monitor.incident;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.TimeoutError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Drives the ServiceNow classic incident form in a real browser, signed in as you.
 *
 * Create: open the prefilled link, check every field came through as configured, click Submit,
 * then reopen the incident by number to confirm it exists.
 * Resolve: open the incident, set the resolve fields, click Resolve (or Update), confirm the state.
 *
 * Uses the page's own form API (g_form) to read and set values instead of CSS selectors, so form
 * layout changes don't break it. Not thread-safe: call from one thread only.
 */
public class ServiceNowBrowser implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ServiceNowBrowser.class);
    private static final DateTimeFormatter SHOT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    public record Created(String number, String sysId) {}

    public static class IncidentException extends Exception {
        public final String screenshot;
        /** INC number shown on the form when Submit was clicked; set if the failure happened after Submit. */
        public final String submittedNumber;
        public IncidentException(String message, String screenshot) {
            this(message, screenshot, null);
        }
        public IncidentException(String message, String screenshot, String submittedNumber) {
            super(message + (screenshot != null ? " (screenshot: " + screenshot + ")" : ""));
            this.screenshot = screenshot;
            this.submittedNumber = submittedNumber;
        }
    }

    private final IncidentProperties.Browser cfg;
    private final String instanceUrl;
    private Playwright playwright;
    private BrowserContext context;

    public ServiceNowBrowser(IncidentProperties.Browser cfg, String instanceUrl) {
        this.cfg = cfg;
        this.instanceUrl = instanceUrl;
    }

    public Created createIncident(String url, Map<String, String> expected) throws IncidentException {
        Page page = page();
        open(page, url);

        String number = str(page.evaluate("() => g_form.getValue('number')"));
        boolean isNew = Boolean.TRUE.equals(page.evaluate("() => g_form.isNewRecord()"));
        if (number.isBlank() || !isNew) {
            throw new IncidentException("Prefilled link did not open a new incident form", screenshot(page, "not-new-form"));
        }

        List<String> mismatches = new ArrayList<>();
        for (var e : expected.entrySet()) {
            String actual = str(page.evaluate("f => g_form.getValue(f)", e.getKey()));
            if (!matches(e.getKey(), e.getValue(), actual)) {
                mismatches.add(e.getKey() + " expected '" + abbrev(e.getValue()) + "' but form has '" + abbrev(actual) + "'");
            }
        }
        if (!mismatches.isEmpty()) {
            throw new IncidentException("Form did not prefill as configured - not submitting: " + String.join("; ", mismatches),
                    screenshot(page, "prefill-mismatch"));
        }

        try {
            clickAndWait(page, "#sysverb_insert", "#sysverb_insert_bottom");
            String error = errorMessage(page);
            if (error != null) {
                throw new IncidentException("ServiceNow rejected the submit: " + error, screenshot(page, "submit-error"));
            }
        } catch (IncidentException | RuntimeException e) {
            // Submit was clicked: the incident may exist even though this step failed. Carry the number
            // so a retry checks for it instead of submitting a second ticket.
            throw new IncidentException(e.getMessage(), null, number);
        }

        // Confirm it exists by opening it by number.
        Created created = findIncident(number);
        if (created == null) {
            throw new IncidentException("Submit clicked but " + number + " was not found afterwards",
                    screenshot(page, "not-created"), number);
        }
                return created;
    }

    /** Opens an incident by number; returns it if it exists, null if not. */
    public Created findIncident(String number) throws IncidentException {
        Page page = page();
        open(page, ServiceNowLinks.incidentByNumber(instanceUrl, number));
        String sysId = str(page.evaluate("() => g_form.getUniqueValue()"));
        String found = str(page.evaluate("() => g_form.getValue('number')"));
        boolean isNew = Boolean.TRUE.equals(page.evaluate("() => g_form.isNewRecord()"));
        if (isNew || sysId.isBlank() || "-1".equals(sysId) || !number.equals(found)) return null;
        return new Created(number, sysId);
    }

    /** Current state value of an incident, e.g. "2" In Progress, "6" Resolved, "7" Closed, "8" Canceled. */
    public String incidentState(String sysId) throws IncidentException {
        Page page = page();
        open(page, ServiceNowLinks.incidentBySysId(instanceUrl, sysId));
        return str(page.evaluate("() => g_form.getValue('state')"));
    }

    public void resolveIncident(String sysId, String number, Map<String, String> fields) throws IncidentException {
        Page page = page();
        open(page, ServiceNowLinks.incidentBySysId(instanceUrl, sysId));
        for (var e : fields.entrySet()) {
            page.evaluate("([f, v]) => g_form.setValue(f, v)", List.of(e.getKey(), e.getValue()));
        }
        if (page.locator("#resolve_incident").count() > 0) {
            clickAndWait(page, "#resolve_incident");
        } else {
            clickAndWait(page, "#sysverb_update", "#sysverb_update_bottom");
        }
        String error = errorMessage(page);
        if (error != null) {
            throw new IncidentException("ServiceNow rejected the resolve on " + number + ": " + error, screenshot(page, "resolve-error"));
        }
        String wantState = fields.get("state");
        if (wantState != null) {
            open(page, ServiceNowLinks.incidentBySysId(instanceUrl, sysId));
            String state = str(page.evaluate("() => g_form.getValue('state')"));
            if (!wantState.equals(state)) {
                throw new IncidentException(number + " state is '" + state + "' after resolve, expected '" + wantState + "'",
                        screenshot(page, "resolve-state"));
            }
        }
    }

    /** Opens a form URL; if the session has expired, waits for you to sign in (headed) or fails (headless). */
    private void open(Page page, String url) throws IncidentException {
        page.navigate(url);
        if (waitForForm(page, 20_000)) return;

        if (cfg.headless()) {
            throw new IncidentException("SERVICENOW_LOGIN_REQUIRED: no ServiceNow session. Run once with "
                    + "monitor.incidents.browser.headless=false and sign in.", screenshot(page, "login-required"));
        }
        log.warn("SERVICENOW_LOGIN_REQUIRED: sign in to ServiceNow in the browser window that just opened "
                + "(waiting up to {} min)", cfg.loginWaitMinutes());
        Instant deadline = Instant.now().plusSeconds(cfg.loginWaitMinutes() * 60L);
        while (Instant.now().isBefore(deadline)) {
            page.waitForTimeout(5_000);
            if (page.url().contains("service-now.com") && !page.url().contains("login")) {
                page.navigate(url);
                if (waitForForm(page, 20_000)) {
                    log.info("ServiceNow session restored");
                    return;
                }
            }
        }
        throw new IncidentException("SERVICENOW_LOGIN_REQUIRED: not signed in within " + cfg.loginWaitMinutes() + " min",
                screenshot(page, "login-timeout"));
    }

    /** ServiceNow keeps background connections open, so wait for the form API rather than network idle. */
    private static boolean waitForForm(Page page, double timeoutMs) {
        try {
            page.waitForFunction("() => typeof window.g_form !== 'undefined' && !!g_form.getTableName",
                    null, new Page.WaitForFunctionOptions().setTimeout(timeoutMs));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** Clicks the first visible button and waits for the page to submit; a client-side validation error means no navigation. */
    private void clickAndWait(Page page, String... selectors) throws IncidentException {
        Locator button = null;
        for (String s : selectors) {
            Locator l = page.locator(s);
            if (l.count() > 0 && l.first().isVisible()) { button = l.first(); break; }
        }
        if (button == null) {
            throw new IncidentException("Button not found: " + String.join(" / ", selectors), screenshot(page, "no-button"));
        }
        final Locator b = button;
        try {
            page.waitForNavigation(new Page.WaitForNavigationOptions().setTimeout(cfg.timeoutSeconds() * 1000.0), b::click);
        } catch (TimeoutError noNavigation) {
            // Stayed on the page: usually a mandatory-field or validation message, checked by the caller.
        }
    }

    private static String errorMessage(Page page) {
        Locator err = page.locator(".outputmsg_error, .notification-error, .fieldmsg.notification-error");
        if (err.count() == 0) return null;
        String text = err.first().innerText().trim();
        return text.isBlank() ? "error message shown on form" : text;
    }


    /** Reference/choice fields must match exactly; free text is compared ignoring whitespace differences. */
    static boolean matches(String field, String expected, String actual) {
        if ("short_description".equals(field) || "description".equals(field) || "work_notes".equals(field)) {
            return norm(expected).equals(norm(actual));
        }
        return expected.trim().equals(actual.trim());
    }

    private static String norm(String s) {
        return s == null ? "" : s.replaceAll("\\s+", " ").trim();
    }

    private static String abbrev(String s) {
        return s == null ? "" : s.length() <= 60 ? s : s.substring(0, 57) + "...";
    }

    private static String str(Object o) {
        return o == null ? "" : o.toString();
    }

    private Page page() throws IncidentException {
        try {
            if (context == null) {
                playwright = Playwright.create();
                BrowserType.LaunchPersistentContextOptions o = new BrowserType.LaunchPersistentContextOptions()
                        .setHeadless(cfg.headless());
                if (cfg.channel() != null && !cfg.channel().isBlank() && !"chromium".equals(cfg.channel())) {
                    o.setChannel(cfg.channel());
                }
                Files.createDirectories(Path.of(cfg.profileDir()));
                context = playwright.chromium().launchPersistentContext(Path.of(cfg.profileDir()), o);
                context.setDefaultTimeout(cfg.timeoutSeconds() * 1000.0);
            }
            return context.pages().isEmpty() ? context.newPage() : context.pages().get(0);
        } catch (Exception e) {
            close();
            throw new IncidentException("Could not start the browser (" + cfg.channel() + "): " + e.getMessage(), null);
        }
    }

    private String screenshot(Page page, String what) {
        try {
            Path dir = Path.of(cfg.screenshotDir());
            Files.createDirectories(dir);
            Path file = dir.resolve(SHOT.format(LocalDateTime.now()) + "_" + what + ".png");
            page.screenshot(new Page.ScreenshotOptions().setPath(file).setFullPage(true));
            return file.toString();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() {
        try { if (context != null) context.close(); } catch (Exception ignored) { }
        try { if (playwright != null) playwright.close(); } catch (Exception ignored) { }
        context = null;
        playwright = null;
    }
}
