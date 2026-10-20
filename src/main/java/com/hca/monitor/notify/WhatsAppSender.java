package com.hca.monitor.notify;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

/**
 * Sends one WhatsApp text through the configured provider:
 *   WAHA       self-hosted open-source gateway (WhatsApp HTTP API) linked to your own WhatsApp number;
 *              can post to groups. Runs next to the engine (deploy/waha).
 *   CALLMEBOT  free, for messaging yourself/your team: each phone gets its own API key (see README)
 *   TWILIO     Twilio WhatsApp API (sandbox for testing, approved sender for production)
 *   META       WhatsApp Business Cloud API (Meta)
 * Twilio and Meta only deliver free-form text to someone who messaged the sender in the last 24 h;
 * otherwise they require a pre-approved template.
 */
public class WhatsAppSender {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final NotifyProperties.WhatsAppProvider cfg;
    private final HttpClient http;
    /** WAHA runs on the same host or network: never through the company proxy. */
    private final HttpClient direct = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    public WhatsAppSender(NotifyProperties.WhatsAppProvider cfg) {
        this.cfg = cfg;
        HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15));
        if (cfg.proxy() != null && !cfg.proxy().isBlank()) {
            String[] hp = cfg.proxy().trim().split(":");
            b.proxy(ProxySelector.of(new InetSocketAddress(hp[0], hp.length > 1 ? Integer.parseInt(hp[1]) : 8080)));
        } else {
            b.proxy(ProxySelector.getDefault());   // honours -Dhttps.proxyHost / system settings
        }
        this.http = b.build();
    }

    public NotifyProperties.Provider provider() {
        return cfg.provider() == null ? NotifyProperties.Provider.WAHA : cfg.provider();
    }

    /** Problems with the provider settings, or null when ready. */
    public String configProblem() {
        return switch (provider()) {
            case WAHA -> blank(cfg.wahaUrl()) ? "WAHA needs monitor.notify.whatsapp.waha-url (e.g. http://localhost:3000)" : null;
            case CALLMEBOT -> null;   // per-recipient key, checked per message
            case TWILIO -> blank(cfg.twilioAccountSid()) || blank(cfg.twilioAuthToken()) || blank(cfg.twilioFrom())
                    ? "TWILIO needs monitor.notify.whatsapp.twilio-account-sid, twilio-auth-token and twilio-from" : null;
            case META -> blank(cfg.metaPhoneNumberId()) || blank(cfg.metaAccessToken())
                    ? "META needs monitor.notify.whatsapp.meta-phone-number-id and meta-access-token" : null;
        };
    }

    public void send(NotificationRules.Recipient to, String text) throws Exception {
        boolean hasChat = to.chatId() != null && !to.chatId().isBlank();
        if (hasChat && provider() != NotifyProperties.Provider.WAHA) {
            throw new IllegalStateException("chat_id (groups) only works with the WAHA provider; " + provider() + " needs a phone");
        }
        String phone = hasChat ? "" : to.phone().replaceAll("[^0-9+]", "");
        HttpRequest req = switch (provider()) {
            case WAHA -> {
                String chatId = hasChat ? to.chatId().trim() : phone.replace("+", "") + "@c.us";
                String body = JSON.writeValueAsString(Map.of("session", session(), "chatId", chatId, "text", text));
                yield waha("/api/sendText").timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build();
            }
            case CALLMEBOT -> {
                if (blank(to.callmebotApikey())) {
                    throw new IllegalStateException("no callmebot_apikey for " + phone + " in notifications.yml");
                }
                yield HttpRequest.newBuilder(URI.create("https://api.callmebot.com/whatsapp.php?phone=" + enc(phone)
                                + "&text=" + enc(text) + "&apikey=" + enc(to.callmebotApikey().trim())))
                        .timeout(Duration.ofSeconds(30)).GET().build();
            }
            case TWILIO -> {
                String form = "From=" + enc("whatsapp:" + normalize(cfg.twilioFrom())) + "&To=" + enc("whatsapp:" + normalize(phone))
                        + "&Body=" + enc(text);
                String auth = Base64.getEncoder().encodeToString(
                        (cfg.twilioAccountSid() + ":" + cfg.twilioAuthToken()).getBytes(StandardCharsets.UTF_8));
                yield HttpRequest.newBuilder(URI.create("https://api.twilio.com/2010-04-01/Accounts/"
                                + cfg.twilioAccountSid() + "/Messages.json"))
                        .timeout(Duration.ofSeconds(30))
                        .header("Authorization", "Basic " + auth)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(form)).build();
            }
            case META -> {
                String body = JSON.writeValueAsString(Map.of(
                        "messaging_product", "whatsapp",
                        "to", phone.replace("+", ""),
                        "type", "text",
                        "text", Map.of("body", text)));
                String version = blank(cfg.metaApiVersion()) ? "v21.0" : cfg.metaApiVersion();
                yield HttpRequest.newBuilder(URI.create("https://graph.facebook.com/" + version + "/"
                                + cfg.metaPhoneNumberId() + "/messages"))
                        .timeout(Duration.ofSeconds(30))
                        .header("Authorization", "Bearer " + cfg.metaAccessToken())
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build();
            }
        };
        HttpClient client = provider() == NotifyProperties.Provider.WAHA ? direct : http;
        HttpResponse<String> res;
        try {
            res = client.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (java.net.ConnectException e) {
            throw new IllegalStateException(provider() == NotifyProperties.Provider.WAHA
                    ? "WAHA is not reachable at " + cfg.wahaUrl() + " - is the waha container running? (docker ps)"
                    : "could not connect to " + provider() + ": " + e.getMessage());
        }
        String body = res.body() == null ? "" : res.body();
        boolean ok = res.statusCode() / 100 == 2;
        if (provider() == NotifyProperties.Provider.CALLMEBOT) {
            // CallMeBot answers 200 even for some errors; its error pages say so in the text.
            String lower = body.toLowerCase();
            if (lower.contains("apikey is invalid") || lower.contains("error") && !lower.contains("queued")) ok = false;
        }
        if (!ok) {
            throw new IllegalStateException(provider() + " answered " + res.statusCode() + ": " + abbrev(stripTags(body)));
        }
    }

    /** WAHA session state, e.g. WORKING, SCAN_QR_CODE, STOPPED, FAILED; or a short error. */
    public String wahaStatus() {
        try {
            HttpResponse<String> res = direct.send(waha("/api/sessions/" + session()).GET().timeout(Duration.ofSeconds(10)).build(),
                    HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() == 401) return "UNAUTHORIZED (waha-api-key doesn't match WAHA_API_KEY)";
            if (res.statusCode() == 404) return "NO_SESSION (start the '" + session() + "' session in the WAHA dashboard)";
            var status = JSON.readTree(res.body()).path("status").asText("");
            return status.isEmpty() ? "HTTP " + res.statusCode() : status;
        } catch (java.net.ConnectException e) {
            return "NOT_REACHABLE at " + cfg.wahaUrl();
        } catch (Exception e) {
            return "ERROR " + e.getMessage();
        }
    }

    /** Groups the linked number is in, as "name -> chat_id", so you can copy the id into notifications.yml. */
    public Map<String, String> wahaGroups() throws Exception {
        HttpResponse<String> res = direct.send(waha("/api/" + session() + "/groups?exclude=participants&sortBy=subject&sortOrder=asc")
                .GET().timeout(Duration.ofSeconds(30)).build(), HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) throw new IllegalStateException("WAHA answered " + res.statusCode());
        Map<String, String> out = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        var root = JSON.readTree(res.body());
        // The list shape differs between WAHA engines: an array, or an object keyed by group id.
        Iterable<com.fasterxml.jackson.databind.JsonNode> items = root.isArray() ? root : () -> root.elements();
        var keys = root.isObject() ? root.fieldNames() : null;
        for (var g : items) {
            String key = keys != null && keys.hasNext() ? keys.next() : null;
            String id = textOf(g.path("id"));
            if (id.isEmpty()) id = textOf(g.path("JID"));
            if (id.isEmpty() && key != null) id = key;
            String name = firstNonBlank(g.path("subject").asText(""), g.path("name").asText(""), g.path("Name").asText(""),
                    g.path("groupMetadata").path("subject").asText(""));
            if (!id.isEmpty()) out.put(name.isEmpty() ? "(no name)" : name, id);
        }
        return out;
    }

    private HttpRequest.Builder waha(String path) {
        String base = cfg.wahaUrl().endsWith("/") ? cfg.wahaUrl().substring(0, cfg.wahaUrl().length() - 1) : cfg.wahaUrl();
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).header("Accept", "application/json");
        if (!blank(cfg.wahaApiKey())) b.header("X-Api-Key", cfg.wahaApiKey());
        return b;
    }

    private String session() {
        return blank(cfg.wahaSession()) ? "default" : cfg.wahaSession().trim();
    }

    private static String textOf(com.fasterxml.jackson.databind.JsonNode n) {
        if (n.isTextual()) return n.asText();
        if (n.has("_serialized")) return n.get("_serialized").asText("");
        return "";
    }

    private static String firstNonBlank(String... s) {
        for (String x : s) if (x != null && !x.isBlank()) return x;
        return "";
    }

    private static String normalize(String phone) {
        String p = phone.replaceAll("[^0-9+]", "");
        return p.startsWith("+") ? p : "+" + p;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private static String stripTags(String s) {
        return s.replaceAll("<[^>]+>", " ").replaceAll("\\s+", " ").trim();
    }

    private static String abbrev(String s) {
        return s.length() <= 200 ? s : s.substring(0, 197) + "...";
    }
}
