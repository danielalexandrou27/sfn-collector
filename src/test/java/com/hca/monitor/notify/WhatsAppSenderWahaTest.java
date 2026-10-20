package com.hca.monitor.notify;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** WAHA provider against a fake gateway: request format, API key, group listing, errors. */
class WhatsAppSenderWahaTest {

    HttpServer server;
    String url;
    final List<String> requests = new CopyOnWriteArrayList<>();
    volatile String groups = "[{\"id\":{\"_serialized\":\"120363011111@g.us\"},\"name\":\"EDW Batch\"}]";

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            requests.add(ex.getRequestURI().getPath() + " " + ex.getRequestHeaders().getFirst("X-Api-Key") + " " + body);
            String path = ex.getRequestURI().getPath();
            int code = 200;
            String out;
            if (!"secret".equals(ex.getRequestHeaders().getFirst("X-Api-Key"))) { code = 401; out = "{}"; }
            else if (path.equals("/api/sessions/default")) out = "{\"status\":\"WORKING\"}";
            else if (path.equals("/api/default/groups")) out = groups;
            else if (path.equals("/api/sendText")) { code = 201; out = "{}"; }
            else { code = 404; out = "{}"; }
            byte[] b = out.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(code, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
        url = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    WhatsAppSender sender(String key) {
        return new WhatsAppSender(new NotifyProperties.WhatsAppProvider(NotifyProperties.Provider.WAHA, null, url, key,
                null, null, null, null, null, null, null));
    }

    @Test
    void sendsToGroupsAndPhonesWithApiKey() throws Exception {
        WhatsAppSender s = sender("secret");
        assertThat(s.wahaStatus()).isEqualTo("WORKING");
        s.send(new NotificationRules.Recipient(null, "120363011111@g.us", null), "❌ prod-Application failed");
        s.send(new NotificationRules.Recipient("+1 (714) 555-0123", null, null), "hi");
        assertThat(requests).anyMatch(r -> r.startsWith("/api/sendText secret") && r.contains("\"chatId\":\"120363011111@g.us\"")
                && r.contains("\"session\":\"default\"") && r.contains("❌ prod-Application failed"));
        assertThat(requests).anyMatch(r -> r.contains("\"chatId\":\"17145550123@c.us\""));
    }

    @Test
    void listsGroupsInEitherResponseShape() throws Exception {
        assertThat(sender("secret").wahaGroups()).containsEntry("EDW Batch", "120363011111@g.us");
        groups = "{\"120363033333@g.us\":{\"Name\":\"Ops\"}}";
        assertThat(sender("secret").wahaGroups()).containsEntry("Ops", "120363033333@g.us");
    }

    @Test
    void reportsWrongKeyAndGatewayDown() {
        assertThat(sender("wrong").wahaStatus()).startsWith("UNAUTHORIZED");
        server.stop(0);
        assertThatThrownBy(() -> sender("secret").send(new NotificationRules.Recipient("+17145550123", null, null), "x"))
                .hasMessageContaining("WAHA is not reachable");
    }
}
