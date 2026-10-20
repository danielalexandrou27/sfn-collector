package com.hca.monitor.notify;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * monitor.notify in application.yml / config/local.yml. Provider credentials live here (keep them in
 * config/local.yml); which jobs notify whom, and the message text, live in config/notifications.yml.
 */
@ConfigurationProperties(prefix = "monitor.notify")
public record NotifyProperties(
        String rulesFile,
        boolean desktopEnabled,
        /** Shown as {{dashboard}} in messages, e.g. http://monitor-host:5173 */
        String dashboardUrl,
        String awsRegion,
        String timeZone,
        /** Step Functions failures: how long to wait for the failed step and real error before sending. */
        int failureDetailWaitSeconds,
        WhatsAppProvider whatsapp) {

    public enum Provider { WAHA, CALLMEBOT, TWILIO, META }

    public record WhatsAppProvider(
            Provider provider,
            /** Optional "host:port" of the company proxy, if outbound internet needs one (not used for WAHA on localhost). */
            String proxy,
            /** WAHA gateway (self-hosted, links your WhatsApp number), e.g. http://localhost:3000 */
            String wahaUrl,
            String wahaApiKey,
            /** WAHA session name; the free edition uses "default". */
            String wahaSession,
            String twilioAccountSid,
            String twilioAuthToken,
            /** Twilio WhatsApp sender, e.g. +14155238886 (the sandbox number) */
            String twilioFrom,
            String metaPhoneNumberId,
            String metaAccessToken,
            String metaApiVersion) {}
}
