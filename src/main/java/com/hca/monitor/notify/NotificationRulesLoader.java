package com.hca.monitor.notify;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;

/** Loads config/notifications.yml and reloads it within 30 s of a save. A bad edit keeps the last good version. */
@Component
public class NotificationRulesLoader {

    private static final Logger log = LoggerFactory.getLogger(NotificationRulesLoader.class);

    private final Path file;
    private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    private volatile NotificationRules.File rules;
    private volatile FileTime loadedVersion;
    private volatile boolean missingLogged;

    public NotificationRulesLoader(NotifyProperties props) {
        this.file = Path.of(props.rulesFile());
        reloadIfChanged();
    }

    /** Current rules; null if there is no file (desktop pop-ups then use their defaults, no WhatsApp). */
    public NotificationRules.File rules() {
        return rules;
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void reloadIfChanged() {
        try {
            if (!Files.exists(file)) {
                if (!missingLogged) {
                    missingLogged = true;
                    log.info("No notification rules at {} - desktop pop-ups use defaults, WhatsApp is off. "
                            + "See config/notifications.example.yml.", file.toAbsolutePath());
                }
                rules = null;
                loadedVersion = null;
                return;
            }
            missingLogged = false;
            FileTime modified = Files.getLastModifiedTime(file);
            if (modified.equals(loadedVersion)) return;
            loadedVersion = modified;

            NotificationRules.File parsed = yaml.readValue(file.toFile(), NotificationRules.File.class);
            List<String> problems = NotificationRules.problems(parsed);
            if (!problems.isEmpty()) {
                log.error("NOTIFICATIONS_INVALID: {} has {} problem(s) - keeping the previous version:\n  - {}",
                        file.toAbsolutePath(), problems.size(), String.join("\n  - ", problems));
                return;
            }
            rules = parsed;
            int jobs = parsed.jobs() == null ? 0 : parsed.jobs().size();
            int people = parsed.whatsapp() == null || parsed.whatsapp().recipients() == null ? 0 : parsed.whatsapp().recipients().size();
            log.info("NOTIFICATIONS_LOADED: {} job rule(s), {} WhatsApp recipient(s) from {}", jobs, people, file.toAbsolutePath());
        } catch (Exception e) {
            log.error("NOTIFICATIONS_INVALID: could not read {} - keeping the previous version: {}",
                    file.toAbsolutePath(), e.getMessage());
        }
    }
}
