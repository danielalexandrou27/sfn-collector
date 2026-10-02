package com.hca.monitor.collector.stepfunctions;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "monitor.sqs")
public record SqsCollectorProperties(
        String queueUrl,
        String region,
        int waitTimeSeconds,
        int maxMessages,
        int visibilityTimeoutSeconds,
        int credentialErrorBackoffSeconds,
        boolean enabled) {
}
