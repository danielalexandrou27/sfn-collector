package com.hca.monitor.collector.stepfunctions;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

@Configuration
public class AwsConfig {

    /**
     * Default credential chain: on the laptop it uses AWS_PROFILE (your SSO profile);
     * on EC2 it uses the instance role. No code change between the two.
     */
    @Bean(destroyMethod = "close")
    public SqsClient sqsClient(SqsCollectorProperties props) {
        return SqsClient.builder()
                .region(Region.of(props.region()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

}
