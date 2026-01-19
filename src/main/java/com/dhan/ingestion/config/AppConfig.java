package com.dhan.ingestion.config;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class AppConfig {

    @Bean
    public RestClient dhanRestClient(RestClient.Builder builder) {
        return builder.build();
    }

    @Bean
    public RestClient clickhouseRestClient(RestClient.Builder builder,
                                           @Value("${clickhouse.http.base-url}") String clickhouseBaseUrl) {
        return builder
                .baseUrl(clickhouseBaseUrl)
                .build();
    }

    @Bean
    public RateLimiter dhanRateLimiter() {
        // DhanHQ limit: Let's be safe with 10 requests per second.
        RateLimiterConfig config = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .limitForPeriod(10)
                .timeoutDuration(Duration.ofSeconds(5)) // Wait up to 5s for a permit
                .build();

        RateLimiterRegistry registry = RateLimiterRegistry.of(config);
        return registry.rateLimiter("dhan-api");
    }
}
