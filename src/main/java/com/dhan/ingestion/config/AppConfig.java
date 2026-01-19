package com.dhan.ingestion.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import java.util.concurrent.Semaphore;

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
    public Semaphore dhanApiSemaphore(
            @Value("${dhan.api.inflight-limit:10}") int inflightLimit) {
        return new Semaphore(inflightLimit);
    }
}
