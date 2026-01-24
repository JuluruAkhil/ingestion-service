package com.dhan.ingestion.service;

import com.dhan.ingestion.config.AccessTokenStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class DhanTokenRefresher implements ApplicationRunner {
    private final RestClient dhanRestClient;
    private final AccessTokenStore accessTokenStore;
    private final ConfigurableApplicationContext context;
    private final String baseUrl;
    private final String clientId;
    private final AtomicBoolean refreshInProgress = new AtomicBoolean(false);

    public DhanTokenRefresher(@Qualifier("dhanRestClient") RestClient dhanRestClient,
                              AccessTokenStore accessTokenStore,
                              ConfigurableApplicationContext context,
                              @Value("${dhan.api.base-url}") String baseUrl,
                              @Value("${dhan.api.client-id}") String clientId) {
        this.dhanRestClient = dhanRestClient;
        this.accessTokenStore = accessTokenStore;
        this.context = context;
        this.baseUrl = baseUrl;
        this.clientId = clientId;
    }

    @Override
    public void run(ApplicationArguments args) {
        scheduledRenew();
    }

    @Scheduled(fixedDelay = 43_200_000L, initialDelay = 43_200_000L)
    public void scheduledRenew() {
        if (!refreshInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            renewTokenOrStop();
        } finally {
            refreshInProgress.set(false);
        }
    }

    private void renewTokenOrStop() {
        try {
            String token = accessTokenStore.getAccessToken();
            if (token == null || token.isBlank()) {
                stopService("Missing DhanHQ access token");
                return;
            }

            ResponseEntity<Map<String, Object>> response = dhanRestClient.get()
                    .uri(baseUrl + "/RenewToken")
                    .header("access-token", token)
                    .header("dhanClientId", clientId)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .toEntity(new ParameterizedTypeReference<Map<String, Object>>() {});

            if (response.getStatusCode().value() != 200) {
                stopService("DhanHQ RenewToken returned status=" + response.getStatusCode().value());
                return;
            }

            Map<String, Object> body = response.getBody();
            String newToken = extractAccessToken(body);
            if (newToken == null || newToken.isBlank()) {
                stopService("DhanHQ RenewToken response missing access token");
                return;
            }

            accessTokenStore.setAccessToken(newToken);
            System.setProperty("ACCESS_TOKEN", newToken);
            log.info("DhanHQ access token refreshed");
        } catch (RestClientResponseException e) {
            stopService("DhanHQ RenewToken failed (status=" + e.getStatusCode().value() + ")");
        } catch (Exception e) {
            stopService("DhanHQ RenewToken failed (" + e.getClass().getSimpleName() + ")");
        }
    }

    private String extractAccessToken(Map<String, Object> body) {
        if (body == null) {
            return null;
        }
        Object value = body.get("accessToken");
        if (value == null) {
            value = body.get("access-token");
        }
        if (value == null) {
            value = body.get("token");
        }
        return value == null ? null : String.valueOf(value);
    }

    private void stopService(String reason) {
        log.error("{}; you need to update token from https://web.dhan.co/index/profile", reason);
        int code = SpringApplication.exit(context, () -> 1);
        System.exit(code);
    }

}
