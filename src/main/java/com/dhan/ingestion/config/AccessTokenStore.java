package com.dhan.ingestion.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component
public class AccessTokenStore {
    private final AtomicReference<String> accessToken;

    public AccessTokenStore(@Value("${dhan.api.access-token:}") String initialToken) {
        this.accessToken = new AtomicReference<>(sanitize(initialToken));
    }

    public String getAccessToken() {
        return accessToken.get();
    }

    public void setAccessToken(String token) {
        accessToken.set(sanitize(token));
    }

    private String sanitize(String token) {
        if (token == null) {
            return null;
        }
        String trimmed = token.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
