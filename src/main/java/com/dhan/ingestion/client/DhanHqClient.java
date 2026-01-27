package com.dhan.ingestion.client;

import com.dhan.ingestion.domain.OhlcData;
import com.dhan.ingestion.domain.Ticker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import com.dhan.ingestion.config.AccessTokenStore;

@Service
@Slf4j
public class DhanHqClient implements MarketDataClient {

    private final RestClient dhanRestClient;
    private final Semaphore apiSemaphore;
    private final String baseUrl;
    private final AccessTokenStore accessTokenStore;

    public DhanHqClient(@Qualifier("dhanRestClient") RestClient dhanRestClient,
                        Semaphore dhanApiSemaphore,
                        @Value("${dhan.api.base-url}") String baseUrl,
                        AccessTokenStore accessTokenStore) {
        this.dhanRestClient = dhanRestClient;
        this.apiSemaphore = dhanApiSemaphore;
        this.baseUrl = baseUrl;
        this.accessTokenStore = accessTokenStore;
    }

    @Override
    public List<OhlcData> fetchOhlc(Ticker ticker, LocalDateTime from, LocalDateTime to) {
        if (!from.isBefore(to)) {
            return Collections.emptyList();
        }

        return fetchOhlc1m(ticker, from, to);
    }

    private List<OhlcData> fetchOhlc1m(Ticker ticker, LocalDateTime from, LocalDateTime to) {
        // DhanHQ expects YYYY-MM-DD HH:mm:ss
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromDate = from.format(formatter);
        String toDate = to.format(formatter);

        Map<String, Object> payload = Map.of(
                "securityId", String.valueOf(ticker.getSecurityId()),
                "exchangeSegment", String.valueOf(ticker.getExchangeSegment()),
                "instrument", String.valueOf(ticker.getInstrumentType()),
                "interval", "1",
                "oi", false,
                "fromDate", fromDate,
                "toDate", toDate
        );

        boolean acquired = false;
        try {
            apiSemaphore.acquire();
            acquired = true;

            String url = baseUrl + "/charts/intraday";

            for (int attempt = 1; attempt <= 3; attempt++) {
                try {
                    String token = accessTokenStore.getAccessToken();
                    if (token == null || token.isBlank()) {
                        log.error("Missing DhanHQ access token; unable to fetch data for {}", ticker.getSymbol());
                        return Collections.emptyList();
                    }

                    Map<String, Object> response = dhanRestClient.post()
                            .uri(url)
                            .header("access-token", token)
                            .accept(MediaType.APPLICATION_JSON)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(payload)
                            .retrieve()
                            .body(new ParameterizedTypeReference<Map<String, Object>>() {});

                    if (response == null || !response.containsKey("timestamp")) {
                        return Collections.emptyList();
                    }

                    return parseResponse(ticker.getSymbol(), response);
                } catch (RestClientResponseException e) {
                    if (shouldRetryDhanError(e, attempt) || isRateLimited(e)) {
                        long delayMs = attempt == 1 ? 5_000L : 10_000L;
                        log.warn("Retrying DhanHQ request for {} {} -> {} (attempt {}/3 after {} ms)",
                                ticker.getSymbol(), fromDate, toDate, attempt + 1, delayMs);
                        try {
                            Thread.sleep(delayMs);
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                            return Collections.emptyList();
                        }
                        continue;
                    }
                    if (e.getStatusCode().value() == 400) {
                        e.getResponseBodyAsString();
                        if (e.getResponseBodyAsString().contains("DH-905")) {
                            log.warn("DhanHQ returned DH-905 for {} {} -> {}", ticker.getSymbol(), fromDate, toDate);
                            return Collections.emptyList();
                        }
                    }
                    log.error("Error fetching data for {} {} -> {}", ticker.getSymbol(), fromDate, toDate, e);
                    return Collections.emptyList();
                } catch (Exception e) {
                    log.error("Error fetching data for {} {} -> {}", ticker.getSymbol(), fromDate, toDate, e);
                    return Collections.emptyList();
                }
            }

            return Collections.emptyList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for DhanHQ API slot for {}", ticker.getSymbol());
            return Collections.emptyList();
        } finally {
            if (acquired) {
                apiSemaphore.release();
            }
        }
    }

    private List<OhlcData> parseResponse(String symbol, Map<String, Object> data) {
        List<?> timestamps = requireList(data, "timestamp");
        List<?> open = requireList(data, "open");
        List<?> high = requireList(data, "high");
        List<?> low = requireList(data, "low");
        List<?> close = requireList(data, "close");
        List<?> volume = requireList(data, "volume");

        int size = timestamps.size();
        if (open.size() != size || high.size() != size || low.size() != size || close.size() != size || volume.size() != size) {
            throw new IllegalStateException("Mismatched intraday payload sizes for " + symbol + ": timestamp=" + size
                    + ", open=" + open.size()
                    + ", high=" + high.size()
                    + ", low=" + low.size()
                    + ", close=" + close.size()
                    + ", volume=" + volume.size());
        }

        List<OhlcData> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            long tsSeconds = requireEpochSeconds(symbol, i, timestamps.get(i));
            LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(tsSeconds), ZoneId.of("Asia/Kolkata"));

            BigDecimal openVal = BigDecimal.valueOf(requireNumber(symbol, "open", i, open.get(i)).doubleValue());
            BigDecimal highVal = BigDecimal.valueOf(requireNumber(symbol, "high", i, high.get(i)).doubleValue());
            BigDecimal lowVal = BigDecimal.valueOf(requireNumber(symbol, "low", i, low.get(i)).doubleValue());
            BigDecimal closeVal = BigDecimal.valueOf(requireNumber(symbol, "close", i, close.get(i)).doubleValue());
            long vol = requireNumber(symbol, "volume", i, volume.get(i)).longValue();
            if (vol < 0) {
                vol = 0;
            }

            result.add(OhlcData.builder()
                    .sym(symbol)
                    .open(openVal)
                    .high(highVal)
                    .low(lowVal)
                    .close(closeVal)
                    .volume(vol)
                    .time(time)
                    .build());
        }
        return result;
    }

    private List<?> requireList(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof List<?> list) {
            return list;
        }
        throw new IllegalStateException("Missing or invalid intraday payload field: " + key + " sizes=" + describePayloadSizes(data));
    }

    private String describePayloadSizes(Map<String, Object> data) {
        return "timestamp=" + sizeOf(data.get("timestamp"))
                + ", open=" + sizeOf(data.get("open"))
                + ", high=" + sizeOf(data.get("high"))
                + ", low=" + sizeOf(data.get("low"))
                + ", close=" + sizeOf(data.get("close"))
                + ", volume=" + sizeOf(data.get("volume"));
    }

    private boolean isNoDataError(String responseBody) {
        if (responseBody == null || responseBody.isBlank()) {
            return false;
        }
        return responseBody.contains("no data")
                || responseBody.contains("No data")
                || responseBody.contains("incorrect parameters");
    }

    private boolean shouldRetryDhanError(RestClientResponseException e, int attempt) {
        if (attempt >= 3) {
            return false;
        }
        if (e.getStatusCode().value() != 400) {
            return false;
        }
        String body = e.getResponseBodyAsString();
        return body.contains("DH-905");
    }

    private boolean isRateLimited(RestClientResponseException e) {
        int status = e.getStatusCode().value();
        if (status == 429) {
            return true;
        }
        String body = e.getResponseBodyAsString();
        if (body.isBlank()) {
            return false;
        }
        return body.contains("rate limit") || body.contains("too many requests");
    }

    private String sizeOf(Object value) {
        if (value instanceof List<?> list) {
            return String.valueOf(list.size());
        }
        return "?";
    }

    private Number requireNumber(String symbol, String field, int index, Object value) {
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalStateException("Invalid intraday " + field + " at index " + index + " for " + symbol + ": " + value);
    }

    private long requireEpochSeconds(String symbol, int index, Object value) {
        Number number = requireNumber(symbol, "timestamp", index, value);
        long tsSeconds = number.longValue();
        if (tsSeconds <= 0 || tsSeconds > 10_000_000_000L) {
            throw new IllegalStateException("Invalid epoch seconds timestamp at index " + index + " for " + symbol + ": " + value);
        }
        return tsSeconds;
    }
}
