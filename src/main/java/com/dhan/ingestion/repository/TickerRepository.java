package com.dhan.ingestion.repository;

import com.dhan.ingestion.domain.Ticker;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository
@Slf4j
public class TickerRepository {

    private static final DateTimeFormatter CLICKHOUSE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final RestClient clickhouseRestClient;
    private final ObjectMapper objectMapper;

    @Value("${clickhouse.http.database:default}")
    private String database;

    @Value("${clickhouse.http.user:}")
    private String clickhouseUser;

    @Value("${clickhouse.http.password:}")
    private String clickhousePassword;

    public TickerRepository(@Qualifier("clickhouseRestClient") RestClient clickhouseRestClient) {
        this.clickhouseRestClient = clickhouseRestClient;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        var javaTimeModule = new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule();
        var formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        javaTimeModule.addDeserializer(java.time.LocalDateTime.class,
                new com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer(formatter));
        javaTimeModule.addSerializer(java.time.LocalDateTime.class,
                new com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer(formatter));
        this.objectMapper.registerModule(javaTimeModule);
    }

    public void updateTickerCursor(String symbol, LocalDateTime lastFetchedTime) {
        LocalDateTime cursorTime = lastFetchedTime.truncatedTo(ChronoUnit.SECONDS);
        LocalDateTime updatedAt = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        String cursorLiteral = cursorTime.format(CLICKHOUSE_TIME_FORMATTER);
        String updatedLiteral = updatedAt.format(CLICKHOUSE_TIME_FORMATTER);
        String query = "ALTER TABLE " + database
                + ".tickers UPDATE last_fetched_time = toDateTime('" + cursorLiteral + "'), updated_at = toDateTime('" + updatedLiteral + "') WHERE symbol = '" + escapeSqlString(symbol) + "'";
        try {
            RestClient.RequestHeadersSpec<?> request = clickhouseRestClient.post()
                    .uri(uriBuilder -> uriBuilder.path("/")
                            .queryParam("query", query)
                            .build());
            if (!clickhouseUser.isBlank()) {
                request = request.headers(headers -> headers.setBasicAuth(clickhouseUser, clickhousePassword));
            }
            request.retrieve().toBodilessEntity();
        } catch (org.springframework.web.client.RestClientResponseException ex) {
            log.error("Failed to update ticker cursor for {} (status={}): {}", symbol, ex.getStatusCode(), ex.getResponseBodyAsString());
        } catch (Exception ex) {
            log.error("Failed to update ticker cursor for {}", symbol, ex);
        }
    }

    public Optional<Ticker> findBySymbol(String symbol) {
        String query = "SELECT symbol, security_id, exchange_segment, instrument_type, last_fetched_time, is_active, updated_at "
                + "FROM " + database + ".tickers WHERE symbol = '" + escapeSqlString(symbol) + "' LIMIT 1 FORMAT JSONEachRow";
        List<Ticker> result = fetchTickers(query);
        return result.isEmpty() ? Optional.empty() : Optional.of(result.getFirst());
    }

    public List<Ticker> findAllActive() {
        String query = "SELECT symbol, security_id, exchange_segment, instrument_type, last_fetched_time, is_active, updated_at "
                + "FROM " + database + ".tickers WHERE is_active = 1 "
                + "ORDER BY updated_at DESC LIMIT 1 BY symbol FORMAT JSONEachRow";
        return fetchTickers(query);
    }

    private List<Ticker> fetchTickers(String query) {
        RestClient.RequestHeadersSpec<?> request = clickhouseRestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/")
                        .queryParam("query", query)
                        .build());
        if (!clickhouseUser.isBlank()) {
            request = request.headers(headers -> headers.setBasicAuth(clickhouseUser, clickhousePassword));
        }
        String response;
        try {
            response = request.retrieve().body(String.class);
        } catch (org.springframework.web.client.RestClientResponseException ex) {
            log.error("Failed to fetch tickers (status={}): {}", ex.getStatusCode(), ex.getResponseBodyAsString());
            return List.of();
        } catch (Exception ex) {
            log.error("Failed to fetch tickers", ex);
            return List.of();
        }
        if (response == null || response.isBlank()) {
            return List.of();
        }
        String[] rows = response.trim().split("\\R");
        List<Ticker> result = new ArrayList<>(rows.length);
        for (String row : rows) {
            Ticker ticker = parseTickerJson(row);
            if (ticker != null) {
                result.add(ticker);
            }
        }
        return result;
    }

    private Ticker parseTickerJson(String row) {
        try {
            return objectMapper.readValue(row, Ticker.class);
        } catch (Exception e) {
            log.warn("Failed to parse ticker row: {}", row, e);
            return null;
        }
    }

    private String escapeSqlString(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "''");
    }
}
