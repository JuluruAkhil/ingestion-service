package com.dhan.ingestion.repository;

import com.dhan.ingestion.domain.OhlcData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Repository
@Slf4j
public class OhlcRepository {

    private static final DateTimeFormatter CLICKHOUSE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern CLICKHOUSE_ROW_PATTERN = Pattern.compile("at row (\\d+)");

    private final RestClient clickhouseRestClient;
    private final ObjectMapper objectMapper;

    @Value("${clickhouse.http.database:default}")
    private String database;

    @Value("${clickhouse.http.user:}")
    private String clickhouseUser;

    @Value("${clickhouse.http.password:}")
    private String clickhousePassword;

    public OhlcRepository(@Qualifier("clickhouseRestClient") RestClient clickhouseRestClient) {
        this.clickhouseRestClient = clickhouseRestClient;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN);
    }

    public void batchInsertOhlc(List<OhlcData> data) {
        if (data.isEmpty()) {
            return;
        }

        insertBatch(data);
    }

    private void insertBatch(List<OhlcData> batch) {
        if (batch.isEmpty()) {
            return;
        }
        if (batch.size() == 1) {
            sendSingleRow(batch.getFirst());
            return;
        }
        if (sendBatch(batch)) {
            return;
        }
        int mid = batch.size() / 2;
        insertBatch(batch.subList(0, mid));
        insertBatch(batch.subList(mid, batch.size()));
    }

    public Optional<LocalDateTime> getLastOhlcTime(String symbol) {
        String query = "SELECT max(time) FROM " + database + ".dhan_ohlc WHERE sym = '" + escapeSqlString(symbol) + "'";
        try {
            RestClient.RequestHeadersSpec<?> request = clickhouseRestClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/")
                            .queryParam("query", query)
                            .build());
            if (!clickhouseUser.isBlank()) {
                request = request.headers(headers -> headers.setBasicAuth(clickhouseUser, clickhousePassword));
            }
            String response = request.retrieve().body(String.class);
            if (response == null) {
                return Optional.empty();
            }
            String trimmed = response.trim();
            if (trimmed.isEmpty() || trimmed.equals("\\N")) {
                return Optional.empty();
            }
            return Optional.of(LocalDateTime.parse(trimmed.replace(" ", "T")));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private String serializeRows(List<OhlcData> data) {
        List<String> rows = new ArrayList<>(data.size());
        for (OhlcData ohlc : data) {
            String row = serializeRow(ohlc);
            if (row != null) {
                rows.add(row);
            }
        }
        return String.join("\n", rows);
    }

    private boolean isValidJsonEachRowLine(String row) {
        if (row == null || row.isBlank()) {
            return false;
        }
        return row.indexOf('\u0000') < 0 && row.indexOf('\n') < 0 && row.indexOf('\r') < 0
                && row.startsWith("{") && row.endsWith("}");
    }

    private String extractSymbolHint(String row) {
        if (row == null) {
            return "?";
        }
        int symIndex = row.indexOf("\"sym\"");
        if (symIndex < 0) {
            return "?";
        }
        int colonIndex = row.indexOf(':', symIndex);
        if (colonIndex < 0) {
            return "?";
        }
        int quoteStart = row.indexOf('"', colonIndex + 1);
        if (quoteStart < 0) {
            return "?";
        }
        int quoteEnd = row.indexOf('"', quoteStart + 1);
        if (quoteEnd < 0) {
            return "?";
        }
        return row.substring(quoteStart + 1, quoteEnd);
    }

    private boolean handleClickhouseRowError(byte[] payloadBytes, org.springframework.web.client.RestClientResponseException ex) {
        String body = ex.getResponseBodyAsString();
        if (body == null) {
            return false;
        }
        Matcher matcher = CLICKHOUSE_ROW_PATTERN.matcher(body);
        if (!matcher.find()) {
            return false;
        }
        int rowIndex;
        try {
            rowIndex = Integer.parseInt(matcher.group(1));
        } catch (NumberFormatException ignored) {
            return false;
        }
        String payload = new String(payloadBytes, StandardCharsets.UTF_8);
        String[] lines = payload.split("\n", -1);
        if (rowIndex <= 0 || rowIndex > lines.length) {
            return false;
        }
        String badLine = lines[rowIndex - 1];
        String cleaned = badLine == null ? "" : badLine.replace("\u0000", "");
        if (!isValidJsonEachRowLine(cleaned)) {
            log.warn("Dropping bad row {} due to ClickHouse parse error (sym hint: {})", rowIndex, extractSymbolHint(cleaned));
            return true;
        }
        StringBuilder rebuilt = new StringBuilder(payload.length());
        for (int i = 0; i < lines.length; i++) {
            if (i == rowIndex - 1) {
                continue;
            }
            if (!rebuilt.isEmpty()) {
                rebuilt.append('\n');
            }
            rebuilt.append(lines[i]);
        }
        byte[] retryBytes = rebuilt.toString().getBytes(StandardCharsets.UTF_8);
        log.warn("Retrying ClickHouse insert after dropping row {}", rowIndex);
        try {
            String query = "INSERT INTO " + database + ".dhan_ohlc FORMAT JSONEachRow";
            RestClient.RequestBodySpec retryRequest = clickhouseRestClient.post()
                    .uri(uriBuilder -> uriBuilder.path("/")
                            .queryParam("query", query)
                            .build())
                    .contentType(org.springframework.http.MediaType.parseMediaType("application/json; charset=UTF-8"));
            if (!clickhouseUser.isBlank()) {
                retryRequest = retryRequest.headers(headers -> headers.setBasicAuth(clickhouseUser, clickhousePassword));
            }
            retryRequest.body(retryBytes)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (Exception retryEx) {
            log.error("Retry insert failed after dropping row {}", rowIndex, retryEx);
            return false;
        }
    }

    private boolean sendBatch(List<OhlcData> batch) {
        String payload = serializeRows(batch);
        if (payload == null || payload.isBlank()) {
            return true;
        }
        String[] lines = payload.split("\n");
        int lineNumber = 0;
        StringBuilder sanitized = new StringBuilder(payload.length());
        for (String line : lines) {
            lineNumber++;
            if (!isValidJsonEachRowLine(line)) {
                log.warn("Dropping invalid JSONEachRow line {} in batch (sym hint: {})", lineNumber, extractSymbolHint(line));
                continue;
            }
            if (!sanitized.isEmpty()) {
                sanitized.append('\n');
            }
            sanitized.append(line);
        }
        if (sanitized.isEmpty()) {
            return true;
        }
        byte[] payloadBytes = sanitized.toString().getBytes(StandardCharsets.UTF_8);
        String query = "INSERT INTO " + database + ".dhan_ohlc FORMAT JSONEachRow";
        try {
            RestClient.RequestBodySpec request = clickhouseRestClient.post()
                    .uri(uriBuilder -> uriBuilder.path("/")
                            .queryParam("query", query)
                            .build())
                    .contentType(org.springframework.http.MediaType.parseMediaType("application/json; charset=UTF-8"));
            if (!clickhouseUser.isBlank()) {
                request = request.headers(headers -> headers.setBasicAuth(clickhouseUser, clickhousePassword));
            }
            request.body(payloadBytes)
                    .retrieve()
                    .toBodilessEntity();
            log.debug("Inserted {} rows into dhan_ohlc", batch.size());
            return true;
        } catch (org.springframework.web.client.RestClientResponseException ex) {
            if (handleClickhouseRowError(payloadBytes, ex)) {
                return true;
            }
            log.error("Failed to insert {} rows into dhan_ohlc (status={}): {}", batch.size(), ex.getStatusCode(), ex.getResponseBodyAsString());
            return false;
        } catch (Exception ex) {
            log.error("Failed to insert {} rows into dhan_ohlc", batch.size(), ex);
            return false;
        }
    }

    private void sendSingleRow(OhlcData ohlc) {
        String row = serializeRow(ohlc);
        if (row == null || row.isBlank()) {
            return;
        }
        if (!isValidJsonEachRowLine(row)) {
            log.warn("Dropping invalid JSONEachRow line for {}", ohlc.getSym());
            return;
        }
        byte[] rowBytes = row.getBytes(StandardCharsets.UTF_8);
        String query = "INSERT INTO " + database + ".dhan_ohlc FORMAT JSONEachRow";
        try {
            RestClient.RequestBodySpec request = clickhouseRestClient.post()
                    .uri(uriBuilder -> uriBuilder.path("/")
                            .queryParam("query", query)
                            .build())
                    .contentType(org.springframework.http.MediaType.parseMediaType("application/json; charset=UTF-8"));
            if (!clickhouseUser.isBlank()) {
                request = request.headers(headers -> headers.setBasicAuth(clickhouseUser, clickhousePassword));
            }
            request.body(rowBytes)
                    .retrieve()
                    .toBodilessEntity();
        } catch (org.springframework.web.client.RestClientResponseException ex) {
            log.error("Failed to insert OHLC row for {} (status={}): {} | row={}", ohlc.getSym(), ex.getStatusCode(), ex.getResponseBodyAsString(), row);
        } catch (Exception ex) {
            log.error("Failed to insert OHLC row for {} | row={}", ohlc.getSym(), row, ex);
        }
    }

    private String serializeRow(OhlcData ohlc) {
        String time = ohlc.getTime() == null ? null : ohlc.getTime().format(CLICKHOUSE_TIME_FORMATTER);
        time = sanitizeString(time);
        String sym = sanitizeString(ohlc.getSym());
        if (sym == null || sym.isBlank() || time == null || time.isBlank()) {
            log.warn("Dropping OHLC row with missing fields: sym={} time={}", sym, time);
            return null;
        }
        BigDecimal open = toNumber(ohlc.getOpen());
        BigDecimal high = toNumber(ohlc.getHigh());
        BigDecimal low = toNumber(ohlc.getLow());
        BigDecimal close = toNumber(ohlc.getClose());
        Long volume = ohlc.getVolume();
        Long openInterest = ohlc.getOpenInterest();
        if (open == null || high == null || low == null || close == null || volume == null || openInterest == null) {
            log.warn("Dropping OHLC row with missing fields: sym={} time={} open={} high={} low={} close={} volume={} openInterest={}",
                    sym, time, open, high, low, close, volume, openInterest);
            return null;
        }
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("sym", sym);
        row.put("open", open);
        row.put("high", high);
        row.put("low", low);
        row.put("close", close);
        row.put("volume", volume);
        row.put("open_interest", openInterest);
        row.put("time", time);
        try {
            return objectMapper.writeValueAsString(row);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Failed to serialize OHLC row for {}", sym, e);
            return null;
        }
    }

    private BigDecimal toNumber(BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.stripTrailingZeros();
    }

    private String escapeSqlString(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "''");
    }

    private String sanitizeString(String value) {
        if (value == null) {
            return null;
        }
        return value.replace("\u0000", "").trim();
    }
}
