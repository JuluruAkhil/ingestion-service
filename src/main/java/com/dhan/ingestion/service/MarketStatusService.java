package com.dhan.ingestion.service;

import com.dhan.ingestion.client.MarketDataClient;
import com.dhan.ingestion.domain.MarketStatus;
import com.dhan.ingestion.domain.OhlcData;
import com.dhan.ingestion.domain.Ticker;
import com.dhan.ingestion.repository.TickerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class MarketStatusService {

    private final TickerRepository tickerRepository;
    private final MarketDataClient marketDataClient;

    private volatile LocalDateTime lastBellwetherTime;

    @Value("${ingestion.market.bellwether-symbol}")
    private String bellwetherSymbolRaw; // "IDX_I_13"

    @Value("${ingestion.market.bellwether-fresh-minutes:5}")
    private int bellwetherFreshMinutes;

    @Value("${ingestion.market.bellwether-window-days:7}")
    private int bellwetherWindowDays;

    @Value("${ingestion.market.update-bellwether-cursor:true}")
    private boolean updateBellwetherCursor;

    /**
     * Checks if the market is active by pinging the Bellwether (Nifty 50).
     */
    public MarketStatus getMarketStatus() {
        String symbol = bellwetherSymbolRaw;
        LocalDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).toLocalDateTime();

        try {
            LocalDateTime windowStart = now.minusDays(bellwetherWindowDays);
            log.info("Checking Bellwether {} for data since {} (window {} days)", symbol, windowStart, bellwetherWindowDays);

            Ticker bellwether = tickerRepository.findBySymbol(symbol)
                    .orElseThrow(() -> new RuntimeException("Bellwether symbol " + symbol + " not found in DB! Check CSV load."));

            List<OhlcData> data = marketDataClient.fetchOhlc(bellwether, windowStart, now);

            if (data.isEmpty()) {
                log.info("Bellwether returned no data. Market likely closed.");
                return MarketStatus.CLOSED;
            }

            LocalDateTime newMax = data.getLast().getTime();
            lastBellwetherTime = newMax;
            log.info("Bellwether data available. Latest: {}", newMax);

            if (updateBellwetherCursor) {
                tickerRepository.updateTickerCursor(symbol, newMax);
            } else {
                log.info("Skipping bellwether cursor update for {} (config disabled)", symbol);
            }

            return MarketStatus.ACTIVE;
        } catch (Exception e) {
            log.error("Error checking market status", e);
            return MarketStatus.ERROR;
        }
    }

    public LocalDateTime getLastBellwetherTime() {
        return lastBellwetherTime;
    }
}
