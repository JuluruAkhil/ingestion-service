package com.dhan.ingestion.service;

import com.dhan.ingestion.client.MarketDataClient;
import com.dhan.ingestion.domain.OhlcData;
import com.dhan.ingestion.domain.Ticker;
import com.dhan.ingestion.repository.OhlcRepository;
import com.dhan.ingestion.repository.TickerRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;


@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionService {
    private final TickerRepository tickerRepository;
    private final OhlcRepository ohlcRepository;
    private final MarketDataClient marketDataClient;

    @Value("${ingestion.history.start-date}")
    private String defaultStartDateStr;

    @Value("${ingestion.concurrent.max-tasks:200}")
    private int maxConcurrentTasks;

    @Value("${ingestion.history.max-window-days:89}")
    private int maxWindowDays;

    private ExecutorService executor;
    private Semaphore concurrencyLimit;
    private LocalDateTime defaultStartDate;
    private int maxWindowMinutes;
    private Set<String> inFlightSymbols;

    @PostConstruct
    public void init() {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.concurrencyLimit = new Semaphore(maxConcurrentTasks);
        this.inFlightSymbols = ConcurrentHashMap.newKeySet();
        LocalDate parsed = LocalDate.parse(defaultStartDateStr);
        this.defaultStartDate = parsed.atStartOfDay();
        this.maxWindowMinutes = maxWindowDays * 24 * 60;
    }

    @PreDestroy
    public void shutdown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    public void processTickersParallel(List<Ticker> tickers, LocalDateTime endTime) {
        log.info("Starting parallel sync for {} tickers...", tickers.size());

        for (Ticker ticker : tickers) {
            executor.submit(() -> {
                boolean acquired = false;
                try {
                    concurrencyLimit.acquire();
                    acquired = true;
                    syncTicker(ticker, endTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted while waiting to schedule {}", ticker.getSymbol());
                } finally {
                    if (acquired) {
                        concurrencyLimit.release();
                    }
                }
            });
        }
    }

    private void syncTicker(Ticker ticker, LocalDateTime endTime) {
        String symbol = ticker.getSymbol();
        if (!inFlightSymbols.add(symbol)) {
            log.info("Skipping {} since a sync is already running", symbol);
            return;
        }

        try {
            LocalDateTime baseTime = ticker.getLastFetchedTime();
            if (baseTime == null) {
                baseTime = defaultStartDate;
            }

            LocalDateTime start = baseTime.plusMinutes(1);
            LocalDateTime end = endTime;

            if (!start.isBefore(end)) {
                log.debug("Skipping {} since cursor {} is not before end {}", symbol, start, end);
                return;
            }

            LocalDateTime windowStart = start;
            while (windowStart.isBefore(end)) {
                LocalDateTime windowEnd = windowStart.plusMinutes(maxWindowMinutes);
                if (windowEnd.isAfter(end)) {
                    windowEnd = end;
                }

                if (!windowStart.isBefore(windowEnd)) {
                    log.debug("Skipping {} since window start {} is not before end {}", symbol, windowStart, windowEnd);
                    break;
                }

                log.info("Fetching {} window {} -> {}", symbol, windowStart, windowEnd);

                List<OhlcData> data = marketDataClient.fetchOhlc(ticker, windowStart, windowEnd);

                if (!data.isEmpty()) {
                    ohlcRepository.batchInsertOhlc(data);
                    LocalDateTime lastTime = data.getLast().getTime();
                    tickerRepository.updateTickerCursor(symbol, lastTime);
                }

                windowStart = windowEnd;
            }

        } catch (Exception e) {
            log.error("Failed to sync {}", symbol, e);
        } finally {
            inFlightSymbols.remove(symbol);
        }
    }
}
