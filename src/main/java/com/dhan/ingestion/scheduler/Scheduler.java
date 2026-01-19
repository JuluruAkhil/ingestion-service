package com.dhan.ingestion.scheduler;

import com.dhan.ingestion.domain.MarketStatus;
import com.dhan.ingestion.domain.Ticker;
import com.dhan.ingestion.repository.TickerRepository;
import com.dhan.ingestion.service.IngestionService;
import com.dhan.ingestion.service.MarketStatusService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class Scheduler {

    private final MarketStatusService marketStatusService;
    private final TickerRepository tickerRepository;
    private final IngestionService ingestionService;
    private final AtomicBoolean jobRunning = new AtomicBoolean(false);

    @Value("${ingestion.scheduler.stale-threshold-minutes:5}")
    private int staleThresholdMinutes;

    @Scheduled(cron = "${ingestion.scheduler.cron}", zone = "UTC")
    public void runIngestionJob() {
        if (!jobRunning.compareAndSet(false, true)) {
            log.warn("Previous ingestion job still running. Skipping this run.");
            return;
        }
        try {
            log.info("Starting scheduled ingestion job...");

            // 1. Check Bellwether
            MarketStatus marketStatus = marketStatusService.getMarketStatus();
            if (marketStatus != MarketStatus.ACTIVE) {
                log.info("Market status is {}. Skipping full sync.", marketStatus);
                return;
            }
            LocalDateTime bellwetherTime = marketStatusService.getLastBellwetherTime();
            if (bellwetherTime == null) {
                log.info("Bellwether time unavailable. Skipping full sync.");
                return;
            }

            // 2. Identify Stale Tickers
            List<Ticker> allTickers = tickerRepository.findAllActive();
            LocalDateTime threshold = bellwetherTime.minusMinutes(staleThresholdMinutes);

            List<Ticker> staleTickers = allTickers.stream()
                    .filter(t -> t.getLastFetchedTime() != null && t.getLastFetchedTime().isBefore(threshold))
                    .collect(Collectors.toList());

            if (staleTickers.isEmpty()) {
                log.info("All tickers are up to date.");
                return;
            }

            log.info("Found {} stale tickers. Triggering ingestion...", staleTickers.size());

            // 3. Process Parallel
            ingestionService.processTickersParallel(staleTickers, bellwetherTime);
        } catch (Exception e) {
            log.error("Critical error in ingestion scheduler", e);
        } finally {
            jobRunning.set(false);
        }
    }
}
