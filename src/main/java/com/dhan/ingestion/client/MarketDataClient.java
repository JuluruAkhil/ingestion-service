package com.dhan.ingestion.client;

import com.dhan.ingestion.domain.OhlcData;
import com.dhan.ingestion.domain.Ticker;

import java.time.LocalDateTime;
import java.util.List;

public interface MarketDataClient {
    List<OhlcData> fetchOhlc(Ticker ticker, LocalDateTime from, LocalDateTime to);
}
