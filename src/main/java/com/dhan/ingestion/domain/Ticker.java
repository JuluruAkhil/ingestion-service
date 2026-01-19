package com.dhan.ingestion.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Ticker {
    private String symbol; // e.g., "NSE_EQ_INFY"
    private String securityId;
    private String exchangeSegment;
    private String instrumentType;
    private LocalDateTime lastFetchedTime;
    private Boolean isActive;
    private LocalDateTime updatedAt;
}
