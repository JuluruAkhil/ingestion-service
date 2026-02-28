package com.dhan.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;

import com.dhan.ingestion.domain.MarketStatus;
import com.dhan.ingestion.domain.OhlcData;
import com.dhan.ingestion.domain.Ticker;

@SpringBootApplication
@EnableScheduling
@RegisterReflectionForBinding({MarketStatus.class, OhlcData.class, Ticker.class})
public class IngestionApplication {

	public static void main(String[] args) {
		SpringApplication.run(IngestionApplication.class, args);
	}

}
