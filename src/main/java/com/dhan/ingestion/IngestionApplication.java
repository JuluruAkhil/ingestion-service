package com.dhan.ingestion;

import io.github.cdimascio.dotenv.Dotenv;
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
		// Load .env file
		try {
			Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
			dotenv.entries().forEach(entry -> {
				System.setProperty(entry.getKey(), entry.getValue());
			});
		} catch (Exception e) {
			// Ignore if .env is missing (e.g. in production using real env vars)
		}

		SpringApplication.run(IngestionApplication.class, args);
	}

}
