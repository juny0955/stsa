package dev.junyoung.stsa.collector.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("finnhub")
public record FinnhubProperties(
		@DefaultValue("https://finnhub.io/api/v1") String baseUrl,
		@DefaultValue("") String apiKey,
		@DefaultValue("10s") Duration requestTimeout) {

	public FinnhubProperties {
		if (requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()) {
			throw new IllegalArgumentException("finnhub.request-timeout must be positive");
		}
	}

	@Override
	public String toString() {
		return "FinnhubProperties[baseUrl=" + baseUrl + ", apiKey=<redacted>, requestTimeout="
				+ requestTimeout + "]";
	}
}
