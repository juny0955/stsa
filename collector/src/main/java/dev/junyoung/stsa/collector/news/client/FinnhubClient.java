package dev.junyoung.stsa.collector.news.client;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import dev.junyoung.stsa.collector.config.FinnhubProperties;
import dev.junyoung.stsa.collector.news.client.dto.FinnhubResponse;
import dev.junyoung.stsa.collector.news.domain.CompanyNews;
import dev.junyoung.stsa.collector.news.domain.NewsFetchResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class FinnhubClient {

	private static final List<String> SYMBOLS = List.of("NVDA", "AAPL", "MSFT");

	private final WebClient webClient;
	private final FinnhubProperties properties;

	public FinnhubClient(WebClient.Builder builder, FinnhubProperties properties) {
		this.properties = properties;
		this.webClient = builder.clone()
				.baseUrl(properties.baseUrl())
				.defaultHeader("X-Finnhub-Token", properties.apiKey())
				.build();
	}

	public Flux<NewsFetchResult> fetchCompanyNews(LocalDate from, LocalDate to) {
		Objects.requireNonNull(from, "from must not be null");
		Objects.requireNonNull(to, "to must not be null");
		if (from.isAfter(to)) {
			throw new IllegalArgumentException("from must be on or before to");
		}

		return Flux.defer(() -> {
			if (properties.apiKey().isBlank()) {
				return Flux.error(new IllegalStateException("FINNHUB_API_KEY must be configured"));
			}

			return Flux.fromIterable(SYMBOLS)
				.flatMap(symbol -> webClient.get()
					.uri(uriBuilder -> uriBuilder.path("/company-news")
						.queryParam("symbol", symbol)
						.queryParam("from", from)
						.queryParam("to", to)
						.build())
					.accept(MediaType.APPLICATION_JSON)
					.retrieve()
					.bodyToFlux(FinnhubResponse.class)
					.collectList()
					.timeout(properties.requestTimeout())
					.<NewsFetchResult>map(articles ->
							new NewsFetchResult.Success(new CompanyNews(symbol, articles)))
					.onErrorResume(error -> Mono.just(new NewsFetchResult.Failure(symbol, error))), 3);
		});
	}
}
