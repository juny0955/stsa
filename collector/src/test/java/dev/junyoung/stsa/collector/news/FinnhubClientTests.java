package dev.junyoung.stsa.collector.news;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.junyoung.stsa.collector.config.FinnhubProperties;
import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import dev.junyoung.stsa.collector.news.client.dto.FinnhubResponse;
import dev.junyoung.stsa.collector.news.domain.CompanyNews;

@DisplayName("Finnhub 뉴스 클라이언트")
class FinnhubClientTests {

	private static final LocalDate FROM = LocalDate.of(2026, 10, 27);
	private static final LocalDate TO = LocalDate.of(2026, 10, 28);
	private static final Duration TEST_TIMEOUT = Duration.ofSeconds(5);

	private final BlockingQueue<Response> responses = new LinkedBlockingQueue<>();
	private final BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
	private HttpServer server;
	private FinnhubClient client;

	@BeforeEach
	void setUp() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/api/v1/company-news", this::respond);
		server.start();
		client = newClient("test-api-key", Duration.ofSeconds(3));
	}

	@AfterEach
	void tearDown() {
		server.stop(0);
	}

	@Test
	@DisplayName("고정된 세 종목의 뉴스를 인증 헤더와 조회 기간으로 요청한다")
	void fetchesFixedSymbolsWithAuthenticationAndDateRange() {
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			responses.add(new Response(200, """
					[{
					  "id": 123456789,
					  "category": "company",
					  "datetime": 1793059200,
					  "headline": "%s news",
					  "image": "https://example.com/image.png",
					  "related": "%s",
					  "source": "Example News",
					  "summary": "종목 뉴스 요약",
					  "url": "https://example.com/news"
					}]
					""".formatted(symbol, symbol)));
		}

		List<CompanyNews> result = fetch();

		assertThat(result).extracting(CompanyNews::symbol).containsExactly("NVDA", "AAPL", "MSFT");
		for (CompanyNews company : result) {
			assertThat(company.articles()).containsExactly(new FinnhubResponse(
					123456789L, "company", 1793059200L, company.symbol() + " news",
					"https://example.com/image.png", company.symbol(), "Example News",
					"종목 뉴스 요약", "https://example.com/news"));
			Request request = requests.remove();
			assertThat(request.method()).isEqualTo("GET");
			assertThat(request.path()).isEqualTo("/api/v1/company-news");
			assertThat(request.query()).isEqualTo("symbol=" + company.symbol()
					+ "&from=2026-10-27&to=2026-10-28");
			assertThat(request.token()).isEqualTo("test-api-key");
			assertThat(request.accept()).isEqualTo("application/json");
		}
		assertThat(requests).isEmpty();
	}

	@Test
	@DisplayName("당일 조회에서 뉴스가 없어도 종목별 빈 목록을 반환한다")
	void retainsSymbolsWhenNoNewsExistsAndAllowsSameDayRange() {
		for (int i = 0; i < 3; i++) {
			responses.add(new Response(200, "[]"));
		}

		List<CompanyNews> result = client.fetchCompanyNews(TO, TO).collectList().block(TEST_TIMEOUT);

		assertThat(result).extracting(CompanyNews::symbol).containsExactly("NVDA", "AAPL", "MSFT");
		assertThat(result).allSatisfy(company -> assertThat(company.articles()).isEmpty());
		assertThat(requests).allSatisfy(request ->
				assertThat(request.query()).contains("from=2026-10-28&to=2026-10-28"));
	}

	@DisplayName("HTTP 오류를 전달하고 이후 종목 요청을 중단한다")
	@ParameterizedTest(name = "{displayName} (HTTP {0})")
	@ValueSource(ints = {401, 429, 500})
	void propagatesHttpErrorsAndStopsFurtherRequests(int status) {
		responses.add(new Response(status, "{\"error\":\"Request failed\"}"));

		assertThatThrownBy(this::fetch)
				.isInstanceOfSatisfying(WebClientResponseException.class, exception ->
						assertThat(exception.getStatusCode().value()).isEqualTo(status));
		assertThat(requests).hasSize(1);
	}

	@Test
	@DisplayName("첫 종목 조회 후 오류가 발생하면 이후 종목 요청을 중단한다")
	void propagatesFailureAfterACompletedSymbol() {
		responses.add(new Response(200, "[]"));
		responses.add(new Response(429, "{\"error\":\"Rate limit exceeded\"}"));

		assertThatThrownBy(this::fetch).isInstanceOf(WebClientResponseException.TooManyRequests.class);
		assertThat(requests).extracting(Request::query).containsExactly(
				"symbol=NVDA&from=2026-10-27&to=2026-10-28",
				"symbol=AAPL&from=2026-10-27&to=2026-10-28");
	}

	@Test
	@DisplayName("API 키가 없으면 HTTP 요청 전에 실패한다")
	void rejectsMissingApiKeyBeforeSendingRequests() {
		client = newClient(" ", Duration.ofSeconds(3));

		assertThatThrownBy(this::fetch)
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("FINNHUB_API_KEY must be configured");
		assertThat(requests).isEmpty();
	}

	@Test
	@DisplayName("시작일이 종료일보다 늦으면 HTTP 요청 전에 실패한다")
	void rejectsReversedDateRangeBeforeSendingRequests() {
		assertThatThrownBy(() -> client.fetchCompanyNews(TO, FROM))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("from must be on or before to");
		assertThat(requests).isEmpty();
	}

	@Test
	@DisplayName("서버가 응답하지 않으면 제한 시간 초과로 실패한다")
	void timesOutWhenServerDoesNotRespond() {
		client = newClient("test-api-key", Duration.ofSeconds(1));
		responses.add(new Response(0, ""));

		assertThatThrownBy(this::fetch).hasCauseInstanceOf(TimeoutException.class);
		assertThat(requests).hasSize(1);
	}

	private List<CompanyNews> fetch() {
		return client.fetchCompanyNews(FROM, TO).collectList().block(TEST_TIMEOUT);
	}

	private FinnhubClient newClient(String apiKey, Duration timeout) {
		String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/api/v1";
		return new FinnhubClient(WebClient.builder(), new FinnhubProperties(baseUrl, apiKey, timeout));
	}

	private void respond(HttpExchange exchange) throws IOException {
		requests.add(new Request(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
				exchange.getRequestURI().getRawQuery(), exchange.getRequestHeaders().getFirst("X-Finnhub-Token"),
				exchange.getRequestHeaders().getFirst("Accept")));
		Response response = responses.poll();
		if (response == null) {
			response = new Response(500, "{\"error\":\"No test response configured\"}");
		}
		if (response.status() == 0) {
			return;
		}
		try (exchange) {
			byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/json");
			exchange.sendResponseHeaders(response.status(), body.length);
			exchange.getResponseBody().write(body);
		}
	}

	private record Request(String method, String path, String query, String token, String accept) {
	}

	private record Response(int status, String body) {
	}
}
