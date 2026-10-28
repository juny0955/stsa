package dev.junyoung.stsa.collector.news;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

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

	private final Map<String, Response> responses = new ConcurrentHashMap<>();
	private final BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
	private ExecutorService serverExecutor;
	private HttpServer server;
	private FinnhubClient client;

	@BeforeEach
	void setUp() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		serverExecutor = Executors.newFixedThreadPool(3);
		server.setExecutor(serverExecutor);
		server.createContext("/api/v1/company-news", this::respond);
		server.start();
		client = newClient("test-api-key", Duration.ofSeconds(3));
	}

	@AfterEach
	void tearDown() throws InterruptedException {
		responses.values().forEach(response -> response.release().countDown());
		server.stop(0);
		serverExecutor.shutdownNow();
		assertThat(serverExecutor.awaitTermination(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
	}

	@Test
	@DisplayName("고정된 세 종목의 뉴스를 인증 헤더와 조회 기간으로 요청한다")
	void fetchesFixedSymbolsWithAuthenticationAndDateRange() {
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			responses.put(symbol, new Response(200, """
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

		assertThat(result).extracting(CompanyNews::symbol).containsExactlyInAnyOrder("NVDA", "AAPL", "MSFT");
		for (CompanyNews company : result) {
			assertThat(company.articles()).containsExactly(new FinnhubResponse(
					123456789L, "company", 1793059200L, company.symbol() + " news",
					"https://example.com/image.png", company.symbol(), "Example News",
					"종목 뉴스 요약", "https://example.com/news"));
		}
		assertThat(requests).hasSize(3);
		assertThat(requests).extracting(Request::query).containsExactlyInAnyOrder(
				"symbol=NVDA&from=2026-10-27&to=2026-10-28",
				"symbol=AAPL&from=2026-10-27&to=2026-10-28",
				"symbol=MSFT&from=2026-10-27&to=2026-10-28");
		assertThat(requests).allSatisfy(request -> {
			assertThat(request.method()).isEqualTo("GET");
			assertThat(request.path()).isEqualTo("/api/v1/company-news");
			assertThat(request.token()).isEqualTo("test-api-key");
			assertThat(request.accept()).isEqualTo("application/json");
		});
	}

	@Test
	@DisplayName("세 종목을 응답 대기 없이 동시에 요청하고 응답 도착순으로 반환한다")
	void requestsAllSymbolsConcurrentlyAndEmitsInResponseOrder() throws Exception {
		client = newClient("test-api-key", Duration.ofSeconds(20));
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			responses.put(symbol, new Response(200, "[]", new CountDownLatch(1)));
		}
		BlockingQueue<String> emittedSymbols = new LinkedBlockingQueue<>();
		CompletableFuture<List<CompanyNews>> result = client.fetchCompanyNews(FROM, TO)
				.doOnNext(company -> emittedSymbols.add(company.symbol()))
				.collectList().toFuture();

		try {
			assertAllRequestsArrived();
			assertThat(result).isNotDone();
			assertThat(emittedSymbols).isEmpty();

			for (String symbol : List.of("MSFT", "AAPL", "NVDA")) {
				responses.get(symbol).release().countDown();
				assertThat(emittedSymbols.poll(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isEqualTo(symbol);
			}
			assertThat(result.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
					.extracting(CompanyNews::symbol).containsExactly("MSFT", "AAPL", "NVDA");
		}
		finally {
			result.cancel(true);
			responses.values().forEach(response -> response.release().countDown());
		}
	}

	@Test
	@DisplayName("당일 조회에서 뉴스가 없어도 종목별 빈 목록을 반환한다")
	void retainsSymbolsWhenNoNewsExistsAndAllowsSameDayRange() {
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			responses.put(symbol, new Response(200, "[]"));
		}

		List<CompanyNews> result = client.fetchCompanyNews(TO, TO).collectList().block(TEST_TIMEOUT);

		assertThat(result).extracting(CompanyNews::symbol).containsExactlyInAnyOrder("NVDA", "AAPL", "MSFT");
		assertThat(result).allSatisfy(company -> assertThat(company.articles()).isEmpty());
		assertThat(requests).allSatisfy(request ->
				assertThat(request.query()).contains("from=2026-10-28&to=2026-10-28"));
	}

	@DisplayName("동시 요청 중 발생한 HTTP 오류를 전달한다")
	@ParameterizedTest(name = "{displayName} (HTTP {0})")
	@ValueSource(ints = {401, 429, 500})
	void propagatesHttpErrorsFromConcurrentRequests(int status) {
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			responses.put(symbol, new Response(status, "{\"error\":\"Request failed\"}"));
		}

		assertThatThrownBy(this::fetch)
				.isInstanceOfSatisfying(WebClientResponseException.class, exception ->
						assertThat(exception.getStatusCode().value()).isEqualTo(status));
	}

	@Test
	@DisplayName("일부 종목 조회 후 오류가 발생하면 나머지 응답을 기다리지 않고 실패한다")
	void propagatesFailureAfterACompletedSymbolWithoutWaitingForOtherResponses() throws Exception {
		client = newClient("test-api-key", Duration.ofSeconds(20));
		responses.put("NVDA", new Response(200, "[]", new CountDownLatch(1)));
		responses.put("AAPL", new Response(429, "{\"error\":\"Rate limit exceeded\"}", new CountDownLatch(1)));
		responses.put("MSFT", new Response(200, "[]", new CountDownLatch(1)));
		BlockingQueue<String> emittedSymbols = new LinkedBlockingQueue<>();
		CompletableFuture<List<CompanyNews>> result = client.fetchCompanyNews(FROM, TO)
				.doOnNext(company -> emittedSymbols.add(company.symbol()))
				.collectList().toFuture();

		try {
			assertAllRequestsArrived();
			responses.get("NVDA").release().countDown();
			assertThat(emittedSymbols.poll(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isEqualTo("NVDA");
			responses.get("AAPL").release().countDown();

			assertThatThrownBy(() -> result.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
					.hasCauseInstanceOf(WebClientResponseException.TooManyRequests.class);
			assertThat(emittedSymbols).isEmpty();
			assertThat(responses.get("MSFT").release().getCount()).isEqualTo(1);
		}
		finally {
			result.cancel(true);
			responses.values().forEach(response -> response.release().countDown());
		}
	}

	@Test
	@DisplayName("한 종목이 실패하면 진행 중인 나머지 두 요청의 구독을 취소한다")
	void cancelsOtherSubscriptionsWhenOneRequestFails() {
		Sinks.One<ClientResponse> failingResponse = Sinks.one();
		AtomicInteger subscriptions = new AtomicInteger();
		AtomicInteger cancellations = new AtomicInteger();
		WebClient.Builder builder = WebClient.builder().exchangeFunction(request -> {
			subscriptions.incrementAndGet();
			String symbol = UriComponentsBuilder.fromUri(request.url()).build().getQueryParams().getFirst("symbol");
			if ("NVDA".equals(symbol)) {
				return failingResponse.asMono();
			}
			return Mono.<ClientResponse>never().doOnCancel(cancellations::incrementAndGet);
		});
		client = new FinnhubClient(builder,
				new FinnhubProperties("https://example.com/api/v1", "test-api-key", Duration.ofSeconds(3)));
		CompletableFuture<List<CompanyNews>> result = client.fetchCompanyNews(FROM, TO).collectList().toFuture();

		try {
			assertThat(subscriptions).hasValue(3);
			assertThat(failingResponse.tryEmitValue(ClientResponse.create(HttpStatus.TOO_MANY_REQUESTS).build()))
					.isEqualTo(Sinks.EmitResult.OK);

			assertThatThrownBy(() -> result.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
					.hasCauseInstanceOf(WebClientResponseException.TooManyRequests.class);
			assertThat(cancellations).hasValue(2);
		}
		finally {
			result.cancel(true);
		}
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
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			responses.put(symbol, new Response(0, ""));
		}

		assertThatThrownBy(this::fetch).hasCauseInstanceOf(TimeoutException.class);
	}

	private void assertAllRequestsArrived() throws InterruptedException {
		for (int i = 0; i < 3; i++) {
			assertThat(requests.poll(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
					.as("응답을 반환하기 전에 세 종목의 요청이 모두 도착해야 한다")
					.isNotNull();
		}
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
		String symbol = UriComponentsBuilder.fromUri(exchange.getRequestURI()).build().getQueryParams().getFirst("symbol");
		Response response = responses.get(symbol);
		if (response == null) {
			response = new Response(500, "{\"error\":\"No test response configured\"}");
		}
		if (response.status() == 0) {
			return;
		}
		try (exchange) {
			try {
				if (!response.release().await(TEST_TIMEOUT.multipliedBy(2).toMillis(), TimeUnit.MILLISECONDS)) {
					throw new IOException("Test response was not released for " + symbol);
				}
			}
			catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
				return;
			}
			byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/json");
			exchange.sendResponseHeaders(response.status(), body.length);
			exchange.getResponseBody().write(body);
		}
	}

	private record Request(String method, String path, String query, String token, String accept) {
	}

	private record Response(int status, String body, CountDownLatch release) {

		private Response(int status, String body) {
			this(status, body, new CountDownLatch(0));
		}
	}
}
