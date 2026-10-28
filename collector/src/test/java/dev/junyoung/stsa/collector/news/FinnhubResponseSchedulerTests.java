package dev.junyoung.stsa.collector.news;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.config.FixedRateTask;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import dev.junyoung.stsa.collector.news.domain.CompanyNews;
import dev.junyoung.stsa.collector.news.domain.NewsFetchResult;
import dev.junyoung.stsa.collector.news.client.dto.FinnhubResponse;
import dev.junyoung.stsa.collector.news.scheduler.NewsScheduler;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisplayName("뉴스 수집 스케줄러")
class FinnhubResponseSchedulerTests {

	private static final Duration TEST_TIMEOUT = Duration.ofSeconds(5);

	private Logger logger;
	private Level previousLogLevel;
	private ListAppender<ILoggingEvent> logAppender;

	@BeforeEach
	void setUp() {
		logger = (Logger)LoggerFactory.getLogger(NewsScheduler.class);
		previousLogLevel = logger.getLevel();
		logger.setLevel(Level.INFO);
		logAppender = new ListAppender<>();
		logAppender.list = new CopyOnWriteArrayList<>();
		logAppender.start();
		logger.addAppender(logAppender);
	}

	@AfterEach
	void tearDown() {
		logger.detachAppender(logAppender);
		logAppender.stop();
		logger.setLevel(previousLogLevel);
	}

	@Test
	@DisplayName("시작 즉시 실행하고 기본 30분 주기로 등록한다")
	void startsImmediatelyAndUsesThirtyMinuteIntervalByDefault() {
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicInteger subscriptions = new AtomicInteger();
		when(client.fetchCompanyNews(any(), any())).thenReturn(Flux.defer(() -> {
			subscriptions.incrementAndGet();
			return Flux.just(success("NVDA"));
		}));
		LocalDate before = LocalDate.now(ZoneOffset.UTC);

		contextRunner(client).run(context -> {
			assertThat(context).hasNotFailed().hasSingleBean(NewsScheduler.class);
			await().atMost(Duration.ofSeconds(5)).until(() -> subscriptions.get() == 1);
			LocalDate after = LocalDate.now(ZoneOffset.UTC);
			verify(client).fetchCompanyNews(
					argThat(from ->
						from.equals(before.minusDays(1)) || from.equals(after.minusDays(1))),
					argThat(to -> to.equals(before) || to.equals(after)));
			var processor = context.getBean(ScheduledAnnotationBeanPostProcessor.class);
			assertThat(processor.getScheduledTasks()).singleElement().satisfies(task -> {
				assertThat(task.getTask()).isInstanceOfSatisfying(FixedRateTask.class, fixedRate -> {
					assertThat(fixedRate.getIntervalDuration()).isEqualTo(Duration.ofMinutes(30));
					assertThat(fixedRate.getInitialDelayDuration()).isEqualTo(Duration.ZERO);
				});
			});
		});
	}

	@Test
	@DisplayName("설정한 주기마다 뉴스 조회를 다시 구독한다")
	void subscribesAgainOnEachScheduledExecution() {
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicInteger subscriptions = new AtomicInteger();
		when(client.fetchCompanyNews(any(), any())).thenReturn(Flux.defer(() -> {
			subscriptions.incrementAndGet();
			return Flux.empty();
		}));

		contextRunner(client).withPropertyValues("finnhub.news.interval=PT0.05S").run(context -> {
			assertThat(context).hasNotFailed();
			await().atMost(Duration.ofSeconds(5)).until(() -> subscriptions.get() >= 2);
			verify(client, atLeast(2)).fetchCompanyNews(any(), any());
		});
	}

	@Test
	@DisplayName("조회가 실패해도 다음 주기에 다시 실행한다")
	void continuesScheduledExecutionAfterAnError() {
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicInteger successfulSubscriptions = new AtomicInteger();
		when(client.fetchCompanyNews(any(), any()))
				.thenReturn(Flux.error(new IllegalStateException("Temporary test failure")))
				.thenReturn(Flux.defer(() -> {
					successfulSubscriptions.incrementAndGet();
					return Flux.just(success("NVDA"));
				}));

		contextRunner(client).withPropertyValues("finnhub.news.interval=PT0.05S").run(context -> {
			assertThat(context).hasNotFailed();
			await().atMost(Duration.ofSeconds(5)).until(() -> successfulSubscriptions.get() >= 1);
			verify(client, atLeast(2)).fetchCompanyNews(any(), any());
		});
	}

	@Test
	@DisplayName("비활성화하면 스케줄러를 등록하지 않는다")
	void doesNotRegisterSchedulerWhenDisabled() {
		FinnhubClient client = mock(FinnhubClient.class);

		contextRunner(client).withPropertyValues("finnhub.news.enabled=false").run(context -> {
			assertThat(context).hasNotFailed().doesNotHaveBean(NewsScheduler.class);
			verifyNoInteractions(client);
		});
	}

	@Test
	@DisplayName("빈 뉴스 목록도 성공으로 집계하고 실제 기사 수와 최종 건수를 기록한다")
	void logsSuccessfulResultsAndSummary() {
		FinnhubClient client = mock(FinnhubClient.class);
		FinnhubResponse article = new FinnhubResponse(1L, "company", 1793059200L, "뉴스 제목",
				"", "NVDA", "Example News", "요약", "https://example.com/news");
		when(client.fetchCompanyNews(any(), any())).thenReturn(Flux.just(
				new NewsFetchResult.Success(new CompanyNews("NVDA", List.of(article))),
				success("AAPL"), success("MSFT")));

		new NewsScheduler(client).collectNews();

		assertThat(logAppender.list).extracting(ILoggingEvent::getFormattedMessage).containsExactly(
				"NVDA 뉴스 1건 조회 완료", "AAPL 뉴스 0건 조회 완료", "MSFT 뉴스 0건 조회 완료",
				"Finnhub 뉴스 수집 완료: 성공 3종목, 실패 0종목");
		assertErrors();
	}

	@Test
	@DisplayName("성공과 실패를 받은 순서대로 기록하고 원본 예외를 보존한다")
	void logsMixedResultsAndOriginalException() {
		FinnhubClient client = mock(FinnhubClient.class);
		RuntimeException cause = new IllegalStateException("원인 예외");
		when(client.fetchCompanyNews(any(), any())).thenReturn(Flux.just(
				success("AAPL"), new NewsFetchResult.Failure("NVDA", cause), success("MSFT")));

		new NewsScheduler(client).collectNews();

		assertThat(logAppender.list).extracting(ILoggingEvent::getFormattedMessage).containsExactly(
				"AAPL 뉴스 0건 조회 완료", failureMessage("NVDA"), "MSFT 뉴스 0건 조회 완료",
				"Finnhub 뉴스 수집 완료: 성공 2종목, 실패 1종목");
		assertErrors("NVDA");
		assertThat(errorLogs().getFirst().getThrowableProxy()).isInstanceOfSatisfying(ThrowableProxy.class,
				proxy -> assertThat(proxy.getThrowable()).isSameAs(cause));
	}

	@Test
	@DisplayName("모든 종목이 실패하면 오류를 종목마다 한 번 기록하고 성공 0건과 실패 3건을 집계한다")
	void logsAllFailuresAndSummary() {
		FinnhubClient client = mock(FinnhubClient.class);
		RuntimeException cause = new IllegalStateException("조회 실패");
		when(client.fetchCompanyNews(any(), any())).thenReturn(Flux.just(
				new NewsFetchResult.Failure("NVDA", cause),
				new NewsFetchResult.Failure("AAPL", cause),
				new NewsFetchResult.Failure("MSFT", cause)));

		new NewsScheduler(client).collectNews();

		assertErrors("NVDA", "AAPL", "MSFT");
		assertThat(summaryLogs()).extracting(ILoggingEvent::getFormattedMessage)
				.containsExactly("Finnhub 뉴스 수집 완료: 성공 0종목, 실패 3종목");
		assertThat(logAppender.list).hasSize(4);
	}

	@Test
	@DisplayName("수집을 다시 실행하면 이전 실행의 성공과 실패 건수를 누적하지 않는다")
	void resetsCountsOnEachExecution() {
		FinnhubClient client = mock(FinnhubClient.class);
		RuntimeException cause = new IllegalStateException("조회 실패");
		when(client.fetchCompanyNews(any(), any()))
				.thenReturn(Flux.just(success("NVDA"),
						new NewsFetchResult.Failure("AAPL", cause),
						new NewsFetchResult.Failure("MSFT", cause)))
				.thenReturn(Flux.just(success("NVDA"), success("AAPL"), success("MSFT")));
		NewsScheduler scheduler = new NewsScheduler(client);

		scheduler.collectNews();
		scheduler.collectNews();

		assertThat(summaryLogs()).extracting(ILoggingEvent::getFormattedMessage).containsExactly(
				"Finnhub 뉴스 수집 완료: 성공 1종목, 실패 2종목",
				"Finnhub 뉴스 수집 완료: 성공 3종목, 실패 0종목");
		assertErrors("AAPL", "MSFT");
	}

	@Test
	@DisplayName("전체 완료를 기다리지 않고 성공과 실패를 도착 즉시 처리한다")
	void handlesResultsBeforeCollectionCompletes() throws Exception {
		FinnhubClient client = mock(FinnhubClient.class);
		Sinks.Many<NewsFetchResult> source = Sinks.many().unicast().onBackpressureBuffer();
		when(client.fetchCompanyNews(any(), any())).thenReturn(source.asFlux());
		NewsScheduler scheduler = new NewsScheduler(client);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<?> execution = executor.submit(scheduler::collectNews);

		try {
			await().atMost(TEST_TIMEOUT).until(() -> source.currentSubscriberCount() == 1);
			assertThat(source.tryEmitNext(success("AAPL"))).isEqualTo(Sinks.EmitResult.OK);
			await().atMost(TEST_TIMEOUT).untilAsserted(() ->
					assertThat(logAppender.list).extracting(ILoggingEvent::getFormattedMessage)
							.containsExactly("AAPL 뉴스 0건 조회 완료"));
			assertThat(execution.isDone()).isFalse();

			assertThat(source.tryEmitNext(new NewsFetchResult.Failure("NVDA",
					new IllegalStateException("조회 실패")))).isEqualTo(Sinks.EmitResult.OK);
			await().atMost(TEST_TIMEOUT).untilAsserted(() -> assertErrors("NVDA"));
			assertThat(summaryLogs()).isEmpty();
			assertThat(execution.isDone()).isFalse();

			assertThat(source.tryEmitNext(success("MSFT"))).isEqualTo(Sinks.EmitResult.OK);
			assertThat(source.tryEmitComplete()).isEqualTo(Sinks.EmitResult.OK);
			execution.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
			assertThat(summaryLogs()).extracting(ILoggingEvent::getFormattedMessage)
					.containsExactly("Finnhub 뉴스 수집 완료: 성공 2종목, 실패 1종목");
			assertErrors("NVDA");
		}
		finally {
			source.tryEmitComplete();
			execution.cancel(true);
			executor.shutdownNow();
			assertThat(executor.awaitTermination(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
		}
	}

	private NewsFetchResult.Success success(String symbol) {
		return new NewsFetchResult.Success(new CompanyNews(symbol, List.of()));
	}

	private List<ILoggingEvent> summaryLogs() {
		return logAppender.list.stream().filter(event -> event.getLevel() == Level.INFO
				&& event.getFormattedMessage().startsWith("Finnhub 뉴스 수집 완료:")).toList();
	}

	private List<ILoggingEvent> errorLogs() {
		return logAppender.list.stream().filter(event -> event.getLevel() == Level.ERROR).toList();
	}

	private String failureMessage(String symbol) {
		return "Finnhub " + symbol + " 뉴스 조회 실패";
	}

	private void assertErrors(String... symbols) {
		assertThat(errorLogs()).extracting(ILoggingEvent::getFormattedMessage).containsExactlyInAnyOrder(
				Arrays.stream(symbols).map(this::failureMessage).toArray(String[]::new));
		assertThat(errorLogs()).allSatisfy(event -> assertThat(event.getThrowableProxy()).isNotNull());
	}

	private ApplicationContextRunner contextRunner(FinnhubClient client) {
		return new ApplicationContextRunner()
				.withUserConfiguration(SchedulingConfiguration.class)
				.withBean(FinnhubClient.class, () -> client);
	}

	@TestConfiguration(proxyBeanMethods = false)
	@EnableScheduling
	@ComponentScan(basePackageClasses = NewsScheduler.class)
	static class SchedulingConfiguration {
	}
}
