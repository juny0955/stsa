package dev.junyoung.stsa.collector.news;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.config.FixedRateTask;

import dev.junyoung.stsa.collector.domain.JobStatus;
import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import dev.junyoung.stsa.collector.domain.CompanyNews;
import dev.junyoung.stsa.collector.domain.JobExecution;
import dev.junyoung.stsa.collector.service.JobExecutionService;
import dev.junyoung.stsa.collector.news.client.dto.FinnhubResponse;
import dev.junyoung.stsa.collector.scheduler.NewsScheduler;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisplayName("뉴스 수집 스케줄러")
class FinnhubResponseSchedulerTests {

	private static final Duration TEST_TIMEOUT = Duration.ofSeconds(5);

	private JobExecutionService jobExecutionService;
	private JobExecution startedExecution;

	@BeforeEach
	void setUp() {
		jobExecutionService = mock(JobExecutionService.class);
		startedExecution = JobExecution.builder()
				.jobName("News Collect")
				.processedCount(3)
				.status(JobStatus.RUNNING)
				.build();
		when(jobExecutionService.start(anyString(), anyInt())).thenReturn(startedExecution);
	}

	@Test
	@DisplayName("시작 즉시 실행하고 기본 30분 주기로 등록한다")
	void startsImmediatelyAndUsesThirtyMinuteIntervalByDefault() {
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicInteger subscriptions = new AtomicInteger();
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.defer(() -> {
			subscriptions.incrementAndGet();
			return Flux.just(success("NVDA"));
		}));
		LocalDate before = LocalDate.now(ZoneOffset.UTC);

		contextRunner(client).run(context -> {
			assertThat(context).hasNotFailed().hasSingleBean(NewsScheduler.class);
			await().atMost(Duration.ofSeconds(5)).until(() -> subscriptions.get() == 1);
			LocalDate after = LocalDate.now(ZoneOffset.UTC);
			verify(client).fetchCompanyNews(
					eq(List.of("NVDA", "AAPL", "MSFT")),
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
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.defer(() -> {
			subscriptions.incrementAndGet();
			return Flux.empty();
		}));

		contextRunner(client).withPropertyValues("finnhub.news.interval=PT0.05S").run(context -> {
			assertThat(context).hasNotFailed();
			await().atMost(Duration.ofSeconds(5)).until(() -> subscriptions.get() >= 2);
			verify(client, atLeast(2)).fetchCompanyNews(any(), any(), any());
		});
	}

	@Test
	@DisplayName("조회가 실패해도 다음 주기에 다시 실행한다")
	void continuesScheduledExecutionAfterAnError() {
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicInteger successfulSubscriptions = new AtomicInteger();
		when(client.fetchCompanyNews(any(), any(), any()))
				.thenReturn(Flux.error(new IllegalStateException("Temporary test failure")))
				.thenReturn(Flux.defer(() -> {
					successfulSubscriptions.incrementAndGet();
					return Flux.just(success("NVDA"));
				}));

		contextRunner(client).withPropertyValues("finnhub.news.interval=PT0.05S").run(context -> {
			assertThat(context).hasNotFailed();
			await().atMost(Duration.ofSeconds(5)).until(() -> successfulSubscriptions.get() >= 1);
			verify(client, atLeast(2)).fetchCompanyNews(any(), any(), any());
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
	@DisplayName("빈 뉴스 목록도 성공 종목으로 집계한다")
	void countsEmptyNewsListsAsSuccessfulSymbols() {
		FinnhubClient client = mock(FinnhubClient.class);
		FinnhubResponse article = new FinnhubResponse(1L, "company", 1793059200L, "뉴스 제목",
				"", "NVDA", "Example News", "요약", "https://example.com/news");
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.just(
				new NewsFetchResult.Success(new CompanyNews("NVDA", List.of(article))),
				success("AAPL"), success("MSFT")));

		new NewsScheduler(jobExecutionService, client).collectNews();

		verify(jobExecutionService).finish(startedExecution, 3, 0, null);
	}

	@Test
	@DisplayName("성공과 실패 종목 수를 종료 처리에 전달한다")
	void countsMixedResults() {
		FinnhubClient client = mock(FinnhubClient.class);
		RuntimeException cause = new IllegalStateException("원인 예외");
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.just(
				success("AAPL"), new NewsFetchResult.Failure("NVDA", cause), success("MSFT")));

		new NewsScheduler(jobExecutionService, client).collectNews();

		verify(jobExecutionService).finish(startedExecution, 2, 1, null);
	}

	@Test
	@DisplayName("모든 종목이 실패하면 성공 0건과 실패 3건을 전달한다")
	void countsAllFailures() {
		FinnhubClient client = mock(FinnhubClient.class);
		RuntimeException cause = new IllegalStateException("조회 실패");
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.just(
				new NewsFetchResult.Failure("NVDA", cause),
				new NewsFetchResult.Failure("AAPL", cause),
				new NewsFetchResult.Failure("MSFT", cause)));

		new NewsScheduler(jobExecutionService, client).collectNews();

		verify(jobExecutionService).finish(startedExecution, 0, 3, null);
	}

	@Test
	@DisplayName("수집을 다시 실행하면 이전 실행의 성공과 실패 건수를 누적하지 않는다")
	void resetsCountsOnEachExecution() {
		FinnhubClient client = mock(FinnhubClient.class);
		RuntimeException cause = new IllegalStateException("조회 실패");
		when(client.fetchCompanyNews(any(), any(), any()))
				.thenReturn(Flux.just(success("NVDA"),
						new NewsFetchResult.Failure("AAPL", cause),
						new NewsFetchResult.Failure("MSFT", cause)))
				.thenReturn(Flux.just(success("NVDA"), success("AAPL"), success("MSFT")));
		NewsScheduler scheduler = new NewsScheduler(jobExecutionService, client);

		scheduler.collectNews();
		scheduler.collectNews();

		var order = inOrder(jobExecutionService);
		order.verify(jobExecutionService).finish(startedExecution, 1, 2, null);
		order.verify(jobExecutionService).finish(startedExecution, 3, 0, null);
	}

	@Test
	@DisplayName("응답 스트림이 완료될 때까지 실행을 종료하지 않고 완료 후 집계 결과를 전달한다")
	void finishesOnlyAfterStreamCompletes() throws Exception {
		FinnhubClient client = mock(FinnhubClient.class);
		Sinks.Many<NewsFetchResult> source = Sinks.many().unicast().onBackpressureBuffer();
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(source.asFlux());
		NewsScheduler scheduler = new NewsScheduler(jobExecutionService, client);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<?> execution = executor.submit(scheduler::collectNews);

		try {
			await().atMost(TEST_TIMEOUT).until(() -> source.currentSubscriberCount() == 1);
			assertThat(source.tryEmitNext(success("AAPL"))).isEqualTo(Sinks.EmitResult.OK);
			assertThat(execution.isDone()).isFalse();

			assertThat(source.tryEmitNext(new NewsFetchResult.Failure("NVDA",
					new IllegalStateException("조회 실패")))).isEqualTo(Sinks.EmitResult.OK);
			verify(jobExecutionService, never()).finish(any(), anyInt(), anyInt(), any());
			assertThat(execution.isDone()).isFalse();

			assertThat(source.tryEmitNext(success("MSFT"))).isEqualTo(Sinks.EmitResult.OK);
			assertThat(source.tryEmitComplete()).isEqualTo(Sinks.EmitResult.OK);
			execution.get(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
			verify(jobExecutionService).finish(startedExecution, 2, 1, null);
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

	private ApplicationContextRunner contextRunner(FinnhubClient client) {
		return new ApplicationContextRunner()
				.withUserConfiguration(SchedulingConfiguration.class)
				.withBean(JobExecutionService.class, () -> jobExecutionService)
				.withBean(FinnhubClient.class, () -> client);
	}

	@TestConfiguration(proxyBeanMethods = false)
	@EnableScheduling
	@ComponentScan(basePackageClasses = NewsScheduler.class)
	static class SchedulingConfiguration {
	}
}
