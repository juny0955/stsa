package dev.junyoung.stsa.collector.job;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import dev.junyoung.stsa.collector.domain.CompanyNews;
import dev.junyoung.stsa.collector.news.NewsFetchResult;
import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import dev.junyoung.stsa.collector.scheduler.NewsScheduler;
import dev.junyoung.stsa.persistence.jooq.tables.records.JobExecutionRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import static dev.junyoung.stsa.persistence.jooq.Tables.JOB_EXECUTION;
import static dev.junyoung.stsa.persistence.jooq.Tables.STOCK;
import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE;
import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE_STOCK;
import static dev.junyoung.stsa.collector.job.NewsTestData.article;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("스케줄러 작업 실행 이력 통합")
class NewsSchedulerIntegrationTests extends JobExecutionTestSupport {

	@Test
	@DisplayName("API 요청 전에 RUNNING을 커밋하고 수집 종료 뒤 같은 실행을 완료한다")
	void commitsStartBeforeFetchingAndFinishesTheSameExecution() {
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			registerStock(symbol);
		}
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicReference<JobExecutionRecord> started = new AtomicReference<>();
		when(client.fetchCompanyNews(any(), any(), any())).thenAnswer(invocation -> {
			assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
			var running = observer.selectFrom(JOB_EXECUTION).fetchSingle();
			assertThat(running.getStatus()).isEqualTo("RUNNING");
			assertThat(running.getFinishedAt()).isNull();
			started.set(running);
			List<String> symbols = invocation.getArgument(0);
			return Flux.fromIterable(symbols).map(symbol ->
					new NewsFetchResult.Success(new CompanyNews(symbol, List.of())));
		});

		new NewsScheduler(service, newsPersistenceService, client).collectNews();

		var finished = observer.selectFrom(JOB_EXECUTION).fetchSingle();
		assertThat(finished.getId()).isEqualTo(started.get().getId());
		assertThat(finished.getStatus()).isEqualTo("SUCCESS");
		assertThat(finished.getProcessedCount()).isEqualTo(3);
		assertThat(finished.getSuccessCount()).isEqualTo(3);
		assertThat(finished.getFailureCount()).isZero();
		assertThat(finished.getFinishedAt()).isNotNull();
		assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
	}

	@Test
	@DisplayName("미등록 종목의 저장 실패를 격리하고 나머지 뉴스를 저장해 PARTIAL_SUCCESS로 종료한다")
	void continuesSavingOtherStocksAfterOneStockFails() {
		registerStock("AAPL");
		registerStock("MSFT");
		FinnhubClient client = mock(FinnhubClient.class);
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(
			Flux.just(response("NVDA", 101), response("AAPL", 102), response("MSFT", 103)));

		new NewsScheduler(service, newsPersistenceService, client).collectNews();

		var execution = observer.selectFrom(JOB_EXECUTION).fetchSingle();
		assertThat(execution.getStatus()).isEqualTo("PARTIAL_SUCCESS");
		assertThat(execution.getProcessedCount()).isEqualTo(3);
		assertThat(execution.getSuccessCount()).isEqualTo(2);
		assertThat(execution.getFailureCount()).isEqualTo(1);
		assertThat(execution.getErrorMessage()).isNull();
		assertThat(observer.fetchCount(NEWS_ARTICLE)).isEqualTo(2);
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(2);
		assertThat(observer.fetchCount(STOCK)).isEqualTo(2);
		assertThat(observer.fetchExists(NEWS_ARTICLE, NEWS_ARTICLE.EXTERNAL_ID.eq("101"))).isFalse();
	}

	@Test
	@DisplayName("전체 수집 오류 전에 받은 기사는 커밋하고 실행은 FAILED로 종료한다")
	void commitsReceivedNewsBeforeFinishingWithStreamError() {
		registerStock("NVDA");
		FinnhubClient client = mock(FinnhubClient.class);
		RuntimeException cause = new IllegalStateException("Fetch stopped");
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(
			Flux.concat(Flux.<NewsFetchResult>just(response("NVDA", 101)), Flux.error(cause)));

		new NewsScheduler(service, newsPersistenceService, client).collectNews();

		var execution = observer.selectFrom(JOB_EXECUTION).fetchSingle();
		assertThat(execution.getStatus()).isEqualTo("FAILED");
		assertThat(execution.getSuccessCount()).isEqualTo(1);
		assertThat(execution.getFailureCount()).isZero();
		assertThat(execution.getErrorMessage()).isEqualTo("Fetch stopped");
		assertThat(observer.fetchCount(NEWS_ARTICLE)).isEqualTo(1);
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(1);
	}

	@Test
	@DisplayName("실행 종료 갱신이 실패해도 종목별로 커밋한 뉴스는 유지한다")
	void retainsNewsWhenExecutionFinishFails() {
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			registerStock(symbol);
		}
		FinnhubClient client = mock(FinnhubClient.class);
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(
			Flux.just(response("NVDA", 101), response("AAPL", 102), response("MSFT", 103)));
		sql.execute("ALTER TABLE job_execution ADD CONSTRAINT test_running_only CHECK (status = 'RUNNING')");
		try {
			new NewsScheduler(service, newsPersistenceService, client).collectNews();

			assertThat(observer.selectFrom(JOB_EXECUTION).fetchSingle().getStatus()).isEqualTo("RUNNING");
			assertThat(observer.selectFrom(JOB_EXECUTION).fetchSingle().getFinishedAt()).isNull();
			assertThat(observer.fetchCount(NEWS_ARTICLE)).isEqualTo(3);
			assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(3);
		} finally {
			sql.execute("ALTER TABLE job_execution DROP CONSTRAINT test_running_only");
		}
	}

	@Test
	@DisplayName("다른 종목의 응답을 기다리는 동안 수신한 종목의 뉴스를 먼저 커밋한다")
	void commitsEachResponseBeforeCollectionCompletes() throws Exception {
		for (String symbol : List.of("NVDA", "AAPL", "MSFT")) {
			registerStock(symbol);
		}
		Sinks.Many<NewsFetchResult> source = Sinks.many().unicast().onBackpressureBuffer();
		FinnhubClient client = mock(FinnhubClient.class);
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(source.asFlux());
		var executor = Executors.newSingleThreadExecutor();
		var collecting = executor.submit(new NewsScheduler(service, newsPersistenceService, client)::collectNews);
		try {
			await().atMost(Duration.ofSeconds(5)).until(() -> source.currentSubscriberCount() == 1);
			assertThat(source.tryEmitNext(response("NVDA", 101))).isEqualTo(Sinks.EmitResult.OK);
			await().atMost(Duration.ofSeconds(5)).until(() -> observer.fetchCount(NEWS_ARTICLE) == 1);
			assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(1);
			assertThat(observer.selectFrom(JOB_EXECUTION).fetchSingle().getStatus()).isEqualTo("RUNNING");
			assertThat(collecting.isDone()).isFalse();

			assertThat(source.tryEmitNext(response("AAPL", 102))).isEqualTo(Sinks.EmitResult.OK);
			assertThat(source.tryEmitNext(response("MSFT", 103))).isEqualTo(Sinks.EmitResult.OK);
			assertThat(source.tryEmitComplete()).isEqualTo(Sinks.EmitResult.OK);
			collecting.get(5, TimeUnit.SECONDS);

			assertThat(observer.fetchCount(NEWS_ARTICLE)).isEqualTo(3);
			assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(3);
			assertThat(observer.selectFrom(JOB_EXECUTION).fetchSingle().getStatus()).isEqualTo("SUCCESS");
		} finally {
			source.tryEmitComplete();
			collecting.cancel(true);
			executor.shutdownNow();
			assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
		}
	}

	private NewsFetchResult.Success response(String symbol, long articleId) {
		return new NewsFetchResult.Success(new CompanyNews(symbol, List.of(article(articleId))));
	}
}
