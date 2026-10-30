package dev.junyoung.stsa.collector.job;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import dev.junyoung.stsa.collector.domain.CompanyNews;
import dev.junyoung.stsa.collector.domain.JobExecution;
import dev.junyoung.stsa.collector.domain.JobStatus;
import dev.junyoung.stsa.collector.news.NewsFetchResult;
import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import dev.junyoung.stsa.collector.scheduler.NewsScheduler;
import dev.junyoung.stsa.collector.service.JobExecutionService;
import dev.junyoung.stsa.collector.service.NewsPersistenceService;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@DisplayName("스케줄러 실행 이력 실패 처리")
class NewsSchedulerExecutionTests {

	private final JobExecutionService service = mock(JobExecutionService.class);
	private final NewsPersistenceService newsPersistenceService = mock(NewsPersistenceService.class);
	private final FinnhubClient client = mock(FinnhubClient.class);
	private final JobExecution execution = JobExecution.builder()
		.id(42L)
		.jobName("News Collect")
		.processedCount(3)
		.status(JobStatus.RUNNING)
		.build();
	private final NewsScheduler scheduler = new NewsScheduler(service, newsPersistenceService, client);

	@BeforeEach
	void setUp() {
		when(service.start("News Collect", 3)).thenReturn(execution);
	}

	@Test
	@DisplayName("시작 저장 실패 시 API 요청과 종료 갱신을 수행하지 않는다")
	void abortsBeforeFetchingWhenStartFails() {
		RuntimeException cause = new IllegalStateException("Start failed");
		when(service.start("News Collect", 3)).thenThrow(cause);

		scheduler.collectNews();

		verifyNoInteractions(client, newsPersistenceService);
		verify(service).start("News Collect", 3);
		verifyNoMoreInteractions(service);
	}

	@Test
	@DisplayName("일부 응답 후 전체 오류가 나면 실제 집계와 원본 예외를 종료 처리에 전달한다")
	void finishesWithObservedCountsAndOriginalStreamError() {
		RuntimeException cause = new IllegalStateException("Stream failed");
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.concat(
				Flux.just(success("NVDA")), Flux.error(cause)));

		scheduler.collectNews();

		verify(service).finish(execution, 1, 0, cause, List.of());
	}

	@Test
	@DisplayName("종목별 실패는 집계하되 전체 실행 예외와 구분한다")
	void distinguishesSymbolFailuresFromExecutionErrors() {
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.just(success("NVDA"),
				new NewsFetchResult.Failure("AAPL", new IllegalStateException("HTTP failed")), success("MSFT")));

		scheduler.collectNews();

		verify(service).finish(execution, 2, 1, null,
			List.of("AAPL 조회 실패: java.lang.IllegalStateException: HTTP failed"));
	}

	@Test
	@DisplayName("응답이 없으면 미응답 종목을 실패 건수에 추가하지 않는다")
	void forwardsZeroCountsForEmptyStream() {
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.empty());

		scheduler.collectNews();

		verify(service).finish(execution, 0, 0, null, List.of());
	}

	@Test
	@DisplayName("수집과 종료 갱신이 모두 실패해도 예외를 전파하지 않는다")
	void handlesCollectionAndFinishFailures() {
		RuntimeException fetchError = new IllegalStateException("Fetch failed");
		RuntimeException updateError = new IllegalStateException("Update failed");
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.error(fetchError));
		doThrow(updateError).when(service).finish(execution, 0, 0, fetchError, List.of());

		assertThatCode(scheduler::collectNews).doesNotThrowAnyException();

		verify(service).finish(execution, 0, 0, fetchError, List.of());
	}

	@Test
	@DisplayName("종료 갱신이 실패해도 다음 실행에서 다시 수집하고 새로운 실행을 종료한다")
	void continuesAfterFinishFailure() {
		JobExecution next = execution.toBuilder().id(43L).build();
		when(service.start("News Collect", 3)).thenReturn(execution, next);
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(
				Flux.just(success("NVDA"), success("AAPL"), success("MSFT")));
		doThrow(new IllegalStateException("Update failed")).doNothing()
				.when(service).finish(any(), anyInt(), anyInt(), isNull(), anyList());

		scheduler.collectNews();
		scheduler.collectNews();

		verify(client, times(2)).fetchCompanyNews(any(), any(), any());
		verify(service).finish(execution, 3, 0, null, List.of());
		verify(service).finish(next, 3, 0, null, List.of());
	}

	@Test
	@DisplayName("한 종목의 저장 실패를 격리하고 다른 종목을 순서대로 저장한다")
	void continuesAfterStorageFailure() {
		var nvda = success("NVDA");
		var aapl = success("AAPL");
		var msft = success("MSFT");
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.just(nvda, aapl, msft));
		doThrow(new IllegalStateException("Save failed")).when(newsPersistenceService).save(nvda.news());

		scheduler.collectNews();

		var order = inOrder(newsPersistenceService);
		order.verify(newsPersistenceService).save(nvda.news());
		order.verify(newsPersistenceService).save(aapl.news());
		order.verify(newsPersistenceService).save(msft.news());
		verify(service).finish(execution, 2, 1, null,
			List.of("NVDA 저장 실패: java.lang.IllegalStateException: Save failed"));
	}

	@Test
	@DisplayName("실행 시간 초과 시 수집을 취소하고 기존 집계와 오류를 종료 처리에 전달한다")
	void finishesWithObservedCountsAndTimeout() {
		ReflectionTestUtils.setField(scheduler, "executionTimeoutMillis", 100L);
		AtomicBoolean cancelled = new AtomicBoolean();
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.concat(
			Flux.just(new NewsFetchResult.Failure("NVDA", new IllegalStateException("HTTP failed"))),
			Flux.<NewsFetchResult>never().doOnCancel(() -> cancelled.set(true))));

		assertThatCode(scheduler::collectNews).doesNotThrowAnyException();

		ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
		verify(service).finish(eq(execution), eq(0),
			eq(1), error.capture(),
			eq(List.of("NVDA 조회 실패: java.lang.IllegalStateException: HTTP failed")));
		assertThat(error.getValue()).hasCauseInstanceOf(TimeoutException.class);
		assertThat(cancelled).isTrue();
	}

	@Test
	@DisplayName("저장 작업이 인터럽트에 반응하지 않아도 실행 제한 후 종료 처리한다")
	void finishesOnTimeoutWhileStorageIsStillRunning() throws Exception {
		ReflectionTestUtils.setField(scheduler, "executionTimeoutMillis", 2_000L);
		var result = success("NVDA");
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		CountDownLatch storageFinished = new CountDownLatch(1);
		doAnswer(invocation -> {
			entered.countDown();
			while (true) {
				try {
					release.await();
					break;
				} catch (InterruptedException ignored) {
					// JDBC drivers may not stop an in-flight call on interruption.
				}
			}
			storageFinished.countDown();
			return null;
		}).when(newsPersistenceService).save(result.news());
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.just(result));
		var executor = Executors.newSingleThreadExecutor();
		var collecting = executor.submit(scheduler::collectNews);
		try {
			assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
			collecting.get(5, TimeUnit.SECONDS);
			assertThat(storageFinished.getCount()).isEqualTo(1);
			ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
			verify(service).finish(eq(execution), eq(0), eq(0), error.capture(), eq(List.of()));
			assertThat(error.getValue()).hasCauseInstanceOf(TimeoutException.class);
		} finally {
			release.countDown();
			collecting.cancel(true);
			executor.shutdownNow();
			assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
			assertThat(storageFinished.await(5, TimeUnit.SECONDS)).isTrue();
		}
	}

	@Test
	@DisplayName("비동기 수집이 전체 오류로 끝나도 진행 중인 저장 완료를 기다려 종료한다")
	void waitsForStorageBeforeReportingUpstreamError() throws Exception {
		var result = success("NVDA");
		RuntimeException cause = new IllegalStateException("Stream failed during storage");
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		AtomicBoolean savedOutsideEventLoop = new AtomicBoolean();
		doAnswer(invocation -> {
			savedOutsideEventLoop.set(!Schedulers.isInNonBlockingThread());
			entered.countDown();
			if (!release.await(5, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Storage was not released");
			}
			return null;
		}).when(newsPersistenceService).save(result.news());
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(
			Flux.concat(Flux.<NewsFetchResult>just(result), Flux.error(cause)).publishOn(Schedulers.parallel()));
		var executor = Executors.newSingleThreadExecutor();
		var collecting = executor.submit(scheduler::collectNews);
		try {
			assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(savedOutsideEventLoop).isTrue();
			assertThat(collecting.isDone()).isFalse();
			verify(service, never()).finish(any(), anyInt(), anyInt(), any(), anyList());

			release.countDown();
			collecting.get(5, TimeUnit.SECONDS);

			verify(service).finish(execution, 1, 0, cause, List.of());
		} finally {
			release.countDown();
			collecting.cancel(true);
			executor.shutdownNow();
			assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
		}
	}

	private NewsFetchResult.Success success(String symbol) {
		return new NewsFetchResult.Success(new CompanyNews(symbol, List.of()));
	}
}
