package dev.junyoung.stsa.collector.job;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import dev.junyoung.stsa.collector.domain.CompanyNews;
import dev.junyoung.stsa.collector.domain.JobExecution;
import dev.junyoung.stsa.collector.domain.JobStatus;
import dev.junyoung.stsa.collector.news.NewsFetchResult;
import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import dev.junyoung.stsa.collector.scheduler.NewsScheduler;
import dev.junyoung.stsa.collector.service.JobExecutionService;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@DisplayName("스케줄러 실행 이력 실패 처리")
class NewsSchedulerExecutionTests {

	private final JobExecutionService service = mock(JobExecutionService.class);
	private final FinnhubClient client = mock(FinnhubClient.class);
	private final JobExecution execution = JobExecution.builder()
		.id(42L)
		.jobName("News Collect")
		.processedCount(3)
		.status(JobStatus.RUNNING)
		.build();
	private final NewsScheduler scheduler = new NewsScheduler(service, client);

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

		verifyNoInteractions(client);
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

		verify(service).finish(execution, 1, 0, cause);
	}

	@Test
	@DisplayName("종목별 실패는 집계하되 전체 실행 예외와 구분한다")
	void distinguishesSymbolFailuresFromExecutionErrors() {
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.just(success("NVDA"),
				new NewsFetchResult.Failure("AAPL", new IllegalStateException("HTTP failed")), success("MSFT")));

		scheduler.collectNews();

		verify(service).finish(execution, 2, 1, null);
	}

	@Test
	@DisplayName("응답이 없으면 미응답 종목을 실패 건수에 추가하지 않는다")
	void forwardsZeroCountsForEmptyStream() {
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.empty());

		scheduler.collectNews();

		verify(service).finish(execution, 0, 0, null);
	}

	@Test
	@DisplayName("수집과 종료 갱신이 모두 실패해도 예외를 전파하지 않는다")
	void handlesCollectionAndFinishFailures() {
		RuntimeException fetchError = new IllegalStateException("Fetch failed");
		RuntimeException updateError = new IllegalStateException("Update failed");
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(Flux.error(fetchError));
		doThrow(updateError).when(service).finish(execution, 0, 0, fetchError);

		assertThatCode(scheduler::collectNews).doesNotThrowAnyException();

		verify(service).finish(execution, 0, 0, fetchError);
	}

	@Test
	@DisplayName("종료 갱신이 실패해도 다음 실행에서 다시 수집하고 새로운 실행을 종료한다")
	void continuesAfterFinishFailure() {
		JobExecution next = execution.toBuilder().id(43L).build();
		when(service.start("News Collect", 3)).thenReturn(execution, next);
		when(client.fetchCompanyNews(any(), any(), any())).thenReturn(
				Flux.just(success("NVDA"), success("AAPL"), success("MSFT")));
		doThrow(new IllegalStateException("Update failed")).doNothing()
				.when(service).finish(any(), anyInt(), anyInt(), isNull());

		scheduler.collectNews();
		scheduler.collectNews();

		verify(client, times(2)).fetchCompanyNews(any(), any(), any());
		verify(service).finish(execution, 3, 0, null);
		verify(service).finish(next, 3, 0, null);
	}

	private NewsFetchResult.Success success(String symbol) {
		return new NewsFetchResult.Success(new CompanyNews(symbol, List.of()));
	}
}
