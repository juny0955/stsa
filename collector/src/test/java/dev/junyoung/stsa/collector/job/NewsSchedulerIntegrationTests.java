package dev.junyoung.stsa.collector.job;

import java.util.List;
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

import static dev.junyoung.stsa.persistence.jooq.Tables.JOB_EXECUTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("스케줄러 작업 실행 이력 통합")
class NewsSchedulerIntegrationTests extends JobExecutionTestSupport {

	@Test
	@DisplayName("API 요청 전에 RUNNING을 커밋하고 수집 종료 뒤 같은 실행을 완료한다")
	void commitsStartBeforeFetchingAndFinishesTheSameExecution() {
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

		new NewsScheduler(service, client).collectNews();

		var finished = observer.selectFrom(JOB_EXECUTION).fetchSingle();
		assertThat(finished.getId()).isEqualTo(started.get().getId());
		assertThat(finished.getStatus()).isEqualTo("SUCCESS");
		assertThat(finished.getProcessedCount()).isEqualTo(3);
		assertThat(finished.getSuccessCount()).isEqualTo(3);
		assertThat(finished.getFailureCount()).isZero();
		assertThat(finished.getFinishedAt()).isNotNull();
		assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
	}
}
