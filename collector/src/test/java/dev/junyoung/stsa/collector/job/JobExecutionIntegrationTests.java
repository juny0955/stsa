package dev.junyoung.stsa.collector.job;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.junyoung.stsa.collector.domain.JobStatus;

import static dev.junyoung.stsa.persistence.jooq.Tables.JOB_EXECUTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("작업 실행 이력 저장·갱신")
class JobExecutionIntegrationTests extends JobExecutionTestSupport {

	@Test
	@DisplayName("시작 행을 DB 기본 시각과 함께 저장하고 독립 연결에서 즉시 조회할 수 있다")
	void commitsRunningExecutionWithDatabaseDefaults() {
		var execution = service.start("News Collect", 3);

		var saved = observer.selectFrom(JOB_EXECUTION).where(JOB_EXECUTION.ID.eq(execution.getId())).fetchSingle();
		assertThat(execution.getId()).isPositive();
		assertThat(execution.getJobName()).isEqualTo("News Collect");
		assertThat(execution.getStatus()).isEqualTo(JobStatus.RUNNING);
		assertThat(execution.getStartedAt()).isNotNull().isEqualTo(saved.getStartedAt());
		assertThat(execution.getCreatedAt()).isNotNull().isEqualTo(saved.getCreatedAt());
		assertThat(execution.getUpdatedAt()).isNotNull().isEqualTo(saved.getUpdatedAt());
		assertThat(execution.getFinishedAt()).isNull();
		assertThat(saved.getStatus()).isEqualTo("RUNNING");
		assertThat(saved.getProcessedCount()).isEqualTo(3);
		assertThat(saved.getSuccessCount()).isZero();
		assertThat(saved.getFailureCount()).isZero();
		assertThat(saved.getErrorMessage()).isNull();
	}

	@ParameterizedTest
	@CsvSource({"3,0,SUCCESS", "2,1,PARTIAL_SUCCESS", "0,3,FAILED", "0,0,FAILED", "1,0,FAILED"})
	@DisplayName("처리 결과에 맞춰 같은 행을 갱신하고 시작 정보와 대상 수를 유지한다")
	void commitsFinalResult(int succeeded, int failed, JobStatus expected) {
		var execution = service.start("News Collect", 3);

		service.finish(execution, succeeded, failed, null);

		var saved = observer.selectFrom(JOB_EXECUTION).where(JOB_EXECUTION.ID.eq(execution.getId())).fetchSingle();
		assertThat(saved.getStatus()).isEqualTo(expected.name());
		assertThat(saved.getSuccessCount()).isEqualTo(succeeded);
		assertThat(saved.getFailureCount()).isEqualTo(failed);
		assertThat(saved.getProcessedCount()).isEqualTo(3);
		assertThat(saved.getJobName()).isEqualTo(execution.getJobName());
		assertThat(saved.getStartedAt()).isEqualTo(execution.getStartedAt());
		assertThat(saved.getCreatedAt()).isEqualTo(execution.getCreatedAt());
		assertThat(saved.getFinishedAt()).isNotNull();
		assertThat(saved.getFinishedAt().toInstant()).isAfterOrEqualTo(saved.getStartedAt().toInstant());
		assertThat(saved.getUpdatedAt()).isEqualTo(saved.getFinishedAt());
		assertThat(saved.getErrorMessage()).isNull();
		assertThat(observer.fetchCount(JOB_EXECUTION)).isEqualTo(1);
	}

	@ParameterizedTest
	@ValueSource(ints = {0, 1, 3})
	@DisplayName("전체 실행 오류가 있으면 성공 건수와 무관하게 FAILED와 원인을 기록한다")
	void failsOnExecutionErrorWithoutInventingMissingResults(int succeeded) {
		var execution = service.start("News Collect", 3);

		service.finish(execution, succeeded, 0, new IllegalStateException("Fetch failed"));

		var saved = observer.selectFrom(JOB_EXECUTION).where(JOB_EXECUTION.ID.eq(execution.getId())).fetchSingle();
		assertThat(saved.getStatus()).isEqualTo("FAILED");
		assertThat(saved.getSuccessCount()).isEqualTo(succeeded);
		assertThat(saved.getFailureCount()).isZero();
		assertThat(saved.getProcessedCount()).isEqualTo(3);
		assertThat(saved.getErrorMessage()).isEqualTo("Fetch failed");
	}

	@Test
	@DisplayName("수집을 반복하면 새로운 실행 행을 생성한다")
	void createsDistinctRowsForEachRun() {
		var first = service.start("News Collect", 3);
		service.finish(first, 3, 0, null);

		var second = service.start("News Collect", 3);

		assertThat(second.getId()).isNotEqualTo(first.getId());
		assertThat(observer.fetchCount(JOB_EXECUTION)).isEqualTo(2);
		assertThat(observer.selectFrom(JOB_EXECUTION).where(JOB_EXECUTION.ID.eq(first.getId())).fetchSingle()
				.getStatus()).isEqualTo("SUCCESS");
		assertThat(observer.selectFrom(JOB_EXECUTION).where(JOB_EXECUTION.ID.eq(second.getId())).fetchSingle()
				.getStatus()).isEqualTo("RUNNING");
	}

	@Test
	@DisplayName("존재하지 않는 실행의 갱신을 성공으로 처리하지 않는다")
	void rejectsMissingExecutionUpdate() {
		var execution = service.start("News Collect", 3);

		assertThatThrownBy(() -> service.finish(execution.toBuilder().id(-1L).build(), 3, 0, null))
				.isInstanceOf(IllegalStateException.class);
		assertThat(observer.selectFrom(JOB_EXECUTION).where(JOB_EXECUTION.ID.eq(execution.getId())).fetchSingle()
				.getStatus()).isEqualTo("RUNNING");
	}

	@Test
	@DisplayName("종료 갱신의 DB 오류가 이미 커밋한 시작 행을 롤백시키지 않는다")
	void retainsCommittedStartWhenFinishFails() {
		var execution = service.start("News Collect", 3);

		assertThatThrownBy(() -> service.finish(execution, 4, 0, null))
				.hasMessageContaining("job_execution_count_ck");

		var saved = observer.selectFrom(JOB_EXECUTION).where(JOB_EXECUTION.ID.eq(execution.getId())).fetchSingle();
		assertThat(saved.getStatus()).isEqualTo("RUNNING");
		assertThat(saved.getSuccessCount()).isZero();
		assertThat(saved.getFinishedAt()).isNull();
	}

	@Test
	@DisplayName("결과 갱신은 전달 객체의 변경된 시작 정보와 대상 수를 반영하지 않는다")
	void onlyUpdatesResultColumns() {
		var execution = service.start("News Collect", 3);
		OffsetDateTime finishedAt = OffsetDateTime.now(ZoneOffset.UTC);
		var result = execution.toBuilder().jobName("Different job").processedCount(99)
				.startedAt(execution.getStartedAt().minusDays(1))
				.createdAt(execution.getCreatedAt().minusDays(1))
				.status(JobStatus.SUCCESS).successCount(3).finishedAt(finishedAt).updatedAt(finishedAt).build();

		repository.update(result);

		var saved = observer.selectFrom(JOB_EXECUTION).where(JOB_EXECUTION.ID.eq(execution.getId())).fetchSingle();
		assertThat(saved.getJobName()).isEqualTo("News Collect");
		assertThat(saved.getProcessedCount()).isEqualTo(3);
		assertThat(saved.getStartedAt()).isEqualTo(execution.getStartedAt());
		assertThat(saved.getCreatedAt()).isEqualTo(execution.getCreatedAt());
		assertThat(saved.getStatus()).isEqualTo("SUCCESS");
	}
}
