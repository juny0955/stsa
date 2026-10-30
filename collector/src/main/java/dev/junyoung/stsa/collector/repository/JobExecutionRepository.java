package dev.junyoung.stsa.collector.repository;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import dev.junyoung.stsa.collector.domain.JobExecution;
import dev.junyoung.stsa.collector.domain.JobStatus;
import lombok.RequiredArgsConstructor;

import static dev.junyoung.stsa.persistence.jooq.Tables.JOB_EXECUTION;

@Repository
@RequiredArgsConstructor
public class JobExecutionRepository {

	private final DSLContext dsl;

	public JobExecution save(JobExecution execution) {
		return dsl.insertInto(JOB_EXECUTION)
			.set(JOB_EXECUTION.JOB_NAME, execution.getJobName())
			.set(JOB_EXECUTION.PROCESSED_COUNT, execution.getProcessedCount())
			.set(JOB_EXECUTION.STATUS, execution.getStatus().name())
			.set(JOB_EXECUTION.SUCCESS_COUNT, execution.getSuccessCount())
			.set(JOB_EXECUTION.FAILURE_COUNT, execution.getFailureCount())
			.set(JOB_EXECUTION.ERROR_MESSAGE, execution.getErrorMessage())
			.returning()
			.fetchSingle(record -> JobExecution.builder()
				.id(record.getId())
				.jobName(record.getJobName())
				.status(JobStatus.valueOf(record.getStatus()))
				.startedAt(record.getStartedAt())
				.finishedAt(record.getFinishedAt())
				.processedCount(record.getProcessedCount())
				.successCount(record.getSuccessCount())
				.failureCount(record.getFailureCount())
				.errorMessage(record.getErrorMessage())
				.createdAt(record.getCreatedAt())
				.updatedAt(record.getUpdatedAt())
				.build());
	}

	public void update(JobExecution execution) {
		int updated = dsl.update(JOB_EXECUTION)
			.set(JOB_EXECUTION.STATUS, execution.getStatus().name())
			.set(JOB_EXECUTION.SUCCESS_COUNT, execution.getSuccessCount())
			.set(JOB_EXECUTION.FAILURE_COUNT, execution.getFailureCount())
			.set(JOB_EXECUTION.ERROR_MESSAGE, execution.getErrorMessage())
			.set(JOB_EXECUTION.FINISHED_AT, execution.getFinishedAt())
			.set(JOB_EXECUTION.UPDATED_AT, execution.getUpdatedAt())
			.where(JOB_EXECUTION.ID.eq(execution.getId()))
			.execute();
		if (updated != 1) {
			throw new IllegalStateException("Expected to update one job execution: " + execution.getId());
		}
	}
}
