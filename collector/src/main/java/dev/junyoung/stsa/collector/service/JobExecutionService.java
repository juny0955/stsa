package dev.junyoung.stsa.collector.service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dev.junyoung.stsa.collector.domain.JobExecution;
import dev.junyoung.stsa.collector.domain.JobStatus;
import dev.junyoung.stsa.collector.repository.JobExecutionRepository;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class JobExecutionService {

	private final JobExecutionRepository jobExecutionRepository;

	@Transactional
	public JobExecution start(String jobName, int processedCount) {
		return jobExecutionRepository.save(JobExecution.builder()
			.jobName(jobName)
			.status(JobStatus.RUNNING)
			.processedCount(processedCount)
			.build());
	}

	@Transactional
	public void finish(JobExecution execution, int successCount, int failureCount, Throwable executionError) {
		JobStatus status;
		if (executionError != null || successCount + failureCount != execution.getProcessedCount()) {
			status = JobStatus.FAILED;
		} else if (failureCount == 0) {
			status = JobStatus.SUCCESS;
		} else if (successCount > 0) {
			status = JobStatus.PARTIAL_SUCCESS;
		} else {
			status = JobStatus.FAILED;
		}

		OffsetDateTime finishedAt = OffsetDateTime.now(ZoneOffset.UTC);
		jobExecutionRepository.update(execution.toBuilder()
			.status(status)
			.successCount(successCount)
			.failureCount(failureCount)
			.errorMessage(executionError == null ? null : executionError.getMessage())
			.finishedAt(finishedAt)
			.updatedAt(finishedAt)
			.build());
	}
}
