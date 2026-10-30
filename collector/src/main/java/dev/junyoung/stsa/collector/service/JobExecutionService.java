package dev.junyoung.stsa.collector.service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.StringJoiner;

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
	public void finish(JobExecution execution, int successCount, int failureCount,
			Throwable executionError, List<String> failureMessages) {
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

		StringJoiner errors = new StringJoiner("\n");
		failureMessages.forEach(errors::add);
		if (executionError != null) {
			String message = executionError.getMessage();
			errors.add(message == null || message.isBlank() ? executionError.getClass().getName() : message);
		} else if (successCount + failureCount != execution.getProcessedCount()) {
			errors.add("응답 수 불일치: 대상 %d종목, 성공 %d종목, 실패 %d종목"
				.formatted(execution.getProcessedCount(), successCount, failureCount));
		}

		OffsetDateTime finishedAt = OffsetDateTime.now(ZoneOffset.UTC);
		jobExecutionRepository.update(execution.toBuilder()
			.status(status)
			.successCount(successCount)
			.failureCount(failureCount)
			.errorMessage(errors.length() == 0 ? null : errors.toString())
			.finishedAt(finishedAt)
			.updatedAt(finishedAt)
			.build());
	}
}
