package dev.junyoung.stsa.collector.domain;

import java.time.OffsetDateTime;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder(toBuilder = true)
public class JobExecution {
	private Long id;
	private String jobName;
	private JobStatus status;
	private OffsetDateTime startedAt;
	private OffsetDateTime finishedAt;
	private int processedCount;
	private int successCount;
	private int failureCount;
	private String errorMessage;
	private OffsetDateTime createdAt;
	private OffsetDateTime updatedAt;
}
