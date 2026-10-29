package dev.junyoung.stsa.collector.scheduler;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.junyoung.stsa.collector.domain.JobExecution;
import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import dev.junyoung.stsa.collector.news.NewsFetchResult;
import dev.junyoung.stsa.collector.service.JobExecutionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
@Component
@ConditionalOnProperty(prefix = "finnhub.news", name = "enabled", havingValue = "true", matchIfMissing = true)
public class NewsScheduler {

	private static final String JOB_NAME = "News Collect";
	private static final List<String> SYMBOLS = List.of("NVDA", "AAPL", "MSFT");

	private final JobExecutionService jobExecutionService;

	private final FinnhubClient client;

	@Scheduled(fixedRateString = "${finnhub.news.interval:PT30M}", initialDelay = 0)
	public void collectNews() {
		LocalDate today = LocalDate.now(ZoneOffset.UTC);
		AtomicInteger succeeded = new AtomicInteger();
		AtomicInteger failed = new AtomicInteger();
		JobExecution execution;
		try {
			execution = jobExecutionService.start(JOB_NAME, SYMBOLS.size());
		} catch (RuntimeException e) {
			log.error("뉴스 수집 실행 이력 저장 실패. 수집을 시작하지 않습니다.", e);
			return;
		}
		RuntimeException executionError = null;
		try {
			client.fetchCompanyNews(SYMBOLS, today.minusDays(1), today)
				.doOnNext(result -> {
					switch (result) {
						case NewsFetchResult.Success success -> {
							succeeded.incrementAndGet();
							log.debug("{} 뉴스 {}건 조회 완료", success.symbol(), success.news().articles().size());
						}
						case NewsFetchResult.Failure failure -> {
							failed.incrementAndGet();
							log.error("Finnhub {} 뉴스 조회 실패", failure.symbol(), failure.cause());
						}
					}
				})
				.doOnComplete(() -> log.info("Finnhub 뉴스 수집 완료: 성공 {}종목, 실패 {}종목", succeeded.get(), failed.get()))
				.blockLast();
		} catch (RuntimeException e) {
			executionError = e;
			log.error("뉴스 조회 실패. 다음 주기에 다시 시도합니다.", e);
		} finally {
			try {
				jobExecutionService.finish(execution, succeeded.get(), failed.get(), executionError);
			} catch (RuntimeException e) {
				log.error("뉴스 수집 실행 이력 갱신 실패: executionId={}", execution.getId(), e);
			}
		}
	}
}
