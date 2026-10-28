package dev.junyoung.stsa.collector.news.scheduler;

import java.time.LocalDate;
import java.time.ZoneOffset;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
@Component
@ConditionalOnProperty(prefix = "finnhub.news", name = "enabled", havingValue = "true", matchIfMissing = true)
public class NewsScheduler {

	private final FinnhubClient client;

	@Scheduled(fixedRateString = "${finnhub.news.interval:PT30M}", initialDelay = 0)
	public void collectNews() {
		LocalDate today = LocalDate.now(ZoneOffset.UTC);
		try {
			client.fetchCompanyNews(today.minusDays(1), today)
				.doOnNext(company -> log.info("{} 뉴스 {}건 조회 완료", company.symbol(), company.articles().size()))
				.blockLast();
		} catch (RuntimeException e) {
			log.error("뉴스 조회 실패. 다음 주기에 다시 시도합니다.", e);
		}
	}
}
