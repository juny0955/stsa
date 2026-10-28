package dev.junyoung.stsa.collector.news;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.config.FixedRateTask;

import dev.junyoung.stsa.collector.news.client.FinnhubClient;
import dev.junyoung.stsa.collector.news.domain.CompanyNews;
import dev.junyoung.stsa.collector.news.scheduler.NewsScheduler;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisplayName("뉴스 수집 스케줄러")
class FinnhubResponseSchedulerTests {

	@Test
	@DisplayName("시작 즉시 실행하고 기본 30분 주기로 등록한다")
	void startsImmediatelyAndUsesThirtyMinuteIntervalByDefault() {
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicInteger subscriptions = new AtomicInteger();
		when(client.fetchCompanyNews(any(), any())).thenReturn(Flux.defer(() -> {
			subscriptions.incrementAndGet();
			return Flux.just(new CompanyNews("NVDA", List.of()));
		}));
		LocalDate before = LocalDate.now(ZoneOffset.UTC);

		contextRunner(client).run(context -> {
			assertThat(context).hasNotFailed().hasSingleBean(NewsScheduler.class);
			await().atMost(Duration.ofSeconds(5)).until(() -> subscriptions.get() == 1);
			LocalDate after = LocalDate.now(ZoneOffset.UTC);
			verify(client).fetchCompanyNews(
					argThat(from ->
						from.equals(before.minusDays(1)) || from.equals(after.minusDays(1))),
					argThat(to -> to.equals(before) || to.equals(after)));
			var processor = context.getBean(ScheduledAnnotationBeanPostProcessor.class);
			assertThat(processor.getScheduledTasks()).singleElement().satisfies(task -> {
				assertThat(task.getTask()).isInstanceOfSatisfying(FixedRateTask.class, fixedRate -> {
					assertThat(fixedRate.getIntervalDuration()).isEqualTo(Duration.ofMinutes(30));
					assertThat(fixedRate.getInitialDelayDuration()).isEqualTo(Duration.ZERO);
				});
			});
		});
	}

	@Test
	@DisplayName("설정한 주기마다 뉴스 조회를 다시 구독한다")
	void subscribesAgainOnEachScheduledExecution() {
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicInteger subscriptions = new AtomicInteger();
		when(client.fetchCompanyNews(any(), any())).thenReturn(Flux.defer(() -> {
			subscriptions.incrementAndGet();
			return Flux.empty();
		}));

		contextRunner(client).withPropertyValues("finnhub.news.interval=PT0.05S").run(context -> {
			assertThat(context).hasNotFailed();
			await().atMost(Duration.ofSeconds(5)).until(() -> subscriptions.get() >= 2);
			verify(client, atLeast(2)).fetchCompanyNews(any(), any());
		});
	}

	@Test
	@DisplayName("조회가 실패해도 다음 주기에 다시 실행한다")
	void continuesScheduledExecutionAfterAnError() {
		FinnhubClient client = mock(FinnhubClient.class);
		AtomicInteger successfulSubscriptions = new AtomicInteger();
		when(client.fetchCompanyNews(any(), any()))
				.thenReturn(Flux.error(new IllegalStateException("Temporary test failure")))
				.thenReturn(Flux.defer(() -> {
					successfulSubscriptions.incrementAndGet();
					return Flux.just(new CompanyNews("NVDA", List.of()));
				}));

		contextRunner(client).withPropertyValues("finnhub.news.interval=PT0.05S").run(context -> {
			assertThat(context).hasNotFailed();
			await().atMost(Duration.ofSeconds(5)).until(() -> successfulSubscriptions.get() >= 1);
			verify(client, atLeast(2)).fetchCompanyNews(any(), any());
		});
	}

	@Test
	@DisplayName("비활성화하면 스케줄러를 등록하지 않는다")
	void doesNotRegisterSchedulerWhenDisabled() {
		FinnhubClient client = mock(FinnhubClient.class);

		contextRunner(client).withPropertyValues("finnhub.news.enabled=false").run(context -> {
			assertThat(context).hasNotFailed().doesNotHaveBean(NewsScheduler.class);
			verifyNoInteractions(client);
		});
	}

	private ApplicationContextRunner contextRunner(FinnhubClient client) {
		return new ApplicationContextRunner()
				.withUserConfiguration(SchedulingConfiguration.class)
				.withBean(FinnhubClient.class, () -> client);
	}

	@TestConfiguration(proxyBeanMethods = false)
	@EnableScheduling
	@ComponentScan(basePackageClasses = NewsScheduler.class)
	static class SchedulingConfiguration {
	}
}
