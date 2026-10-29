package dev.junyoung.stsa.collector.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.LongStream;

import org.jooq.ExecuteContext;
import org.jooq.ExecuteListener;
import org.jooq.ExecuteListenerProvider;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import dev.junyoung.stsa.collector.domain.CompanyNews;

import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE;
import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE_STOCK;
import static org.assertj.core.api.Assertions.assertThat;

@Import(NewsPersistenceBatchTests.CounterConfig.class)
@DisplayName("뉴스 일괄 저장 SQL 실행 횟수")
class NewsPersistenceBatchTests extends JobExecutionTestSupport {

	@Autowired
	QueryCounter counter;

	@Test
	@DisplayName("200건의 기사와 연결을 개별 INSERT 없이 두 번의 INSERT로 저장한다")
	void insertsMultipleArticlesWithTwoStatements() {
		registerStock("NVDA");
		var articles = LongStream.rangeClosed(1, 200).mapToObj(NewsTestData::article).toList();
		counter.statements.clear();

		newsPersistenceService.save(new CompanyNews("NVDA", articles));

		assertThat(counter.insertCount()).isEqualTo(2);
		assertThat(counter.statements).hasSize(4);
		assertThat(observer.fetchCount(NEWS_ARTICLE)).isEqualTo(200);
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(200);
	}

	@Test
	@DisplayName("501건은 500건과 1건으로 나눠 기사·연결 INSERT를 네 번 실행한다")
	void splitsStatementsAtFiveHundredArticles() {
		registerStock("NVDA");
		var articles = LongStream.rangeClosed(1, 501).mapToObj(NewsTestData::article).toList();
		counter.statements.clear();

		newsPersistenceService.save(new CompanyNews("NVDA", articles));

		assertThat(counter.insertCount()).isEqualTo(4);
		assertThat(counter.statements).hasSize(7);
		assertThat(observer.fetchCount(NEWS_ARTICLE)).isEqualTo(501);
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(501);
	}

	@Test
	@DisplayName("빈 응답에는 종목 조회만 실행하고 INSERT를 실행하지 않는다")
	void onlyLooksUpStockForEmptyResponse() {
		registerStock("NVDA");
		counter.statements.clear();

		newsPersistenceService.save(new CompanyNews("NVDA", List.of()));

		assertThat(counter.insertCount()).isZero();
		assertThat(counter.statements).hasSize(1);
	}

	static class QueryCounter implements ExecuteListener {

		final List<String> statements = new ArrayList<>();

		@Override
		public void executeStart(ExecuteContext context) {
			statements.add(context.sql());
		}

		long insertCount() {
			return statements.stream().filter(statement -> statement.toLowerCase(Locale.ROOT).startsWith("insert into")).count();
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class CounterConfig {

		@Bean
		QueryCounter queryCounter() {
			return new QueryCounter();
		}

		@Bean
		ExecuteListenerProvider queryCounterProvider(QueryCounter counter) {
			return new DefaultExecuteListenerProvider(counter);
		}
	}
}
