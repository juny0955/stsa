package dev.junyoung.stsa.collector.job;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.junyoung.stsa.collector.domain.CompanyNews;
import dev.junyoung.stsa.collector.news.client.dto.FinnhubResponse;

import static dev.junyoung.stsa.collector.job.NewsTestData.article;
import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE;
import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE_STOCK;
import static dev.junyoung.stsa.persistence.jooq.Tables.STOCK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("종목별 뉴스 일괄 저장")
class NewsPersistenceIntegrationTests extends JobExecutionTestSupport {

	@Test
	@DisplayName("기사의 모든 필드와 게시 시각을 저장하고 요청 종목과 연결한다")
	void savesArticleFieldsAndRequestedStockLinks() {
		long stockId = registerStock("NVDA");

		newsPersistenceService.save(new CompanyNews("NVDA", List.of(article(101), article(102))));

		var saved = observer.selectFrom(NEWS_ARTICLE).where(NEWS_ARTICLE.EXTERNAL_ID.eq("101")).fetchSingle();
		assertThat(saved.getId()).isPositive();
		assertThat(saved.getProvider()).isEqualTo("finnhub");
		assertThat(saved.getPublishedAt().toInstant()).isEqualTo(Instant.parse("2023-11-14T22:13:20Z"));
		assertThat(saved.getHeadline()).isEqualTo("Article 101");
		assertThat(saved.getUrl()).isEqualTo("https://example.com/news/101");
		assertThat(saved.getCategory()).isEqualTo("company");
		assertThat(saved.getSource()).isEqualTo("Example News");
		assertThat(saved.getSummary()).isEqualTo("Summary 101");
		assertThat(saved.getImageUrl()).isEqualTo("https://example.com/image/101");
		assertThat(saved.getRelated()).isEqualTo("NVDA,AAPL");
		assertThat(saved.getCreatedAt()).isNotNull().isEqualTo(saved.getUpdatedAt());
		assertThat(observer.fetchCount(NEWS_ARTICLE)).isEqualTo(2);
		assertThat(observer.selectFrom(NEWS_ARTICLE_STOCK).fetch(NEWS_ARTICLE_STOCK.STOCK_ID))
			.containsExactly(stockId, stockId);
		assertThat(observer.fetchCount(STOCK)).isEqualTo(1);
	}

	@Test
	@DisplayName("재수집해도 기사 내용과 시각을 변경하거나 기사·연결을 중복 생성하지 않는다")
	void retainsExistingArticleAndLinkOnRepeatedCollection() {
		registerStock("NVDA");
		newsPersistenceService.save(new CompanyNews("NVDA", List.of(article(101, "Original"))));
		var original = observer.selectFrom(NEWS_ARTICLE).fetchSingle();

		newsPersistenceService.save(new CompanyNews("NVDA", List.of(article(101, "Changed"))));

		assertThat(observer.selectFrom(NEWS_ARTICLE).fetchSingle()).isEqualTo(original);
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(1);
	}

	@Test
	@DisplayName("다른 종목에서 같은 기사를 수집하면 기존 기사에 종목 연결만 추가한다")
	void sharesOneArticleBetweenStocks() {
		long nvda = registerStock("NVDA");
		long aapl = registerStock("AAPL");
		newsPersistenceService.save(new CompanyNews("NVDA", List.of(article(101, "Original"))));

		newsPersistenceService.save(new CompanyNews("AAPL", List.of(article(101, "Changed"))));

		var saved = observer.selectFrom(NEWS_ARTICLE).fetchSingle();
		assertThat(saved.getHeadline()).isEqualTo("Original");
		assertThat(observer.selectFrom(NEWS_ARTICLE_STOCK).fetch(NEWS_ARTICLE_STOCK.ARTICLE_ID))
			.containsExactly(saved.getId(), saved.getId());
		assertThat(observer.selectFrom(NEWS_ARTICLE_STOCK).fetch(NEWS_ARTICLE_STOCK.STOCK_ID))
			.containsExactlyInAnyOrder(nvda, aapl);
	}

	@Test
	@DisplayName("등록된 종목의 빈 응답은 기사나 연결을 생성하지 않는다")
	void acceptsEmptyResponseForRegisteredStock() {
		registerStock("NVDA");

		newsPersistenceService.save(new CompanyNews("NVDA", List.of()));

		assertThat(observer.fetchCount(NEWS_ARTICLE)).isZero();
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isZero();
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	@DisplayName("응답에 기사 유무와 무관하게 미등록 종목은 저장하지 않는다")
	void rejectsUnregisteredStockWithoutCreatingIt(boolean hasArticles) {
		var news = new CompanyNews("UNKNOWN", hasArticles ? List.of(article(101)) : List.of());

		assertThatThrownBy(() -> newsPersistenceService.save(news))
			.isInstanceOf(IllegalArgumentException.class).hasMessageContaining("UNKNOWN");

		assertThat(observer.fetchCount(STOCK)).isZero();
		assertThat(observer.fetchCount(NEWS_ARTICLE)).isZero();
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isZero();
	}

	@Test
	@DisplayName("응답 내부의 중복 ID는 첫 기사만 사용한다")
	void retainsFirstArticleForDuplicateIdsWithinResponse() {
		registerStock("NVDA");

		newsPersistenceService.save(new CompanyNews("NVDA", List.of(article(101, "First"), article(101, "Second"))));

		assertThat(observer.selectFrom(NEWS_ARTICLE).fetchSingle().getHeadline()).isEqualTo("First");
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(1);
	}

	@Test
	@DisplayName("500건을 넘는 여러 청크의 기사와 연결을 모두 저장한다")
	void savesMultipleChunks() {
		long stockId = registerStock("NVDA");
		var articles = LongStream.rangeClosed(1, 1_201).mapToObj(NewsTestData::article).toList();

		newsPersistenceService.save(new CompanyNews("NVDA", articles));

		assertThat(observer.fetchCount(NEWS_ARTICLE)).isEqualTo(1_201);
		assertThat(observer.selectCount().from(NEWS_ARTICLE_STOCK)
			.where(NEWS_ARTICLE_STOCK.STOCK_ID.eq(stockId)).fetchSingle().value1()).isEqualTo(1_201);
	}

	@Test
	@DisplayName("후속 청크의 기사 저장 실패 시 앞선 청크와 연결도 롤백한다")
	void rollsBackEarlierChunksWhenLaterArticleFails() {
		registerStock("NVDA");
		List<FinnhubResponse> articles = new ArrayList<>(LongStream.rangeClosed(1, 500)
			.mapToObj(NewsTestData::article).toList());
		articles.add(article(501, null));

		assertThatThrownBy(() -> newsPersistenceService.save(new CompanyNews("NVDA", articles)))
			.hasMessageContaining("headline");

		assertThat(observer.fetchCount(NEWS_ARTICLE)).isZero();
		assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isZero();
		assertThat(observer.fetchCount(STOCK)).isEqualTo(1);
	}

	@Test
	@DisplayName("기사 저장 후 연결 저장이 실패하면 기사도 롤백한다")
	void rollsBackArticlesWhenLinkInsertionFails() {
		long stockId = registerStock("NVDA");
		sql.execute("ALTER TABLE news_article_stock ADD CONSTRAINT test_reject_links CHECK (stock_id <> " + stockId + ")");
		try {
			assertThatThrownBy(() -> newsPersistenceService.save(new CompanyNews("NVDA", List.of(article(101)))))
				.hasMessageContaining("test_reject_links");

			assertThat(observer.fetchCount(NEWS_ARTICLE)).isZero();
			assertThat(observer.fetchCount(NEWS_ARTICLE_STOCK)).isZero();
		} finally {
			sql.execute("ALTER TABLE news_article_stock DROP CONSTRAINT test_reject_links");
		}
	}
}
