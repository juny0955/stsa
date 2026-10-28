package dev.junyoung.stsa.persistence.tooling;

import java.time.OffsetDateTime;
import java.util.stream.Stream;

import org.jooq.Field;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE;
import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE_STOCK;
import static dev.junyoung.stsa.persistence.jooq.Tables.STOCK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("공통 뉴스 스키마")
class NewsSchemaTests extends PostgresTestSupport {

	@Test
	@DisplayName("마이그레이션을 재실행해도 이미 적용한 스키마를 변경하지 않는다")
	void doesNotRepeatAppliedMigration() {
		assertThat(MigrateDatabase.configured(database()).migrate().migrationsExecuted).isZero();
		assertThat(sql.fetchCount(STOCK)).isZero();
		assertThat(sql.fetchCount(NEWS_ARTICLE)).isZero();
		assertThat(sql.fetchCount(NEWS_ARTICLE_STOCK)).isZero();
	}

	@Test
	@DisplayName("종목은 기본 활성 상태와 생성·수정 시각을 갖는다")
	void suppliesStockDefaults() {
		long id = stock("NVDA");

		var saved = sql.selectFrom(STOCK).where(STOCK.ID.eq(id)).fetchSingle();
		assertThat(saved.getEnabled()).isTrue();
		assertThat(saved.getName()).isNull();
		assertThat(saved.getCreatedAt()).isNotNull().isEqualTo(saved.getUpdatedAt());
	}

	@Test
	@DisplayName("기사의 게시 시각을 보존하고 선택 필드를 비워 둘 수 있다")
	void preservesPublicationInstantAndSuppliesArticleDefaults() {
		long id = article("finnhub", "100");

		var saved = sql.selectFrom(NEWS_ARTICLE).where(NEWS_ARTICLE.ID.eq(id)).fetchSingle();
		assertThat(saved.getPublishedAt().toInstant())
				.isEqualTo(OffsetDateTime.parse("2026-10-28T09:00:00+09:00").toInstant());
		assertThat(saved.getCategory()).isNull();
		assertThat(saved.getSource()).isNull();
		assertThat(saved.getSummary()).isNull();
		assertThat(saved.getImageUrl()).isNull();
		assertThat(saved.getRelated()).isNull();
		assertThat(saved.getCreatedAt()).isNotNull().isEqualTo(saved.getUpdatedAt());
	}

	@Test
	@DisplayName("같은 symbol을 가진 종목은 중복 등록할 수 없다")
	void rejectsDuplicateStockSymbol() {
		stock("NVDA");

		assertThatThrownBy(() -> stock("NVDA"))
				.isInstanceOfSatisfying(DataAccessException.class,
						error -> assertThat(error.sqlState()).isEqualTo("23505"));
	}

	@Test
	@DisplayName("같은 제공자의 같은 외부 기사 ID는 중복 등록할 수 없다")
	void rejectsDuplicateProviderArticleIdentifier() {
		article("finnhub", "100");

		assertThatThrownBy(() -> article("finnhub", "100"))
				.isInstanceOfSatisfying(DataAccessException.class,
						error -> assertThat(error.sqlState()).isEqualTo("23505"));
	}

	@ParameterizedTest
	@MethodSource("requiredArticleColumns")
	@DisplayName("기사의 필수 컬럼은 null을 허용하지 않는다")
	void rejectsMissingArticleField(Field<?> column) {
		var article = sql.newRecord(NEWS_ARTICLE);
		article.setProvider("finnhub");
		article.setExternalId("100");
		article.setPublishedAt(OffsetDateTime.parse("2026-10-28T00:00:00Z"));
		article.setHeadline("Company news");
		article.setUrl("https://example.com/news");
		article.set(column, null);

		assertThatThrownBy(article::store)
				.isInstanceOfSatisfying(DataAccessException.class,
						error -> assertThat(error.sqlState()).isEqualTo("23502"));
	}

	@Test
	@DisplayName("제공자가 다르면 외부 ID가 같아도 저장하고 URL 중복도 허용한다")
	void allowsProviderSpecificIdentifiersAndSharedUrls() {
		article("finnhub", "100");
		article("another", "100");
		article("finnhub", "101");

		assertThat(sql.fetchCount(NEWS_ARTICLE)).isEqualTo(3);
	}

	@Test
	@DisplayName("같은 기사를 여러 종목과 연결할 수 있다")
	void linksArticleToMultipleStocks() {
		long articleId = article("finnhub", "100");
		link(articleId, stock("NVDA"));
		link(articleId, stock("MSFT"));

		assertThat(sql.select(STOCK.SYMBOL).from(STOCK).join(NEWS_ARTICLE_STOCK)
				.on(STOCK.ID.eq(NEWS_ARTICLE_STOCK.STOCK_ID))
				.where(NEWS_ARTICLE_STOCK.ARTICLE_ID.eq(articleId)).fetch(STOCK.SYMBOL))
				.containsExactlyInAnyOrder("NVDA", "MSFT");
	}

	@Test
	@DisplayName("동일 기사와 종목의 연결은 중복 저장할 수 없다")
	void rejectsDuplicateArticleStockLink() {
		long articleId = article("finnhub", "100");
		long stockId = stock("NVDA");
		link(articleId, stockId);

		assertThatThrownBy(() -> link(articleId, stockId))
				.isInstanceOfSatisfying(DataAccessException.class,
						error -> assertThat(error.sqlState()).isEqualTo("23505"));
	}

	@Test
	@DisplayName("존재하지 않는 기사를 종목과 연결할 수 없다")
	void rejectsUnknownArticle() {
		long stockId = stock("NVDA");

		assertThatThrownBy(() -> link(999L, stockId))
				.isInstanceOfSatisfying(DataAccessException.class,
						error -> assertThat(error.sqlState()).isEqualTo("23503"));
	}

	@Test
	@DisplayName("존재하지 않는 종목을 기사와 연결할 수 없다")
	void rejectsUnknownStock() {
		long articleId = article("finnhub", "100");

		assertThatThrownBy(() -> link(articleId, 999L))
				.isInstanceOfSatisfying(DataAccessException.class,
						error -> assertThat(error.sqlState()).isEqualTo("23503"));
	}

	@Test
	@DisplayName("기사를 삭제하면 종목 연결도 삭제하며 종목 자체는 유지한다")
	void cascadesArticleDeletionToLinks() {
		long articleId = article("finnhub", "100");
		link(articleId, stock("NVDA"));

		sql.deleteFrom(NEWS_ARTICLE).where(NEWS_ARTICLE.ID.eq(articleId)).execute();

		assertThat(sql.fetchCount(NEWS_ARTICLE_STOCK)).isZero();
		assertThat(sql.fetchCount(STOCK)).isEqualTo(1);
	}

	@Test
	@DisplayName("기사와 연결된 종목은 삭제할 수 없지만 비활성화할 수 있다")
	void restrictsLinkedStockDeletionButAllowsDisabling() {
		long stockId = stock("NVDA");
		link(article("finnhub", "100"), stockId);

		assertThatThrownBy(() -> sql.deleteFrom(STOCK).where(STOCK.ID.eq(stockId)).execute())
				.isInstanceOfSatisfying(DataAccessException.class,
						error -> assertThat(error.sqlState()).isEqualTo("23001"));
		sql.update(STOCK).set(STOCK.ENABLED, false).where(STOCK.ID.eq(stockId)).execute();
		assertThat(sql.selectFrom(STOCK).where(STOCK.ID.eq(stockId)).fetchSingle().getEnabled()).isFalse();
		assertThat(sql.fetchCount(NEWS_ARTICLE_STOCK)).isEqualTo(1);
	}

	private static Stream<Field<?>> requiredArticleColumns() {
		return Stream.of(NEWS_ARTICLE.PROVIDER, NEWS_ARTICLE.EXTERNAL_ID, NEWS_ARTICLE.PUBLISHED_AT,
				NEWS_ARTICLE.HEADLINE, NEWS_ARTICLE.URL);
	}

	private long stock(String symbol) {
		return sql.insertInto(STOCK).set(STOCK.SYMBOL, symbol).returningResult(STOCK.ID).fetchSingle().value1();
	}

	private long article(String provider, String externalId) {
		return sql.insertInto(NEWS_ARTICLE)
				.set(NEWS_ARTICLE.PROVIDER, provider)
				.set(NEWS_ARTICLE.EXTERNAL_ID, externalId)
				.set(NEWS_ARTICLE.PUBLISHED_AT, OffsetDateTime.parse("2026-10-28T09:00:00+09:00"))
				.set(NEWS_ARTICLE.HEADLINE, "Company news")
				.set(NEWS_ARTICLE.URL, "https://example.com/news")
				.returningResult(NEWS_ARTICLE.ID).fetchSingle().value1();
	}

	private void link(long articleId, long stockId) {
		sql.insertInto(NEWS_ARTICLE_STOCK)
				.set(NEWS_ARTICLE_STOCK.ARTICLE_ID, articleId)
				.set(NEWS_ARTICLE_STOCK.STOCK_ID, stockId).execute();
	}
}
