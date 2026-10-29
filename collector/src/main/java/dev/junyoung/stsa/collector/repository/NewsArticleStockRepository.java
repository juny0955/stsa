package dev.junyoung.stsa.collector.repository;

import java.util.List;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE_STOCK;

@Repository
@RequiredArgsConstructor
public class NewsArticleStockRepository {

	private final DSLContext dsl;

	public void saveAll(long stockId, List<Long> articleIds) {
		if (articleIds.isEmpty()) {
			return;
		}
		var insert = dsl.insertInto(NEWS_ARTICLE_STOCK, NEWS_ARTICLE_STOCK.ARTICLE_ID, NEWS_ARTICLE_STOCK.STOCK_ID);
		for (Long articleId : articleIds) {
			insert.values(articleId, stockId);
		}
		insert.onConflict(NEWS_ARTICLE_STOCK.ARTICLE_ID, NEWS_ARTICLE_STOCK.STOCK_ID).doNothing().execute();
	}
}
