package dev.junyoung.stsa.collector.repository;

import java.util.List;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import dev.junyoung.stsa.collector.domain.NewsArticle;
import lombok.RequiredArgsConstructor;

import static dev.junyoung.stsa.persistence.jooq.Tables.NEWS_ARTICLE;

@Repository
@RequiredArgsConstructor
public class NewsArticleRepository {

	private final DSLContext dsl;

	public void saveAll(List<NewsArticle> articles) {
		if (articles.isEmpty()) {
			return;
		}
		var insert = dsl.insertInto(NEWS_ARTICLE,
			NEWS_ARTICLE.PROVIDER, NEWS_ARTICLE.EXTERNAL_ID, NEWS_ARTICLE.PUBLISHED_AT,
			NEWS_ARTICLE.HEADLINE, NEWS_ARTICLE.URL, NEWS_ARTICLE.CATEGORY,
			NEWS_ARTICLE.SOURCE, NEWS_ARTICLE.SUMMARY, NEWS_ARTICLE.IMAGE_URL, NEWS_ARTICLE.RELATED);
		for (NewsArticle article : articles) {
			insert.values(article.provider(), article.externalId(), article.publishedAt(),
				article.headline(), article.url(), article.category(), article.source(),
				article.summary(), article.imageUrl(), article.related());
		}
		insert.onConflict(NEWS_ARTICLE.PROVIDER, NEWS_ARTICLE.EXTERNAL_ID).doNothing().execute();
	}

	public List<Long> findIds(String provider, List<String> externalIds) {
		return dsl.select(NEWS_ARTICLE.ID).from(NEWS_ARTICLE)
			.where(NEWS_ARTICLE.PROVIDER.eq(provider).and(NEWS_ARTICLE.EXTERNAL_ID.in(externalIds)))
			.fetch(NEWS_ARTICLE.ID);
	}
}
