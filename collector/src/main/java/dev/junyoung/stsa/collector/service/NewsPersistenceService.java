package dev.junyoung.stsa.collector.service;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dev.junyoung.stsa.collector.domain.CompanyNews;
import dev.junyoung.stsa.collector.domain.NewsArticle;
import dev.junyoung.stsa.collector.news.client.dto.FinnhubResponse;
import dev.junyoung.stsa.collector.repository.NewsArticleRepository;
import dev.junyoung.stsa.collector.repository.NewsArticleStockRepository;
import dev.junyoung.stsa.collector.repository.StockRepository;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class NewsPersistenceService {

	private static final String PROVIDER = "finnhub";
	private static final int CHUNK_SIZE = 500;

	private final StockRepository stockRepository;
	private final NewsArticleRepository articleRepository;
	private final NewsArticleStockRepository articleStockRepository;

	@Transactional
	public void save(CompanyNews news) {
		long stockId = stockRepository.findIdBySymbol(news.symbol())
			.orElseThrow(() -> new IllegalArgumentException("Stock must be registered before collecting news: " + news.symbol()));

		Map<Long, NewsArticle> unique = new LinkedHashMap<>();
		for (FinnhubResponse response : news.articles()) {
			unique.computeIfAbsent(response.id(), ignored -> new NewsArticle(
				PROVIDER, Long.toString(response.id()), Instant.ofEpochSecond(response.datetime()).atOffset(ZoneOffset.UTC),
				response.headline(), response.url(), response.category(), response.source(), response.summary(),
				response.image(), response.related()));
		}

		List<NewsArticle> articles = new ArrayList<>(unique.values());
		for (int offset = 0; offset < articles.size(); offset += CHUNK_SIZE) {
			List<NewsArticle> chunk = articles.subList(offset, Math.min(offset + CHUNK_SIZE, articles.size()));
			articleRepository.saveAll(chunk);
			List<Long> articleIds = articleRepository.findIds(PROVIDER, chunk.stream().map(NewsArticle::externalId).toList());
			if (articleIds.size() != chunk.size()) {
				throw new IllegalStateException("Could not resolve all article IDs for stock: " + news.symbol());
			}
			articleStockRepository.saveAll(stockId, articleIds);
		}
	}
}
