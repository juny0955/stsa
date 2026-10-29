package dev.junyoung.stsa.collector.job;

import dev.junyoung.stsa.collector.news.client.dto.FinnhubResponse;

final class NewsTestData {

	private NewsTestData() {
	}

	static FinnhubResponse article(long id) {
		return article(id, "Article " + id);
	}

	static FinnhubResponse article(long id, String headline) {
		return new FinnhubResponse(id, "company", 1_700_000_000L, headline,
			"https://example.com/image/" + id, "NVDA,AAPL", "Example News", "Summary " + id,
			"https://example.com/news/" + id);
	}
}
