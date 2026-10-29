package dev.junyoung.stsa.collector.news;

import dev.junyoung.stsa.collector.domain.CompanyNews;

public sealed interface NewsFetchResult {

	String symbol();

	record Success(CompanyNews news) implements NewsFetchResult {

		@Override
		public String symbol() {
			return news.symbol();
		}
	}

	record Failure(String symbol, Throwable cause) implements NewsFetchResult {
	}
}
