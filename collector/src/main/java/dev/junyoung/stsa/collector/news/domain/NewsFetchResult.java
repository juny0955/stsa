package dev.junyoung.stsa.collector.news.domain;

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
