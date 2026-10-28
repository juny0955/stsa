package dev.junyoung.stsa.collector.news.domain;

import java.util.List;

import dev.junyoung.stsa.collector.news.client.dto.FinnhubResponse;

public record CompanyNews(
	String symbol,
	List<FinnhubResponse> articles
) {
	public CompanyNews {
		articles = List.copyOf(articles);
	}
}
