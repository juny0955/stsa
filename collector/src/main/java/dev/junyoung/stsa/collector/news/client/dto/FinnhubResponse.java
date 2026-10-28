package dev.junyoung.stsa.collector.news.client.dto;

public record FinnhubResponse(
	long id,
	String category,
	long datetime,
	String headline,
	String image,
	String related,
	String source,
	String summary,
	String url
) {
}
