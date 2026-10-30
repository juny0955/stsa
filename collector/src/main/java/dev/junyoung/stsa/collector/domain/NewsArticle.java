package dev.junyoung.stsa.collector.domain;

import java.time.OffsetDateTime;

public record NewsArticle(
	String provider,
	String externalId,
	OffsetDateTime publishedAt,
	String headline,
	String url,
	String category,
	String source,
	String summary,
	String imageUrl,
	String related
) {
}
