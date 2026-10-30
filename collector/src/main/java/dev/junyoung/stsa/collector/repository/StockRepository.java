package dev.junyoung.stsa.collector.repository;

import java.util.Optional;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

import static dev.junyoung.stsa.persistence.jooq.Tables.STOCK;

@Repository
@RequiredArgsConstructor
public class StockRepository {

	private final DSLContext dsl;

	public Optional<Long> findIdBySymbol(String symbol) {
		return dsl.select(STOCK.ID).from(STOCK).where(STOCK.SYMBOL.eq(symbol)).fetchOptional(STOCK.ID);
	}
}
