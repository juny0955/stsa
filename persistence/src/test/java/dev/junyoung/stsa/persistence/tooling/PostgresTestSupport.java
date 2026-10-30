package dev.junyoung.stsa.persistence.tooling;

import java.sql.Connection;
import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
abstract class PostgresTestSupport {

	@Container
	static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6");

	private Connection connection;
	DSLContext sql;

	@BeforeAll
	static void migrateEmptyDatabase() {
		assertThat(MigrateDatabase.configured(database()).migrate().migrationsExecuted).isEqualTo(1);
	}

	@BeforeEach
	void openConnection() throws SQLException {
		connection = POSTGRES.createConnection("");
		sql = DSL.using(connection, SQLDialect.POSTGRES);
		sql.execute("TRUNCATE news_article_stock, news_article, stock RESTART IDENTITY");
	}

	@AfterEach
	void closeConnection() throws SQLException {
		connection.close();
	}

	static DatabaseSettings database() {
		return new DatabaseSettings(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
	}
}
