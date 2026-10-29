package dev.junyoung.stsa.collector.job;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

import dev.junyoung.stsa.collector.CollectorApplication;
import dev.junyoung.stsa.collector.repository.JobExecutionRepository;
import dev.junyoung.stsa.collector.service.JobExecutionService;
import dev.junyoung.stsa.collector.service.NewsPersistenceService;

import static dev.junyoung.stsa.persistence.jooq.Tables.STOCK;

@SpringBootTest(classes = CollectorApplication.class, properties = "finnhub.news.enabled=false")
@Import(JobExecutionTestSupport.DatabaseConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class JobExecutionTestSupport {

	@Autowired
	JobExecutionService service;
	@Autowired
	JobExecutionRepository repository;
	@Autowired
	NewsPersistenceService newsPersistenceService;
	@Autowired
	DSLContext sql;
	@Autowired
	DataSource dataSource;
	DSLContext observer;
	private Connection observerConnection;

	@BeforeAll
	void migrateTestDatabase() {
		Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
	}

	@BeforeEach
	void resetDatabaseAndOpenObserver() throws SQLException {
		sql.execute("TRUNCATE news_article_stock, news_article, stock, job_execution RESTART IDENTITY");
		observerConnection = dataSource.getConnection();
		observer = DSL.using(observerConnection, SQLDialect.POSTGRES);
	}

	@AfterEach
	void closeObserver() throws SQLException {
		if (observerConnection != null) {
			observerConnection.close();
		}
	}

	long registerStock(String symbol) {
		return sql.insertInto(STOCK).set(STOCK.SYMBOL, symbol).returningResult(STOCK.ID).fetchSingle().value1();
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class DatabaseConfig {

		@Bean
		@ServiceConnection
		PostgreSQLContainer postgres() {
			return new PostgreSQLContainer("postgres:18.6");
		}
	}
}
