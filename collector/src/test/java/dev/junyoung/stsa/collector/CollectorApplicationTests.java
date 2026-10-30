package dev.junyoung.stsa.collector;

import org.jooq.DSLContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "finnhub.news.enabled=false")
@Import(CollectorApplicationTests.DatabaseConfig.class)
@DisplayName("Collector 애플리케이션")
class CollectorApplicationTests {

	@Test
	@DisplayName("애플리케이션 컨텍스트가 정상적으로 로드된다")
	void contextLoads() {
	}

	@Test
	@DisplayName("애플리케이션 기동 시 공통 스키마를 자동으로 생성하지 않는다")
	void doesNotMigrateDatabaseOnStartup(@Autowired DSLContext sql) {
		assertThat(sql.fetchOne("SELECT to_regclass('public.stock')::text AS relation")
				.get("relation", String.class)).isNull();
		assertThat(sql.fetchOne("SELECT to_regclass('public.flyway_schema_history')::text AS relation")
				.get("relation", String.class)).isNull();
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
