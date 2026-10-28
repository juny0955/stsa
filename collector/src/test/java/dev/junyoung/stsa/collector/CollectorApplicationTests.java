package dev.junyoung.stsa.collector;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(properties = "finnhub.news.enabled=false")
@Import(CollectorApplicationTests.DatabaseConfig.class)
@DisplayName("Collector 애플리케이션")
class CollectorApplicationTests {

	@Test
	@DisplayName("애플리케이션 컨텍스트가 정상적으로 로드된다")
	void contextLoads() {
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
