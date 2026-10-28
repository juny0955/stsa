package dev.junyoung.stsa.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import dev.junyoung.stsa.collector.config.FinnhubProperties;

@SpringBootApplication
@EnableConfigurationProperties(FinnhubProperties.class)
@EnableScheduling
public class CollectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(CollectorApplication.class, args);
	}

}
