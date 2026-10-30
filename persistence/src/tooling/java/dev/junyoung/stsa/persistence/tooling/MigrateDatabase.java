package dev.junyoung.stsa.persistence.tooling;

import org.flywaydb.core.Flyway;

public final class MigrateDatabase {

	private MigrateDatabase() {
	}

	public static void main(String[] args) {
		configured(DatabaseSettings.fromEnvironment("MIGRATION")).migrate();
	}

	static Flyway configured(DatabaseSettings database) {
		return Flyway.configure()
				.dataSource(database.url(), database.user(), database.password())
				.locations("classpath:db/migration")
				.defaultSchema("public")
				.schemas("public")
				.cleanDisabled(true)
				.load();
	}
}
