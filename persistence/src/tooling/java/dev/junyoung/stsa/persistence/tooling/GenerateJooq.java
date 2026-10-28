package dev.junyoung.stsa.persistence.tooling;

import java.nio.file.Path;

import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Database;
import org.jooq.meta.jaxb.Generate;
import org.jooq.meta.jaxb.Generator;
import org.jooq.meta.jaxb.Jdbc;
import org.jooq.meta.jaxb.Target;

public final class GenerateJooq {

	private GenerateJooq() {
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			throw new IllegalArgumentException("Expected the generated source directory as the only argument");
		}
		generate(DatabaseSettings.fromEnvironment("CODEGEN"), Path.of(args[0]));
	}

	static void generate(DatabaseSettings database, Path output) throws Exception {
		MigrateDatabase.configured(database).migrate();
		GenerationTool.generate(new Configuration()
				.withJdbc(new Jdbc()
						.withDriver("org.postgresql.Driver")
						.withUrl(database.url())
						.withUser(database.user())
						.withPassword(database.password()))
				.withGenerator(new Generator()
						.withDatabase(new Database()
								.withName("org.jooq.meta.postgres.PostgresDatabase")
								.withInputSchema("public")
								.withExcludes("flyway_schema_history"))
						.withGenerate(new Generate()
								.withDaos(false)
								.withPojos(false)
								.withRecords(true)
								.withGeneratedAnnotationDate(false))
						.withTarget(new Target()
								.withPackageName("dev.junyoung.stsa.persistence.jooq")
								.withDirectory(output.toAbsolutePath().toString()))));
	}
}
