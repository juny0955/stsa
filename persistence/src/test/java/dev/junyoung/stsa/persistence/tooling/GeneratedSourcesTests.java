package dev.junyoung.stsa.persistence.tooling;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("공통 jOOQ 생성 코드")
class GeneratedSourcesTests extends PostgresTestSupport {

	@Test
	@DisplayName("Flyway 스키마에서 재생성한 코드가 저장소의 코드와 일치한다")
	void matchesCommittedSources(@TempDir Path output) throws Exception {
		Path committed = Path.of(System.getProperty("persistence.generatedSources"));

		GenerateJooq.generate(database(), output);

		List<Path> actualFiles = sourceFiles(output);
		assertThat(actualFiles).isNotEmpty().containsExactlyElementsOf(sourceFiles(committed));
		for (Path relative : actualFiles) {
			assertThat(Files.readString(output.resolve(relative))).as(relative.toString())
					.isEqualTo(Files.readString(committed.resolve(relative)));
		}
	}

	private static List<Path> sourceFiles(Path root) throws IOException {
		try (var paths = Files.walk(root)) {
			return paths.filter(Files::isRegularFile).map(root::relativize).sorted().toList();
		}
	}
}
