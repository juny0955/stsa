package dev.junyoung.stsa.persistence.tooling;

record DatabaseSettings(String url, String user, String password) {

	static DatabaseSettings fromEnvironment(String prefix) {
		return new DatabaseSettings(required(prefix + "_DB_URL"), required(prefix + "_DB_USER"),
				required(prefix + "_DB_PASSWORD"));
	}

	private static String required(String name) {
		String value = System.getenv(name);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Required environment variable is missing or blank: " + name);
		}
		return value;
	}

	@Override
	public String toString() {
		return "DatabaseSettings[redacted]";
	}
}
