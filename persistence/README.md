# persistence

모든 실행 모듈이 공유하는 PostgreSQL `public` 스키마와 jOOQ 타입을 제공하는 Java 라이브러리다.
repository, 업무 모델, 트랜잭션 경계, DataSource·DSLContext 설정은 사용하는 모듈이 관리한다.
이 모듈은 Spring 컴포넌트와 collector DTO에 의존하지 않는다.

## 스키마

정의 원본은 `src/main/resources/db/migration`의 Flyway SQL이다. 전체 모듈에서 하나의 버전 순서와 이력 테이블을 사용한다.

| 테이블 | 역할 | 식별·연결 규칙 |
| --- | --- | --- |
| `stock` | 주식 종목 마스터 | 내부 identity PK, `symbol` UNIQUE, 기본 활성 상태 |
| `news_article` | 제공자별 기사 | 내부 identity PK, `(provider, external_id)` UNIQUE |
| `news_article_stock` | 기사와 종목의 다대다 연결 | `(article_id, stock_id)` 복합 PK와 FK |

기사를 삭제하면 연결도 삭제한다. 연결된 종목의 삭제는 제한하며 `stock.enabled=false`로 비활성화한다.
기사의 URL 중복은 허용한다. 시간 컬럼은 `timestamptz`이며 jOOQ에서는 `OffsetDateTime`으로 제공한다.
`created_at`과 `updated_at`은 최초 저장 시 DB 현재 시각으로 설정되며, 이후 `updated_at` 갱신은 각 repository가 담당한다.
`related`는 제공자 원문을 보존하는 선택 필드다. 향후 collector의 종목 연결은 조회 요청에 사용한 symbol을 기준으로 한다.

종목 초기 데이터, 수집 이력, 감성 분석 스키마는 포함하지 않는다.

## 마이그레이션

대상 DB는 먼저 생성되어 있어야 한다. 아래 환경변수는 셸 또는 배포 환경에서 설정한다.
Gradle DB 작업은 애플리케이션의 `.env` 파일을 자동으로 읽지 않는다.

```sh
export MIGRATION_DB_URL='jdbc:postgresql://localhost:5432/stsa'
export MIGRATION_DB_USER='postgres'
export MIGRATION_DB_PASSWORD='<password>'
./gradlew :persistence:flywayMigrate
```

세 환경변수는 모두 필수이며 누락되거나 공백이면 작업이 실패한다. 기본 접속 대상은 제공하지 않는다.
마이그레이션은 전용 도구 소스셋에서 실행하며, Flyway 라이브러리를 애플리케이션에 전파하지 않는다.
collector에는 `spring.flyway.enabled=false`도 명시한다. 향후 실행 모듈에서도 기동 시 마이그레이션을 활성화하지 않는다.

배포 순서는 **마이그레이션 성공 → 애플리케이션 배포**다. 적용한 SQL을 수정하지 않고 다음 버전의 SQL을 추가한다.
이번 최초 테이블 생성은 기존 테이블을 수정하지 않는다. 이후 데이터 저장을 시작하면 애플리케이션 롤백 시에도 테이블과 데이터를 유지한다.
자동으로 스키마를 삭제하거나 마이그레이션을 되돌리는 작업은 제공하지 않는다.

## jOOQ 코드 생성

별도로 준비한 개발용 PostgreSQL을 사용한다. 테스트와 동일한 PostgreSQL 18.6 사용을 권장한다.

```sh
export CODEGEN_DB_URL='jdbc:postgresql://localhost:5432/stsa_codegen'
export CODEGEN_DB_USER='postgres'
export CODEGEN_DB_PASSWORD='<password>'
./gradlew :persistence:jooqCodegen
```

이 작업은 **CODEGEN_DB 대상에 Flyway 적용 → jOOQ 생성** 순서로 실행한다.
`MIGRATION_DB_*` 설정은 사용하지 않는다. 코드 생성용 DB에 직접 가한 스키마 변경이 섞이지 않도록 전용 개발 DB를 사용한다.
세 환경변수는 모두 필수이며 누락되거나 공백이면 작업이 실패한다.

- 생성 대상: 위 세 테이블과 관련 테이블·레코드·키·인덱스. Flyway 이력 테이블과 DAO·POJO는 제외한다.
- 패키지: `dev.junyoung.stsa.persistence.jooq`
- 경로: `src/generated/java`
- 생성 코드는 SQL 변경과 함께 커밋하며 직접 수정하지 않는다. 생성 시각을 제외해 재생성 결과를 비교할 수 있다.
- 컴파일·테스트·코드 생성 도구의 버전은 기존 Spring Boot BOM으로 관리한다.

일반 `compileJava`·`assemble` 작업은 마이그레이션이나 코드 생성을 실행하지 않는다.
도구 소스와 의존성은 배포용 라이브러리 JAR에 포함하지 않는다.

## 검증

```sh
./gradlew :persistence:test :collector:test
```

테스트는 Docker의 격리된 PostgreSQL 18.6을 사용한다. 개발·배포 DB 환경변수는 사용하지 않는다.
빈 DB 마이그레이션, 재실행, 유일키·외래키, 다대다 연결, 삭제 규칙, 기본값과 시각을 검증한다.
또한 실제 스키마에서 jOOQ 코드를 임시 디렉터리에 재생성해 저장소 코드와 파일 단위로 비교하며,
collector가 기동 시 테이블과 Flyway 이력을 생성하지 않는지 확인한다.
