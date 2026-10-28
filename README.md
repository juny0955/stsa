# stsa (Stock market Sentiment Analysis - 가칭)

## 모듈

- `collector`: 뉴스 수집.
- `persistence`: 공유 DB 스키마, Flyway 마이그레이션, jOOQ 생성 코드, PostgreSQL 드라이버.

DB 마이그레이션과 jOOQ 코드 생성 방법은 [persistence 문서](persistence/README.md)를 참고한다.

## 빌드 및 테스트

Java 27을 사용한다. 일반 빌드에는 DB 접속이 필요하지 않으며, 통합 테스트에는 Docker가 필요하다.

```sh
./gradlew assemble
./gradlew :persistence:test :collector:test
```

애플리케이션 실행 전 별도 마이그레이션 작업으로 DB를 준비한다. 애플리케이션 기동 시에는 마이그레이션을 실행하지 않는다.
