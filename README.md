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

## 뉴스 저장

수집 대상 `NVDA`, `AAPL`, `MSFT`의 symbol을 실행 전에 `stock` 테이블에 등록해야 한다.
미등록 종목은 자동 등록하지 않고 해당 종목의 저장을 실패 처리한다.

종목별 응답을 최대 500건씩 나눠 기사와 종목 연결을 일괄 저장하며, 여러 청크도 하나의 트랜잭션으로 처리한다.
기존 기사의 내용과 수정 시각은 유지하고 종목 연결만 중복 없이 추가한다.
뉴스 저장이 커밋된 종목만 실행 이력의 성공 건수로 집계한다.
종목별 조회·저장 실패는 종목명과 오류 원인을 `job_execution.error_message`에 함께 저장하며, 전체 실행 예외도 보존한다.
