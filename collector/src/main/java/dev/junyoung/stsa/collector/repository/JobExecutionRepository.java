package dev.junyoung.stsa.collector.repository;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class JobExecutionRepository {

	private final DSLContext dsl;


}
