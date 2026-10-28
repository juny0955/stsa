CREATE TABLE stock (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol text NOT NULL,
    name text,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT stock_symbol_uk UNIQUE (symbol)
);

CREATE TABLE news_article (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider text NOT NULL,
    external_id text NOT NULL,
    published_at timestamptz NOT NULL,
    headline text NOT NULL,
    url text NOT NULL,
    category text,
    source text,
    summary text,
    image_url text,
    related text,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT news_article_provider_external_id_uk UNIQUE (provider, external_id)
);

CREATE INDEX news_article_published_at_id_idx ON news_article (published_at DESC, id DESC);

CREATE TABLE news_article_stock (
    article_id bigint NOT NULL,
    stock_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT news_article_stock_pk PRIMARY KEY (article_id, stock_id),
    CONSTRAINT news_article_stock_article_fk FOREIGN KEY (article_id)
        REFERENCES news_article (id) ON DELETE CASCADE,
    CONSTRAINT news_article_stock_stock_fk FOREIGN KEY (stock_id)
        REFERENCES stock (id) ON DELETE RESTRICT
);

CREATE INDEX news_article_stock_stock_article_idx ON news_article_stock (stock_id, article_id);

CREATE TABLE job_execution (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_name text NOT NULL,
    status text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at timestamptz,
    processed_count integer NOT NULL DEFAULT 0,
    success_count integer NOT NULL DEFAULT 0,
    failure_count integer NOT NULL DEFAULT 0,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT job_execution_status_ck
       CHECK (status IN (
                         'RUNNING',
                         'SUCCESS',
                         'PARTIAL_SUCCESS',
                         'FAILED'
           )),

    CONSTRAINT job_execution_count_ck
       CHECK (
           processed_count >= 0
               AND success_count >= 0
               AND failure_count >= 0
               AND success_count + failure_count <= processed_count
           )
);

CREATE INDEX job_execution_job_started_idx
    ON job_execution (job_name, started_at DESC);