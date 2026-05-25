-- 1. Definire sursa (Kafka)
CREATE TABLE books_input (
    bookID INT,
    title STRING,
    authors STRING,
    average_rating FLOAT,
    language_code STRING,
    num_pages INT,
    ratings_count INT,
    text_reviews_count INT,
    publisher STRING,
    ts_ltz AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'literary_trends',
    'properties.bootstrap.servers' = 'kafka:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 2. Definire Sink: Limbi (Postgres)
CREATE TABLE language_sink (
    language_code STRING,
    book_count BIGINT,
    avg_rating FLOAT,
    window_start TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://books-postgres:5432/books-postgres',
    'table-name' = 'language_stats',
    'username' = 'books',
    'password' = 'books'
);

-- 3. Definire Sink: Edituri (Postgres)
CREATE TABLE publisher_sink (
    publisher STRING,
    total_reviews BIGINT,
    avg_pages DOUBLE PRECISION,
    window_start TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://books-postgres:5432/books-postgres',
    'table-name' = 'publisher_metrics',
    'username' = 'books',
    'password' = 'books'
);

-- 4. Definire Sink: Autori (Postgres)
CREATE TABLE top_author_sink (
    author STRING,
    reads_count BIGINT,
    avg_rating FLOAT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://books-postgres:5432/books-postgres',
    'table-name' = 'top_authors',
    'username' = 'books',
    'password' = 'books'
);

-- 5. Definire Sink: Recomandari (Postgres)
CREATE TABLE recommendation_sink (
    title STRING,
    author STRING,
    average_rating FLOAT,
    ratings_count BIGINT,
    recommendation_score FLOAT,
    reason STRING,
    generated_at TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://books-postgres:5432/books-postgres',
    'table-name' = 'book_recommendations',
    'username' = 'books',
    'password' = 'books'
);

-- ==========================================
-- 5. EXECUTARE ANALIZE (Pornirea Job-urilor)
-- ==========================================

-- Analiza A: Autori
INSERT INTO top_author_sink
SELECT 
    authors, 
    COUNT(*), 
    AVG(average_rating), 
    TUMBLE_START(ts_ltz, INTERVAL '20' SECONDS), 
    TUMBLE_END(ts_ltz, INTERVAL '20' SECONDS)
FROM books_input 
GROUP BY authors, TUMBLE(ts_ltz, INTERVAL '20' SECONDS);

-- Analiza B: Limbi
INSERT INTO language_sink
SELECT 
    language_code, 
    COUNT(*), 
    AVG(average_rating), 
    TUMBLE_START(ts_ltz, INTERVAL '20' SECONDS)
FROM books_input 
GROUP BY language_code, TUMBLE(ts_ltz, INTERVAL '20' SECONDS);

-- Analiza C: Edituri
INSERT INTO publisher_sink
SELECT 
    publisher, 
    SUM(CAST(text_reviews_count AS BIGINT)), 
    AVG(CAST(num_pages AS DOUBLE)), 
    TUMBLE_START(ts_ltz, INTERVAL '20' SECONDS)
FROM books_input 
GROUP BY publisher, TUMBLE(ts_ltz, INTERVAL '20' SECONDS);

-- Analiza D: Recomandari justificate
INSERT INTO recommendation_sink
SELECT
    title,
    authors AS author,
    average_rating,
    CAST(ratings_count AS BIGINT),
    CAST(
        average_rating * 0.6
        + LOG10(CAST(ratings_count + 1 AS DOUBLE)) * 0.3
        + LOG10(CAST(text_reviews_count + 1 AS DOUBLE)) * 0.1
        AS FLOAT
    ) AS recommendation_score,
    CONCAT(
        'Recomandata deoarece are rating ridicat (',
        CAST(average_rating AS STRING),
        '), multe evaluari (',
        CAST(ratings_count AS STRING),
        ') si review-uri (',
        CAST(text_reviews_count AS STRING),
        ').'
    ) AS reason,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS generated_at
FROM books_input
WHERE average_rating >= 4.0
  AND ratings_count >= 1000;

-- Sink: Trenduri literare
CREATE TABLE literary_trends_sink (
    trend_type STRING,
    trend_name STRING,
    count_1h BIGINT,
    count_12h BIGINT,
    count_24h BIGINT,
    trend_score FLOAT,
    window_start TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://books-postgres:5432/books-postgres',
    'table-name' = 'literary_trends',
    'username' = 'books',
    'password' = 'books'
);

-- Analiza E: Trenduri literare pe autori
INSERT INTO literary_trends_sink
SELECT
    'AUTHOR' AS trend_type,
    authors AS trend_name,
    COUNT(*) AS count_1h,
    CAST(NULL AS BIGINT) AS count_12h,
    CAST(NULL AS BIGINT) AS count_24h,
    CAST(
        COUNT(*) * 0.5
        + AVG(average_rating) * 20
        + LOG10(SUM(CAST(ratings_count AS BIGINT)) + 1) * 10
        AS FLOAT
    ) AS trend_score,
    TUMBLE_START(ts_ltz, INTERVAL '20' SECONDS) AS window_start
FROM books_input
GROUP BY authors, TUMBLE(ts_ltz, INTERVAL '20' SECONDS);