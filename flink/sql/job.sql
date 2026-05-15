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

-- ==========================================
-- 5. EXECUTARE ANALIZE (Pornirea Job-urilor)
-- ==========================================

-- Analiza A: Autori
INSERT INTO top_author_sink
SELECT 
    authors, 
    COUNT(*), 
    AVG(average_rating), 
    TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE), 
    TUMBLE_END(ts_ltz, INTERVAL '1' MINUTE)
FROM books_input 
GROUP BY authors, TUMBLE(ts_ltz, INTERVAL '1' MINUTE);

-- Analiza B: Limbi
INSERT INTO language_sink
SELECT 
    language_code, 
    COUNT(*), 
    AVG(average_rating), 
    TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE)
FROM books_input 
GROUP BY language_code, TUMBLE(ts_ltz, INTERVAL '1' MINUTE);

-- Analiza C: Edituri
INSERT INTO publisher_sink
SELECT 
    publisher, 
    SUM(CAST(text_reviews_count AS BIGINT)), 
    AVG(CAST(num_pages AS DOUBLE PRECISION)), 
    TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE)
FROM books_input 
GROUP BY publisher, TUMBLE(ts_ltz, INTERVAL '1' MINUTE);