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

-- 6. Definire Sink: Clasificare carti
CREATE TABLE book_classification_sink (
    title STRING,
    author STRING,
    segment STRING,
    average_rating FLOAT,
    ratings_count BIGINT,
    generated_at TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://books-postgres:5432/books-postgres',
    'table-name' = 'book_classification',
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

-- Sink: Cresteri accelerate
CREATE TABLE fastest_growing_books_sink (
    title STRING,
    author STRING,
    current_count BIGINT,
    previous_count BIGINT,
    growth_rate FLOAT,
    avg_rating FLOAT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://books-postgres:5432/books-postgres',
    'table-name' = 'fastest_growing_books',
    'username' = 'books',
    'password' = 'books'
);

INSERT INTO fastest_growing_books_sink
SELECT
    title,
    authors AS author,
    CAST(ratings_count AS BIGINT) AS current_count,
    CAST(text_reviews_count AS BIGINT) AS previous_count,

    CAST(
        (
            LOG10(CAST(ratings_count + 1 AS DOUBLE))
            -
            LOG10(CAST(text_reviews_count + 1 AS DOUBLE))
        )
        AS FLOAT
    ) AS growth_rate,

    average_rating,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM books_input
WHERE ratings_count > 1000;

-- Analiza F: Clasificare carti dupa calitate si popularitate
INSERT INTO book_classification_sink
SELECT
    title,
    authors AS author,
    CASE
        WHEN average_rating >= 4.2 AND ratings_count >= 100000
            THEN 'Highly Rated & Popular'

        WHEN average_rating >= 4.2 AND ratings_count < 100000
            THEN 'Highly Rated but Niche'

        WHEN average_rating >= 3.8
             AND average_rating < 4.2
             AND ratings_count >= 1000
             AND ratings_count < 100000
            THEN 'Moderately Popular'

        WHEN average_rating < 4.0 AND ratings_count >= 100000
            THEN 'Popular but Lower Rated'

        WHEN ratings_count < 1000
            THEN 'Low Visibility'

        ELSE 'Average'
    END AS segment,
    average_rating,
    CAST(ratings_count AS BIGINT),
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS generated_at
FROM books_input;
