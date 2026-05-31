-- Tabelul Top autori
CREATE TABLE IF NOT EXISTS top_authors (
    author TEXT,
    reads_count BIGINT,
    avg_rating FLOAT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PRIMARY KEY (author, window_start)
);

-- Tabel statistici pe limbi (pentru Pie Chart)
CREATE TABLE IF NOT EXISTS language_stats (
    language_code VARCHAR(50),
    book_count BIGINT,
    avg_rating FLOAT,
    window_start TIMESTAMP,
    PRIMARY KEY (language_code, window_start)
);

-- Tabel statistici edituri (pentru Table/Bar Chart)
CREATE TABLE IF NOT EXISTS publisher_metrics (
    publisher TEXT,
    total_reviews BIGINT,
    avg_pages DOUBLE PRECISION,
    window_start TIMESTAMP,
    PRIMARY KEY (publisher, window_start)
);

--  Trenduri literare
CREATE TABLE IF NOT EXISTS literary_trends (
    trend_type TEXT,
    trend_name TEXT,
    window_count BIGINT,
    avg_rating FLOAT,
    total_ratings BIGINT,
    trend_score FLOAT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PRIMARY KEY (trend_type, trend_name, window_start)
);


-- Recomandări prin scor
CREATE TABLE IF NOT EXISTS book_recommendations (
    title TEXT,
    author TEXT,
    average_rating FLOAT,
    ratings_count BIGINT,
    recommendation_score FLOAT,
    reason TEXT,
    generated_at TIMESTAMP
);

-- Clasificare cărți după calitate și popularitate
CREATE TABLE IF NOT EXISTS book_classification (
    title TEXT,
    author TEXT,
    segment TEXT,
    average_rating FLOAT,
    ratings_count BIGINT,
    generated_at TIMESTAMP
);