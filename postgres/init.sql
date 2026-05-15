-- Tabelul Top Autori
CREATE TABLE IF NOT EXISTS top_authors (
    author TEXT,
    reads_count BIGINT,
    avg_rating FLOAT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PRIMARY KEY (author, window_start)
);

-- Tabel Statistici pe Limbi (pentru Pie Chart)
CREATE TABLE IF NOT EXISTS language_stats (
    language_code VARCHAR(50),
    book_count BIGINT,
    avg_rating FLOAT,
    window_start TIMESTAMP,
    PRIMARY KEY (language_code, window_start)
);

-- Tabel Statistici Edituri (pentru Table/Bar Chart)
CREATE TABLE IF NOT EXISTS publisher_metrics (
    publisher TEXT,
    total_reviews BIGINT,
    avg_pages DOUBLE PRECISION,
    window_start TIMESTAMP,
    PRIMARY KEY (publisher, window_start)
);