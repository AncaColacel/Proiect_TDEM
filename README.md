# Sistem Analiză Literară în Timp Real

Arhitectură de procesare a datelor și identificarea trendurilor literare în timp real.

---

## Arhitectură

```
Producer (Python)
        ↓
      Kafka
        ↓
    Flink SQL
        ↓
    PostgreSQL
        ↓
     Grafana
```

| Componentă | Rol |
|---|---|
| Zookeeper & Kafka | Message broker / streaming |
| Apache Flink | Procesare distribuită în timp real |
| PostgreSQL | Persistența rezultatelor |
| Grafana | Vizualizare live |

---

## Pornire

### 1. Infrastructură (Docker)

```bash
docker-compose up --build -d
```

### 2. Producer

```bash
cd producer/
python producer.py
```

### 3. Flink SQL Job

```bash
docker exec -it books-flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/job.sql
```

Monitorizare Flink UI: `http://localhost:8081`

### 4. Grafana

```
http://localhost:3000   (admin / admin)
```

---

## Componente analytics

### Recomandări explicabile

Scor compozit per carte:

```
score = 0.6 × avg_rating
      + 0.3 × log10(ratings_count + 1)
      + 0.1 × log10(text_reviews_count + 1)
```

Combină calitatea, popularitatea și engagement-ul utilizatorilor. Fiecare recomandare include o justificare textuală generată automat.

### Clasificare inteligentă

Cărțile sunt clasificate automat în șase profile pe baza ratingului și popularității: *Highly Rated & Popular*, *Highly Rated but Niche*, *Moderately Popular*, *Popular but Lower Rated*, *Average*, *Low Visibility*.

### Detectare titluri emergente

```
growth_rate = log10(ratings_count + 1) - log10(text_reviews_count + 1)
```

Identifică titluri cu interes accelerat în flux — nu doar cele mai populare, ci cele cu creștere rapidă în timp real.

---

## Structura proiectului

```
├── docker-compose.yml
├── producer/
│   ├── producer.py        # Simulare stream, trimitere JSON în Kafka
│   └── books.csv          # Dataset Goodreads
├── flink/
│   ├── Dockerfile         # Flink + Java 17 + conectori Kafka/JDBC
│   └── sql/job.sql        # Definiții tabele, agregări, ferestre TUMBLE
└── postgres/
    └── init.sql           # Creare tabele rezultate
```

---

## Comenzi utile

```bash
# Verificare date PostgreSQL
docker exec -it books-postgres psql -U books -d books-postgres \
  -c "SELECT * FROM top_authors LIMIT 5;"

# Verificare recomandări
docker exec -it books-postgres psql -U books -d books-postgres \
  -c "SELECT * FROM book_recommendations LIMIT 10;"

# Loguri Flink
docker logs books-flink-taskmanager --tail 100

# Creare topic Kafka (dacă lipsește)
docker exec -it books-kafka kafka-topics --create \
  --topic literary_trends --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

```
