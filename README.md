# 📚 Proiect Real-Time Analytics: Goodreads Streaming

Acest proiect monitorizează fluxul de date de la Goodreads folosind o arhitectură modernă de streaming:
**Producer (Python) ➔ Kafka ➔ Flink SQL ➔ Postgres ➔ Grafana**

---

## 🚀 Ghid de Pornire (Pas cu Pas)

### 1. Pornirea Infrastructurii (Docker)

📍 Locație: Folderul rădăcină al proiectului (unde se află `docker-compose.yml`)

```bash
# Șterge containerele vechi, pentru o pornire curată (fara -v ca sa nu sterg si volumele ca se pierde dashboard ul grafana)
docker-compose down 

# Construiește și pornește containerele în fundal
docker-compose up --build -d
```

⏳ Așteaptă ~30 secunde pentru inițializare completă.

---

### 2. Pornirea Fluxului de Date (Producer)

📍 Locație: `producer/`

```bash
python producer.py
```

✔ Citește CSV
✔ Trimite JSON în Kafka

---

### 3. Lansarea Analizei (Flink SQL)

📍 Se rulează în container

```bash
docker exec -it books-flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/job.sql
```

✔ Vei primi un **Job ID**
✔ Monitorizare: http://localhost:8081

---

### 4. Vizualizarea Rezultatelor (Grafana)

Accesează:

```
http://localhost:3000
```

🔐 Login:

```
admin / admin
```

⚡ Activează:

* Auto-refresh: 5s

---

## 📁 Structura Proiectului și Rolul Componentelor

### 1. Folderul `flink/`

* **Dockerfile**

  * Configurează Flink
  * Instalează Java 17
  * Adaugă conectori (Kafka, JDBC, Postgres)

* **sql/job.sql**

  * Definește tabelele Kafka + Postgres
  * Rulează agregări folosind ferestre de streaming (`TUMBLE`)
  * Generează recomandări explicabile
  * Clasifică automat cărțile pe segmente
  * Detectează titluri emergente în flux

---

### 2. Folderul `producer/`

* **producer.py**

  * Simulează stream-ul
  * Curăță coloane
  * Trimite JSON în Kafka

* **books.csv**

  * Dataset Goodreads

---

### 3. Folderul `postgres/`

* **init.sql**

  * Creează tabele (ex: `top_authors`)
  * Pregătește baza pentru Flink
  * Creează tabelele necesare pentru rezultatele analizelor:
    * top_authors
    * language_stats
    * publisher_metrics
    * book_recommendations
    * book_classification
    * fastest_growing_books

---

### 4. Rădăcina Proiectului

* **docker-compose.yml**

Orchestrarea serviciilor:

| Componentă        | Rol         |
| ----------------- | ----------- |
| Zookeeper & Kafka | Streaming   |
| Postgres          | Stocare     |
| Flink             | Procesare   |
| Grafana           | Vizualizare |

---

## 🛠️ Comenzi de Verificare (Quick Fix)

### ✔ Verifică datele în Postgres

```bash
docker exec -it books-postgres psql -U books -d books-postgres -c "SELECT * FROM top_authors LIMIT 5;"
```

---

### ✔ Verifică log-uri Flink

```bash
docker logs books-flink-taskmanager --tail 100
```

### ✔ Verifică recomandările

```bash
docker exec -it books-postgres psql -U books -d books-postgres \
-c "SELECT * FROM book_recommendations LIMIT 10;"
```

### ✔ Verifică trendurile

```bash
docker exec -it books-postgres psql -U books -d books-postgres \
-c "SELECT * FROM literary_trends LIMIT 10;"
```

---

## 🧠 Arhitectura Finală

```
Producer (Python)
        ↓
      Kafka
        ↓
    Flink SQL
        ↓
     Postgres
        ↓
     Grafana
```

---

## 📊 Advanced Analytics

Pe lângă agregările clasice (autori, limbi, edituri), proiectul extinde analiza prin componente avansate de procesare în timp real.

### 1. Explainable Book Recommendations

Sistemul generează recomandări globale folosind un scor compozit:

```text
recommendation_score =

0.6 × average_rating
+
0.3 × log10(ratings_count +1)
+
0.1 × log10(text_reviews_count +1)
```

Se iau în calcul:

- calitatea cărții (rating)
- popularitatea (numărul de evaluări)
- engagement-ul (review-uri)

Fiecare recomandare include și o justificare textuală.

---

### 2. Smart Book Classification

Cărțile sunt clasificate automat în funcție de rating și popularitate:

Categorii:

- Highly Rated & Popular
- Highly Rated but Niche
- Moderately Popular
- Popular but Lower Rated
- Average
- Low Visibility

Această clasificare permite identificarea rapidă a profilului fiecărei cărți și reduce concentrarea într-o categorie generică.

---

### 3. Emerging Titles Detection (Real-Time)

Sistemul detectează titluri cu interes emergent folosind un scor de creștere:

```text
growth_rate =

log10(ratings_count +1)
-
log10(text_reviews_count +1)
```

Scorul urmărește relația dintre:

- volumul evaluărilor
- activitatea utilizatorilor
- interesul generat în flux

Sistemul nu identifică doar cele mai populare titluri, ci cărțile care prezintă interes accelerat în fluxul curent.


---

## 🎯 Ce demonstrează proiectul

✔ Streaming real-time
✔ Procesare distribuită (Flink)
✔ Persistență (Postgres)
✔ Vizualizare live (Grafana)
✔ Arhitectură modernă de date
✔ Sistem de recomandări explicabile
✔ Analytics avansat peste simple agregări Top-K
✔ Clasificare inteligentă a cărților
✔ Detectare de titluri emergente în timp real

---

## Grafana inca ramane in picioare (comenzi de test)

```bash
docker-compose stop
```

```bash
docker-compose up -d
```

```bash
docker exec -it books-flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/job.sql
```

```bash
python producer.py
```

```bash
// asta e pentru crearea trendului pt kafka si mai trebuie data uneori manual
docker exec -it books-kafka kafka-topics --create --topic literary_trends --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```


```bash
// asta e pt golirea datelor din grafana
docker exec -it books-postgres psql -U books -d books-postgres -c "TRUNCATE TABLE language_stats, publisher_metrics, top_authors;"
```
