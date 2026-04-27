# Proiect_TDEM
## Sistem de monitorizare și analiză a trendurilor literare în timp real

### 1. Echipa
* **Alexe Alexandra - Florentina** - SSA1-A
* **Colăcel Anca - Maria** - SSA1-A

### 2. Obiectivul Proiectului
Scopul proiectului este dezvoltarea unei platforme de tip **Streaming Analytics** capabilă să proceseze volume mari de date despre cărți și recenzii, pentru a identifica tendințele de popularitate ale autorilor și genurilor literare.

Sistemul va simula un flux continuu de date (evenimente de tip "book rating" sau "new entry"), permițând calcularea unor indicatori de performanță (**KPIs**) în ferestre de timp glisante (ex: top autori cu cele mai bune rating-uri, volumele cu cel mai mare număr de pagini procesate în intervalul curent).

### 3. Seturi de date
Vom utiliza dataset-ul **Goodreads Books**, disponibil pe platforma Kaggle.
https://www.kaggle.com/datasets/jealousleopard/goodreadsbooks?resource=download

* **Conținut:** Date despre peste 11.000 de volume, incluzând titlu, autor, rating mediu, ISBN, număr de pagini și numărul de recenzii.
* **Format:** Fișier structurat `.csv`.
* **Metodă de ingestie:** Datele vor fi citite din fișier printr-un script Python (**Producer**) și injectate în mod secvențial într-un cluster **Kafka** pentru a simula un flux de date în timp real.

### 4. Motoare de Procesare Utilizate
Arhitectura proiectului se bazează pe următoarea stivă tehnologică:

* **Apache Kafka:** Utilizat ca broker de mesaje pentru ingestia și stocarea temporară a fluxurilor de date.
* **Apache Flink:** Motorul principal de procesare. Acesta va fi responsabil pentru:
    * Filtrarea datelor (ex: eliminarea intrărilor cu rating scăzut).
    * Agregarea datelor folosind ferestre de timp (*Time Windows*).
    * Calcularea statisticilor de tip "Top - K" (de exemplu, cei mai populari autori).
* **Python:** Pentru scriptul de pre-procesare și trimitere a datelor către Kafka.
* **Grafana:** Un dashboard simplu pentru vizualizarea rezultatelor procesate de Flink.
