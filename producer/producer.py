import pandas as pd
from kafka import KafkaProducer
import json
import time

# Configurare
TOPIC_NAME = 'literary_trends'
KAFKA_SERVER = 'localhost:9092' 

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 2),
    retries=5
)

def send_data():
    try:
        df = pd.read_csv('data/books.csv', on_bad_lines='skip')
    except FileNotFoundError:
        print("Eroare: Nu am găsit books.csv în folderul data/.")
        return

    # Curățare numele coloanele
    df.columns = [col.strip() for col in df.columns]
    
    print(f"Am încărcat {len(df)} cărți. Începe transmisia...")

    for _, row in df.iterrows():
        try:
            payload = {
                'bookID': int(row['bookID']), 
                'title': str(row['title']),
                'authors': str(row['authors']),
                'average_rating': float(row['average_rating']),
                'language_code': str(row['language_code']), 
                'num_pages': int(row['num_pages']), 
                'ratings_count': int(row['ratings_count']),
                'text_reviews_count': int(row['text_reviews_count']),
                'publisher': str(row['publisher'])
            }
            
            producer.send(TOPIC_NAME, value=payload)
            print(f"Sent: {payload['title']} | Lang: {payload['language_code']}")
            
            # trimite 10 cărți pe secundă
            time.sleep(0.1) 
            
        except Exception as e:
            print(f"Eroare la procesare rând: {e}")
            continue 

    producer.flush()
    print("Transmisie finalizată!")

if __name__ == "__main__":
    send_data()