import psycopg2
from psycopg2.extras import execute_values
import csv
from datetime import datetime

# --- Neon DB connection string ---
DATABASE_URL = "postgresql://neondb_owner:npg_ovkUd6Ch5Dgf@ep-calm-mouse-a93x47fu-pooler.gwc.azure.neon.tech/neondb?sslmode=require&channel_binding=require"

# --- CSV file path ---
CSV_PATH = "sentiment-analysis-comparison/data/Apple-Twitter-Sentiment-DFE.csv"

# --- Create table if it doesn't exist ---
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS apple_tweet_sentiments (
    tweet_id BIGINT PRIMARY KEY,
    tweet_text TEXT,
    sentiment_label TEXT,
    sentiment_confidence FLOAT,
    created_at TIMESTAMP,
    query TEXT
);
"""

# --- Read and parse CSV rows ---
def parse_csv(filepath):
    rows = []
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                tweet_id = int(float(row['id']))
                text = row['text'].strip().replace("\n", " ")
                sentiment = row['sentiment']
                confidence = float(row['sentiment:confidence'])
                created_at = datetime.strptime(row['date'], "%a %b %d %H:%M:%S %z %Y")
                query = row['query']
                rows.append((tweet_id, text, sentiment, confidence, created_at, query))
            except Exception as e:
                print(f"Skipping row due to error: {e}")
                continue
    return rows


def insert_data(rows):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    execute_values(cur, """
        INSERT INTO apple_tweet_sentiments (
            tweet_id, tweet_text, sentiment_label, sentiment_confidence,
            created_at, query
        ) VALUES %s
        ON CONFLICT (tweet_id) DO NOTHING;
    """, rows)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    data = parse_csv(CSV_PATH)
    insert_data(data)
    print(f"Inserted {len(data)} rows into 'apple_tweets'")
