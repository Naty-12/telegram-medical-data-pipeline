import os
import json
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# ✅ Check connected database immediately
cur.execute("SELECT current_database();")
print("Connected to database:", cur.fetchone()[0])

root_dir = "C:/Users/techin/telegram-medical-data-pipeline/data/raw/telegram_messages"

for date_dir in os.listdir(root_dir):
    date_path = os.path.join(root_dir, date_dir)
    if os.path.isdir(date_path):
        for channel in os.listdir(date_path):
            channel_path = os.path.join(date_path, channel)
            if os.path.isdir(channel_path):
                for file in os.listdir(channel_path):
                    if file.endswith(".json"):
                        file_path = os.path.join(channel_path, file)
                        with open(file_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            message_id = data.get("id") or data.get("message_id")
                            if not message_id:
                                print(f"⚠️ No message_id in {file_path}, skipping...")
                                continue
                            message_timestamp = data.get("date")
                            if isinstance(message_timestamp, str):
                                try:
                                    message_timestamp = datetime.fromisoformat(message_timestamp)
                                except Exception:
                                    print(f"⚠️ Bad timestamp in {file_path}, using now.")
                                    message_timestamp = datetime.utcnow()
                            elif message_timestamp is None:
                                message_timestamp = datetime.utcnow()
                            try:
                                cur.execute("""
                                    INSERT INTO raw_telegram_messages 
                                        (message_id, channel_username, message_timestamp, raw_json)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (message_id) DO NOTHING;
                                """, (
                                    message_id,
                                    channel,
                                    message_timestamp,
                                    Json(data)
                                ))
                            except Exception as e:
                                conn.rollback()
                                print(f"❌ Failed to insert {file_path}: {e}")
                            else:
                                conn.commit()

cur.close()
conn.close()
print("✅ Finished loading raw telegram messages.")