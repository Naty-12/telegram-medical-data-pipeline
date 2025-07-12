import os
import psycopg2
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

print(f"Connected to DB {DB_NAME} on {DB_HOST}:{DB_PORT} as {DB_USER}")

root_dir = "C:/Users/techin/telegram-medical-data-pipeline/data/raw/telegram_images"

inserted_count = 0
skipped_count = 0

for date_dir in os.listdir(root_dir):
    date_path = os.path.join(root_dir, date_dir)
    if not os.path.isdir(date_path):
        continue

    try:
        image_date = datetime.strptime(date_dir, "%Y-%m-%d").date()
    except ValueError:
        print(f"⚠️ Skipping folder {date_dir}, not a valid date.")
        continue

    for channel in os.listdir(date_path):
        channel_path = os.path.join(date_path, channel)
        if not os.path.isdir(channel_path):
            continue

        for file in os.listdir(channel_path):
            if not (file.endswith(".jpg") or file.endswith(".png")):
                continue

            file_path = os.path.join("data/raw/telegram_images", date_dir, channel, file)  # relative path

            # Expect filename like: message_<message_id>_<unique>.jpg
            try:
                parts = file.split("_")
                message_id = int(parts[1])
            except (IndexError, ValueError):
                print(f"⚠️ Could not extract message_id from filename {file}")
                skipped_count += 1
                continue

            # Verify message_id exists in parent table
            cur.execute("SELECT 1 FROM raw_telegram_messages WHERE message_id=%s;", (message_id,))
            if not cur.fetchone():
                print(f"⚠️ message_id {message_id} does not exist in raw_telegram_messages. Skipping image.")
                skipped_count += 1
                continue

            try:
                cur.execute("""
                    INSERT INTO raw_telegram_images 
                        (message_id, channel_username, image_path, image_date)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    message_id,
                    channel,
                    file_path,
                    image_date
                ))
                inserted_count += 1
            except Exception as e:
                conn.rollback()
                print(f"❌ Failed to insert {file_path}: {e}")
                skipped_count += 1
            else:
                conn.commit()

cur.close()
conn.close()
print(f"✅ Finished loading raw telegram images. Inserted: {inserted_count}, Skipped: {skipped_count}")