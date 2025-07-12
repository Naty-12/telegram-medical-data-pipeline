import os
import psycopg2
from ultralytics import YOLO
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
DB_HOST = "localhost"
DB_NAME = "telegram_medical"
DB_USER = "postgres"
DB_PASSWORD = "Adey"

IMAGE_BASE_DIR = "C:/Users/techin/telegram-medical-data-pipeline"
YOLO_MODEL_NAME = "yolov8n.pt"

# --- Database Connection ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

# --- Fetch Unprocessed Images ---
def get_unprocessed_images(conn):
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                sti.message_id,
                sti.image_path
            FROM public_staging.stg_telegram_images sti
            LEFT JOIN public_staging.image_detections_staging ids
                ON sti.image_path = ids.image_path
            WHERE ids.image_path IS NULL
            AND sti.image_path IS NOT NULL
            GROUP BY sti.message_id, sti.image_path
            ORDER BY sti.message_id
        """)
        unprocessed_images = cur.fetchall()
        cur.close()
        logging.info(f"Found {len(unprocessed_images)} new images to process.")
        return unprocessed_images
    except Exception as e:
        logging.error(f"Error fetching unprocessed images: {e}")
        return []

# --- Load YOLO Model ---
def load_yolo_model(model_name):
    try:
        model = YOLO(model_name)
        logging.info(f"YOLOv8 model '{model_name}' loaded successfully.")
        return model
    except Exception as e:
        logging.error(f"Error loading YOLOv8 model: {e}")
        raise

# --- Run Detection ---
def process_image_for_detection(model, image_full_path):
    detections = []
    try:
        results = model(image_full_path, conf=0.01) # low threshold for debugging
        total_boxes = 0
        for r in results:
            total_boxes += len(r.boxes)
            for box in r.boxes:
                class_id = int(box.cls)
                confidence = float(box.conf)
                detected_class = model.names[class_id]
                detections.append({
                    "detected_object_class": detected_class,
                    "confidence_score": confidence
                })
        if total_boxes == 0:
            logging.warning(f"No objects detected in {image_full_path}.")
    except Exception as e:
        logging.error(f"Error processing image {image_full_path}: {e}")
    return detections

# --- Insert to DB ---
def insert_detections_into_db(conn, detections_to_insert):
    if not detections_to_insert:
        logging.info("No new detections to insert.")
        return

    sql = """
INSERT INTO public_staging.image_detections_staging (
    message_id, image_path, detected_object_class, confidence_score
) VALUES (%s, %s, %s, %s)
ON CONFLICT (message_id, image_path, detected_object_class) DO NOTHING;
"""
    cur = conn.cursor()
    try:
        data = [
            (det['message_id'], det['image_path'], det['detected_object_class'], det['confidence_score'])
            for det in detections_to_insert
        ]
        cur.executemany(sql, data)
        conn.commit()
        logging.info(f"Successfully inserted {len(data)} detection records.")
    except psycopg2.errors.UniqueViolation as e:
        logging.warning(f"Skipped inserting duplicates: {e}")
        conn.rollback()
    except Exception as e:
        logging.error(f"Error inserting detections: {e}")
        conn.rollback()
    finally:
        cur.close()

# --- Main Pipeline ---
def main():
    conn = None
    try:
        conn = get_db_connection()
        model = load_yolo_model(YOLO_MODEL_NAME)
        
        unprocessed_images = get_unprocessed_images(conn)
        all_detections_for_db = []

        for message_id, image_path in unprocessed_images:
            full_image_path = os.path.join(IMAGE_BASE_DIR, image_path.lstrip('/'))

            if os.path.exists(full_image_path):
                logging.info(f"Processing image: {full_image_path} for message_id: {message_id}")
                detections = process_image_for_detection(model, full_image_path)
                for det in detections:
                    all_detections_for_db.append({
                        "message_id": message_id,
                        "image_path": image_path,
                        "detected_object_class": det["detected_object_class"],
                        "confidence_score": det["confidence_score"]
                    })
            else:
                logging.warning(f"Image file not found: {full_image_path}. Skipping.")

        insert_detections_into_db(conn, all_detections_for_db)

    except Exception as e:
        logging.critical(f"Script failed: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()