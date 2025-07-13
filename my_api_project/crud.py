# my_api_project/crud.py
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor # Already imported via get_db_cursor

# Assuming DB connection functions are in database.py
from .database import get_db_cursor

def get_top_products(conn: psycopg2.extensions.connection, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Returns the most frequently detected objects/products from fct_image_detections.
    """
    query = """
    SELECT
        detected_object_class,
        COUNT(*) AS detection_count -- Use COUNT(*) as it's counting rows
    FROM
        public_marts.fct_image_detections
    WHERE
        detected_object_class IS NOT NULL
    GROUP BY
        detected_object_class
    ORDER BY
        detection_count DESC
    LIMIT %s;
    """
    with get_db_cursor(conn) as cur:
        cur.execute(query, (limit,))
        results = cur.fetchall()
    return results

def get_channel_activity(conn: psycopg2.extensions.connection, channel_name: str) -> List[Dict[str, Any]]:
    """
    Returns the posting activity for a specific channel, aggregated by date.
    """
    query = """
    SELECT
        dc.channel_username,
        dd.full_date AS date_fk, -- Changed 'date' to 'date_fk' to match the Pydantic schema
        COUNT(fm.message_id) AS total_messages,
        SUM(fm.message_views) AS total_views,
        SUM(CASE WHEN fm.has_image_attachment THEN 1 ELSE 0 END) AS messages_with_images,
        AVG(fm.message_length) AS avg_message_length
    FROM
        public_marts.fct_messages fm
    JOIN
        public_marts.dim_channels dc ON fm.channel_fk = dc.channel_sk
    JOIN
        public_marts.dim_dates dd ON fm.date_fk = dd.date_pk
    WHERE
        dc.channel_username ILIKE %s -- Case-insensitive search
    GROUP BY
        dc.channel_username,
        dd.full_date
    ORDER BY
        dd.full_date ASC;
    """
    with get_db_cursor(conn) as cur:
        cur.execute(query, (channel_name,))
        results = cur.fetchall()
    return results

def search_messages(conn: psycopg2.extensions.connection, query_text: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Searches for messages containing a specific keyword in their text.
    Uses PostgreSQL's ILIKE for case-insensitive substring matching.
    For production, consider full-text search (e.g., tsvector/tsquery).
    """
    sql_query_text = f"%{query_text}%" # Add wildcards for substring search
    query = """
    SELECT
        fm.message_id,
        dc.channel_username,
        fm.message_text,
        fm.message_views,
        fm.full_message_timestamp
    FROM
        public_marts.fct_messages fm
    JOIN
        public_marts.dim_channels dc ON fm.channel_fk = dc.channel_sk
    WHERE
        fm.message_text ILIKE %s
    ORDER BY
        fm.full_message_timestamp DESC
    LIMIT %s;
    """
    with get_db_cursor(conn) as cur:
        cur.execute(query, (sql_query_text, limit))
        results = cur.fetchall()
    return results