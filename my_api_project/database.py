# my_api_project/database.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor # To get results as dictionaries
from dotenv import load_dotenv

# Load environment variables from .env file (ensure .env is in your project root or accessible)
load_dotenv(dotenv_path=".env", override=True)

# Database connection details from environment variables
# IMPORTANT: Ensure these environment variables are set (e.g., in a .env file)
DB_NAME = os.getenv("POSTGRES_DB", "telegram_medical")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Adey") # <<< IMPORTANT: Replace or ensure .env is loaded
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")


def get_db_connection():
    """Establishes and returns a new PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        # In a real application, you'd want more robust error handling/logging here
        raise

def get_db_cursor(conn):
    """Returns a cursor for the given connection, configured to return dictionaries."""
    return conn.cursor(cursor_factory=RealDictCursor)

# Dependency for FastAPI to get a DB connection per request
def get_db():
    """FastAPI dependency that provides a database connection and ensures it's closed."""
    conn = None
    try:
        conn = get_db_connection()
        yield conn
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Simple test to verify database connection
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT current_database();")
        print("Successfully connected to database:", cur.fetchone()[0])
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Connection test failed: {e}")