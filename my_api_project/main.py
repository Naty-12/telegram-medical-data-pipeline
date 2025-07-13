# my_api_project/main.py
from fastapi import FastAPI, Depends, HTTPException, Query
from typing import List, Optional
import psycopg2

# Import your database connection and CRUD functions
from .database import get_db
from . import crud
from . import schemas

app = FastAPI(
    title="Telegram Medical Data Analytical API",
    description="API to query dbt-transformed Telegram medical data for analytical insights.",
    version="1.0.0"
)

# --- Health Check Endpoint ---
@app.get("/health", response_model=schemas.HealthCheckResponse, summary="Health check endpoint")
async def health_check(db: psycopg2.extensions.connection = Depends(get_db)):
    """
    Checks the health of the API and its database connection.
    """
    try:
        with db.cursor() as cur:
            cur.execute("SELECT 1")
        db_status = "Connected"
    except Exception as e:
        db_status = f"Failed: {e}"
        # If the database connection fails, raise an HTTPException
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

    return schemas.HealthCheckResponse(
        status="OK",
        database_connection=db_status,
        message="API is running and database connection is active."
    )

# --- Analytical Endpoints ---

@app.get(
    "/api/reports/top-products",
    response_model=List[schemas.TopProduct],
    summary="Get the most frequently detected products"
)
async def get_top_products_report(
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
    db: psycopg2.extensions.connection = Depends(get_db)
):
    """
    Returns a list of the most frequently detected objects/products from image analysis.
    This queries the `public_marts.fct_image_detections` table.
    """
    products = crud.get_top_products(db, limit=limit)
    if not products:
        # Return an empty list if no products are found, as it's a "report"
        return []
    return products

@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=List[schemas.ChannelActivity],
    summary="Get posting activity for a specific channel"
)
async def get_channel_activity_report(
    channel_name: str,
    db: psycopg2.extensions.connection = Depends(get_db)
):
    """
    Returns the daily posting activity (messages, views, images, avg length)
    for a specified Telegram channel.
    """
    activity = crud.get_channel_activity(db, channel_name=channel_name)
    if not activity:
        raise HTTPException(status_code=404, detail=f"No activity found for channel: {channel_name}")
    return activity

@app.get(
    "/api/search/messages",
    response_model=List[schemas.MessageSearchResult],
    summary="Search for messages containing a specific keyword"
)
async def search_telegram_messages(
    query: str = Query(..., min_length=2, description="Keyword to search for in message text"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of messages to return"),
    db: psycopg2.extensions.connection = Depends(get_db)
):
    """
    Searches the `public_marts.fct_messages` table for messages containing the specified keyword
    in their text content.
    """
    messages = crud.search_messages(db, query_text=query, limit=limit)
    if not messages:
        raise HTTPException(status_code=404, detail=f"No messages found matching query: '{query}'")
    return messages