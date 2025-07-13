# my_api_project/schemas.py
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime, date

# --- Base Schemas for Data Mart Entities (for internal use or detailed responses) ---

class ChannelBase(BaseModel):
    channel_sk: str = Field(..., description="Surrogate key for the channel.")
    channel_username: str = Field(..., description="Username of the Telegram channel.")

    class Config:
        from_attributes = True # For Pydantic V2, use orm_mode = True for Pydantic V1

class DateBase(BaseModel):
    date_pk: date = Field(..., description="Primary key for the date (YYYY-MM-DD).")
    year: int
    month: int
    month_name_full: str
    day_of_month: int
    day_of_week: int
    day_name_full: str
    is_weekend: bool

    class Config:
        from_attributes = True

class MessageBase(BaseModel):
    message_id: int
    channel_fk: str = Field(..., description="Foreign key to dim_channels.")
    date_fk: date = Field(..., description="Foreign key to dim_dates.")
    sender_id: Optional[str] = None
    message_text: Optional[str] = None
    message_views: int
    attached_media_type: Optional[str] = None
    attached_media_path: Optional[str] = None
    message_length: Optional[int] = None
    has_media_attachment: bool
    has_image_attachment: bool
    full_message_timestamp: datetime
    num_images_attached: int

    class Config:
        from_attributes = True

class ImageDetectionBase(BaseModel):
    message_id: int
    image_path: str
    detected_object_class: str
    confidence_score: float
    detection_timestamp: datetime

    class Config:
        from_attributes = True

# --- Analytical Response Schemas (for API endpoints) ---

class TopProduct(BaseModel):
    detected_object_class: str = Field(..., description="Name of the detected product/object class.")
    detection_count: int = Field(..., description="Number of times this product/object was detected.")

class ChannelActivity(BaseModel):
    channel_username: str = Field(..., description="Username of the Telegram channel.")
    date_fk: date = Field(..., description="Foreign key to dim_dates.")
    total_messages: int = Field(..., description="Total messages posted on this date.")
    total_views: int = Field(..., description="Total views across all messages on this date.")
    messages_with_images: int = Field(..., description="Number of messages with image attachments on this date.")
    avg_message_length: Optional[float] = Field(..., description="Average length of messages on this date.")

class MessageSearchResult(BaseModel):
    message_id: int
    channel_username: str
    message_text: str
    message_views: int
    full_message_timestamp: datetime

class HealthCheckResponse(BaseModel):
    status: str
    database_connection: str
    message: str