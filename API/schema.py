from pydantic import BaseModel
from typing import Optional

class TelegramMessageSchema(BaseModel):
    id: int
    channel_title: str
    channel_username: str
    message_id: int
    message: str
    message_date: str
    media_path: str

    class Config:
        orm_mode = True

class ObjectDetectionSchema(BaseModel):
    id: int
    image_path: str
    detected_class: str
    confidence: float
    bbox: str

    class Config:
        orm_mode = True
