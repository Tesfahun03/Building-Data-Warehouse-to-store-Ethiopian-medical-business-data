from sqlalchemy import Column, Integer, String, Float
from database import Base

class TelegramMessage(Base):
    __tablename__ = "telegram_messages"

    id = Column(Integer, primary_key=True, index=True)
    channel_title = Column(String)
    channel_username = Column(String)
    message_id = Column(Integer, unique=True, index=True)
    message = Column(String)
    message_date = Column(String)
    media_path = Column(String)

class ObjectDetection(Base):
    __tablename__ = "object_detections"

    id = Column(Integer, primary_key=True, index=True)
    image_path = Column(String)
    detected_class = Column(String)
    confidence = Column(Float)
    bbox = Column(String)
