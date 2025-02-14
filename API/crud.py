from sqlalchemy.orm import Session
import models

def get_telegram_messages(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.TelegramMessage).offset(skip).limit(limit).all()

def get_object_detections(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.ObjectDetection).offset(skip).limit(limit).all()
