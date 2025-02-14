from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import crud, models, schemas
from database import engine, SessionLocal, get_db

# Create tables
models.Base.metadata.create_all(bind=engine)

app = FastAPI()

@app.get("/")
def home():
    return {"message": "FastAPI is running!"}

@app.get("/telegram_messages", response_model=list[schemas.TelegramMessageSchema])
def read_telegram_messages(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return crud.get_telegram_messages(db, skip, limit)

@app.get("/object_detections", response_model=list[schemas.ObjectDetectionSchema])
def read_object_detections(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return crud.get_object_detections(db, skip, limit)
