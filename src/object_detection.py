# pip install opencv-python torch torchvision
# git clone https://github.com/ultralytics/yolov5.git
# cd yolov5
# pip install -r requirements.txt


import os
import logging
import torch
import cv2
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from ultralytics import YOLO

# Load environment variables
load_dotenv("../.env")

# Database connection settings
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# Ensure logs folder exists
os.makedirs("../logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../logs/object_detection.log"),
        logging.StreamHandler()
    ]
)

class YOLODetector:
    def __init__(self, model_path="yolov5s.pt"):
        """Initialize YOLO model."""
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = YOLO(model_path)
        logging.info(f"YOLO model loaded on {self.device}")

    def detect_objects(self, image_path):
        """Run object detection on an image."""
        results = self.model(image_path)
        detections = []

        for result in results:
            for box in result.boxes:
                detection = {
                    "image_path": image_path,
                    "class": result.names[int(box.cls.item())],
                    "confidence": round(box.conf.item(), 4),
                    "bbox": [round(x, 2) for x in box.xyxy[0].tolist()]
                }
                detections.append(detection)

        return detections

    def process_images(self, image_dir):
        """Process all images in a directory."""
        images = list(Path(image_dir).glob("*.jpg"))
        all_detections = []

        for image in images:
            logging.info(f"Processing {image}")
            detections = self.detect_objects(str(image))
            all_detections.extend(detections)

        return all_detections

class DatabaseManager:
    def __init__(self):
        """Initialize database connection."""
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        self.engine = create_engine(DATABASE_URL)

    def create_detection_table(self):
        """Create object detection results table."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS object_detections (
            id SERIAL PRIMARY KEY,
            image_path TEXT,
            detected_class TEXT,
            confidence FLOAT,
            bbox TEXT
        );
        """
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(create_table_query))
        logging.info("Table 'object_detections' ensured in database.")

    def insert_detections(self, detections):
        """Insert detection results into the database."""
        insert_query = """
        INSERT INTO object_detections (image_path, detected_class, confidence, bbox) 
        VALUES (:image_path, :detected_class, :confidence, :bbox);
        """
        with self.engine.begin() as conn:
            for det in detections:
                conn.execute(text(insert_query), {
                    "image_path": det["image_path"],
                    "detected_class": det["class"],
                    "confidence": det["confidence"],
                    "bbox": str(det["bbox"])
                })
        logging.info(f"{len(detections)} detection results inserted into the database.")

if __name__ == "__main__":
    image_dir = "../data/photos"
    
    yolo = YOLODetector()
    db_manager = DatabaseManager()
    
    db_manager.create_detection_table()
    detections = yolo.process_images(image_dir)
    db_manager.insert_detections(detections)
