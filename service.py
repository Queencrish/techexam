import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pymongo import MongoClient
from datetime import datetime

WATCH_DIR = "storage/app/medalists/"
PROCESSED_DIR = "storage/app/processed/"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "medalists_db"
COLLECTION_NAME = "medalists"

os.makedirs(WATCH_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


def process_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            record = {
                "discipline": row.get("discipline", ""),
                "event": row.get("event", ""),
                "event_date": row.get("medal_date", ""),
                "name": row.get("name", ""),
                "medal_type": row.get("medal_type", ""),
                "gender": row.get("gender", ""),
                "country": row.get("country", ""),
                "country_code": row.get("country_code", ""),
                "nationality": row.get("nationality", ""),
                "medal_code": row.get("medal_code", ""),
                "medal_date": row.get("medal_date", "")
            }

            # Date parsing
            for key in ["event_date", "medal_date"]:
                try:
                    record[key] = datetime.strptime(record[key], "%Y-%m-%d")
                except:
                    pass

            # Check duplicates (based on name + event)
            if not collection.find_one({"name": record["name"], "event": record["event"]}):
                collection.insert_one(record)

        print(f"Processed: {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        time.sleep(1)  
        process_csv(event.src_path)
        os.rename(event.src_path, os.path.join(PROCESSED_DIR, os.path.basename(event.src_path)))

if __name__ == "__main__":
    print("Starting background service...")
    event_handler = CSVHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIR, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
