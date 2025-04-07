from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.responses import JSONResponse
from pymongo import MongoClient
import os, shutil, math

UPLOAD_DIR = "storage/app/medalists/"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "medalists_db"
COLLECTION_NAME = "medalists"

os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI()
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")
    
    file_location = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_location, "wb") as f:
        shutil.copyfileobj(file.file, f)

    return {"message": "Upload successful", "file": file.filename}

@app.get("/aggregated_stats/event")
def get_event_stats(request: Request, page: int = 1, limit: int = 10):
    skip = (page - 1) * limit
    total_events = collection.distinct("event")
    total_pages = math.ceil(len(total_events) / limit)

    events = collection.aggregate([
        {"$group": {
            "_id": {
                "discipline": "$discipline",
                "event": "$event",
                "event_date": "$event_date"
            },
            "medalists": {"$push": {
                "name": "$name",
                "medal_type": "$medal_type",
                "gender": "$gender",
                "country": "$country",
                "country_code": "$country_code",
                "nationality": "$nationality",
                "medal_code": "$medal_code",
                "medal_date": "$medal_date"
            }}
        }},
        {"$skip": skip},
        {"$limit": limit}
    ])

    data = []
    for e in events:
        info = e["_id"]
        data.append({
            "discipline": info["discipline"],
            "event": info["event"],
            "event_date": info["event_date"],
            "medalists": e["medalists"]
        })

    base_url = str(request.base_url).rstrip("/")
    return JSONResponse({
        "data": data,
        "paginate": {
            "current_page": page,
            "total_pages": total_pages,
            "next_page": f"{base_url}/aggregated_stats/event?page={page+1}" if page < total_pages else None,
            "previous_page": f"{base_url}/aggregated_stats/event?page={page-1}" if page > 1 else None
        }
    })