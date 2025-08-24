import os
from fastapi import FastAPI, HTTPException
from supabase import create_client, Client 
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

app = FastAPI()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

class UploadNotification(BaseModel):
    storage_path:str

@app.get("/")
def read_root():
    return {"Server is running"}

@app.post("/uploads/initiate")
def initiate_upload():
    unique_file_name = f"audio_{os.urandom(8).hex()}.mp3"
    return {"message": "Upload initiated", "storage_path": unique_file_name}

@app.post("/uploads/notify")
def notify_upload(notification: UploadNotification):
    storage_path = notification.storage_path

    try:
        response = supabase.table("uploads").insert({
            "storage_path": storage_path,
            "file_name": os.path.basename(storage_path),
            "status": "processing"
        }).execute()

        upload_id = response.data[0]['id']

        # TODO: Add processing logic here
        print(f"Upload record created with ID: {upload_id}. ready to process")
        
        return {"message": "Upload recorded, processing started", "upload_id": upload_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

