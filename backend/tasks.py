import os
from celery import Celery
import whisper # The AI model library
from supabase import create_client, Client
from dotenv import load_dotenv

# --- CELERY & SUPABASE SETUP ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Initialize Celery and connect it to our Redis server
celery_app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

# Load the Whisper model. It will download the model files the first time.
# 'tiny' is small and fast. 'base' is better. 'medium' is even better.
print("Loading Whisper model...")
model = whisper.load_model("base")
print("Whisper model loaded.")


# --- THE BACKGROUND TASK ---
@celery_app.task(name="process_audio_task")
def process_audio_task(upload_id: str):
    """The main background job to process an audio file."""
    print(f"Starting task for upload_id: {upload_id}")

    # We need to create a new Supabase client inside the task
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    try:
        # 1. Get the file path from the database
        upload_record = supabase.table("uploads").select("storage_path").eq("id", upload_id).single().execute()
        storage_path = upload_record.data['storage_path']

        # 2. Download the audio file from Supabase Storage
        print(f"Downloading file: {storage_path}")
        # Create a temporary local path to save the file
        local_path = f"/tmp/{os.path.basename(storage_path)}"
        with open(local_path, 'wb+') as f:
            res = supabase.storage.from_("media-uploads").download(storage_path)
            f.write(res)
        print("File downloaded.")

        # 3. Transcribe with Whisper AI
        print("Transcribing audio...")
        result = model.transcribe(local_path)
        transcript = result["text"]
        print("Transcription complete.")

        # 4. Update the database record with the transcript
        supabase.table("uploads").update({
            "transcript": transcript,
            "status": "completed"
        }).eq("id", upload_id).execute()
        print(f"Task for {upload_id} completed successfully.")

    except Exception as e:
        print(f"Error processing {upload_id}: {e}")
        # If something goes wrong, mark it as 'failed' in the database
        supabase.table("uploads").update({"status": "failed"}).eq("id", upload_id).execute()

    finally:
        # 5. Clean up the downloaded file
        if os.path.exists(local_path):
            os.remove(local_path)