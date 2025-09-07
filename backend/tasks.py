import os
from celery import Celery
import whisper
from supabase import create_client, Client
from dotenv import load_dotenv
from transformers import pipeline # NEW: Import the pipeline helper
import nltk # NEW: Import for sentence splitting

# --- SETUP ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

celery_app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

# --- LOAD ALL AI MODELS ON STARTUP ---
print("Loading all AI models...")
# 1. Transcription Model
transcription_model = whisper.load_model("base")

# 2. NEW: Summarization Model
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# 3. NEW: Zero-Shot Classification Model (for topic modeling)
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# 4. NEW: Download sentence tokenizer data
nltk.download('punkt')
print("All models loaded.")


# --- THE ENHANCED BACKGROUND TASK ---
@celery_app.task(name="process_audio_task")
def process_audio_task(upload_id: str):
    print(f"Starting enhanced task for upload_id: {upload_id}")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    local_path = None
    
    try:
        # 1. Get file path and download (Same as before)
        upload_record = supabase.table("uploads").select("storage_path").eq("id", upload_id).single().execute()
        storage_path = upload_record.data['storage_path']
        
        print(f"Downloading file: {storage_path}")
        local_path = f"/tmp/{os.path.basename(storage_path)}"
        with open(local_path, 'wb+') as f:
            res = supabase.storage.from_("media-uploads").download(storage_path)
            f.write(res)
        print("File downloaded.")

        # 2. Transcribe with Whisper AI (Same as before)
        print("Transcribing audio...")
        result = transcription_model.transcribe(local_path, fp16=False)
        transcript = result["text"]
        print("Transcription complete.")
        
        # --- NEW AI STEPS ---

        # 3. Generate Summary
        print("Generating summary...")
        summary_result = summarizer(transcript, max_length=150, min_length=30, do_sample=False)
        summary = summary_result[0]['summary_text']
        print("Summary generated.")

        # 4. Classify Topics
        print("Classifying topics...")
        candidate_labels = ["Pricing Inquiry", "Technical Issue", "Positive Feedback", "Feature Request", "General Question"]
        sentences = nltk.sent_tokenize(transcript)
        
        segments_to_insert = []
        for sentence in sentences:
            if not sentence.strip(): continue # Skip empty sentences
            
            # Run classification on each sentence
            classification_result = classifier(sentence, candidate_labels)
            top_label = classification_result['labels'][0]
            
            segments_to_insert.append({
                "upload_id": upload_id,
                "segment_text": sentence,
                "topic_label": top_label
            })
        
        # Insert all segments into the new table in one go
        if segments_to_insert:
            supabase.table("analysis_segments").insert(segments_to_insert).execute()
        print("Topic classification complete.")

        # --- END OF NEW STEPS ---

        # 5. Update the main record with transcript AND summary
        supabase.table("uploads").update({
            "transcript": transcript,
            "summary": summary, # Add the new summary
            "status": "completed"
        }).eq("id", upload_id).execute()
        print(f"Task for {upload_id} completed successfully.")

    except Exception as e:
        print(f"Error processing {upload_id}: {e}")
        supabase.table("uploads").update({"status": "failed"}).eq("id", upload_id).execute()
    
    finally:
        # 6. Clean up the downloaded file (Same as before)
        if local_path and os.path.exists(local_path):
            os.remove(local_path)