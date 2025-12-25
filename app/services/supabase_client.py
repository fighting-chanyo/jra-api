import os
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

supabase_client: Client = create_client(url, key)

def get_supabase_client() -> Client:
    return supabase_client

def get_analysis_queue(queue_id: str):
    try:
        response = supabase_client.table("analysis_queue").select("*").eq("id", queue_id).single().execute()
        return response.data
    except Exception as e:
        logging.error(f"Error fetching analysis queue {queue_id}: {e}")
        return None

def update_analysis_status(queue_id: str, status: str, result_json: dict = None, error_message: str = None, image_path: str = None):
    try:
        data = {"status": status}
        if result_json:
            data["result_json"] = result_json
        if error_message:
            data["error_message"] = error_message
        if image_path:
            data["image_path"] = image_path
        
        supabase_client.table("analysis_queue").update(data).eq("id", queue_id).execute()
    except Exception as e:
        logging.error(f"Error updating analysis queue {queue_id}: {e}")

def download_file(bucket: str, path: str) -> bytes:
    try:
        response = supabase_client.storage.from_(bucket).download(path)
        return response
    except Exception as e:
        logging.error(f"Error downloading file {bucket}/{path}: {e}")
        return None

def delete_file(bucket: str, path: str):
    try:
        supabase_client.storage.from_(bucket).remove([path])
    except Exception as e:
        logging.error(f"Error deleting file {bucket}/{path}: {e}")
