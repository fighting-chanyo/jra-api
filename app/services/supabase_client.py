import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

supabase_client: Client = create_client(url, key)

def get_supabase_client() -> Client:
    return supabase_client