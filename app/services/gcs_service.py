import os
from google.cloud import storage
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GCSService:
    def __init__(self):
        self.client = None
        try:
            self.client = storage.Client()
            logging.info("GCS Client initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize GCS Client: {e}")

    def upload_bytes(self, bucket_name: str, destination_blob_name: str, content: bytes, content_type: str = "image/jpeg"):
        """Uploads bytes to a GCS bucket."""
        if not self.client:
            logging.error("GCS Client is not initialized.")
            return False

        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(content, content_type=content_type)
            logging.info(f"File uploaded to {bucket_name}/{destination_blob_name}")
            return True
        except Exception as e:
            logging.error(f"Failed to upload to GCS: {e}")
            return False
