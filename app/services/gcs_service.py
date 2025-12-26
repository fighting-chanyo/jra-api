import os
from google.cloud import storage
import logging

logger = logging.getLogger(__name__)

class GCSService:
    def __init__(self):
        self.client = None
        try:
            self.client = storage.Client()
            logger.info("GCS Client initialized successfully.")
        except Exception as e:
            logger.error("Failed to initialize GCS Client: %s", e)

    def upload_bytes(self, bucket_name: str, destination_blob_name: str, content: bytes, content_type: str = "image/jpeg"):
        """Uploads bytes to a GCS bucket."""
        if not self.client:
            logger.error("GCS Client is not initialized.")
            return False

        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(content, content_type=content_type)
            logger.info("File uploaded to %s/%s", bucket_name, destination_blob_name)
            return True
        except Exception as e:
            logger.exception("Failed to upload to GCS")
            return False
