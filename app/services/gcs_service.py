import logging
import os
from typing import Optional

from google.cloud import storage

logger = logging.getLogger(__name__)

class GCSService:
    def __init__(self):
        self.client: Optional[storage.Client] = None
        self._init_error: Optional[str] = None
        self._init_client()

    def _init_client(self) -> None:
        """Initialize GCS client using ADC.

        Notes:
        - On Cloud Run, ADC should work without a key file.
        - If GOOGLE_APPLICATION_CREDENTIALS is set but points to a missing file,
          google-auth fails early and does not fall back to metadata.
          We detect that case and (only on Cloud Run) temporarily ignore the env var
          to allow metadata-based ADC.
        """
        try:
            credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if credentials_path and not os.path.exists(credentials_path):
                logger.error(
                    "GOOGLE_APPLICATION_CREDENTIALS points to a missing file: %s",
                    credentials_path,
                )

                running_on_cloud_run = bool(
                    os.environ.get("K_SERVICE")
                    or os.environ.get("K_REVISION")
                    or os.environ.get("CLOUD_RUN_JOB")
                )

                if running_on_cloud_run:
                    logger.warning(
                        "Running on Cloud Run; attempting ADC fallback by ignoring GOOGLE_APPLICATION_CREDENTIALS"
                    )
                    saved = os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
                    try:
                        self.client = storage.Client()
                        self._init_error = None
                        logger.info("GCS Client initialized successfully (Cloud Run ADC fallback).")
                        return
                    finally:
                        if saved is not None:
                            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = saved

            self.client = storage.Client()
            self._init_error = None
            logger.info("GCS Client initialized successfully.")
        except Exception:
            self.client = None
            self._init_error = "GCS Client init failed (see stacktrace)"
            logger.exception("Failed to initialize GCS Client")

    def upload_bytes(self, bucket_name: str, destination_blob_name: str, content: bytes, content_type: str = "image/jpeg"):
        """Uploads bytes to a GCS bucket."""
        if not self.client:
            # Retry once in case credentials become available after import-time init.
            self._init_client()
        if not self.client:
            logger.error("GCS Client is not initialized.")
            if self._init_error:
                logger.error("GCS init error: %s", self._init_error)
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
