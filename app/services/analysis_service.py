import logging
import os
from app.services.supabase_client import get_analysis_queue, update_analysis_status, download_file, delete_file
from app.services.gemini_service import GeminiService
from app.services.gcs_service import GCSService

gemini_service = GeminiService()
gcs_service = GCSService()

GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "jra-ipat-scraper-images")
SUPABASE_BUCKET_NAME = "ticket-images"

async def process_analysis_queue(queue_id: str):
    logging.info(f"Processing analysis queue: {queue_id}")
    
    # 1. Get Queue Record
    queue_record = get_analysis_queue(queue_id)
    if not queue_record:
        logging.error(f"Queue record not found: {queue_id}")
        return

    image_path = queue_record.get("image_path")
    if not image_path:
        update_analysis_status(queue_id, "error", error_message="Image path not found")
        return

    update_analysis_status(queue_id, "processing")

    try:
        # 2. Download Image from Supabase
        image_bytes = download_file(SUPABASE_BUCKET_NAME, image_path)
        if not image_bytes:
            update_analysis_status(queue_id, "error", error_message="Failed to download image")
            return

        # 3. Analyze Image
        result = await gemini_service.analyze_image(image_bytes)
        if not result:
             update_analysis_status(queue_id, "error", error_message="Analysis failed")
             return

        # Overwrite date if date_order is present
        date_order = queue_record.get("date_order")
        if date_order:
            logging.info(f"Overwriting race date with date_order: {date_order}")
            if result.race:
                result.race.date = date_order
            else:
                from app.schemas import RaceInfo
                result.race = RaceInfo(date=date_order)

        # 4. Upload to GCS
        gcs_path = f"archive/{image_path}" 
        upload_success = gcs_service.upload_bytes(GCS_BUCKET_NAME, gcs_path, image_bytes)
        
        new_image_path = None
        if upload_success:
            # Generate Public URL
            # Format: https://storage.googleapis.com/{bucket}/{path}
            new_image_path = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{gcs_path}"
            logging.info(f"GCS Upload Success. Public URL: {new_image_path}")
        else:
            logging.warning(f"Failed to upload to GCS: {gcs_path}. Proceeding anyway.")

        # 5. Update Queue Status (with new image path if available)
        update_analysis_status(
            queue_id, 
            "completed", 
            result_json=result.model_dump(),
            image_path=new_image_path
        )

        # 6. Delete from Supabase (Only if GCS upload was successful)
        if upload_success:
             delete_file(SUPABASE_BUCKET_NAME, image_path)
        
        logging.info(f"Analysis completed for {queue_id}")

    except Exception as e:
        logging.error(f"Unexpected error processing {queue_id}: {e}")
        update_analysis_status(queue_id, "error", error_message=str(e))
