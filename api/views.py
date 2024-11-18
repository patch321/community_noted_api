from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.response import Response
import os
from datetime import datetime
from api.utils import download_and_decompress_file, load_data_into_database, check_for_new_data
import logging

logger = logging.getLogger(__name__)

@csrf_exempt
@api_view(['POST'])
def download_and_process_view(request):
    """
    API endpoint to download and process new data if available.
    """
    # Get today's date and format it
    current_date = datetime.now()
    date_path = current_date.strftime('%Y/%m/%d')  # This will format as '2024/03/21' (for example)

    # Construct the download URL with current date
    download_url = f'https://ton.twimg.com/birdwatch-public-data/{date_path}/notes/notes-00000.tsv'
    local_file_path = '/tmp/notes.tsv'
    
    try:
        # Check if new data is available
        is_new_data, download_url = check_for_new_data(download_url)
        if not is_new_data:
            return Response({"message": "No new data to process today."}, status=200)

        # Check if already processed
        if has_been_processed_today():
            logger.info("Data has already been processed today, skipping download")
            return Response({"message": "Data has already been processed today."}, status=200)

        # Download and process the file
        download_and_decompress_file(download_url, local_file_path)

        # Load data into the database
        total_records = load_data_into_database(local_file_path)

        # Save the current date as the last processed date
        current_date = datetime.now().strftime('%Y/%m/%d')
        with open("last_processed_date.txt", "w") as f:
            f.write(current_date)

        return Response({"message": f"Successfully processed {total_records} records."}, status=200)

    except Exception as e:
        return Response({"error": str(e)}, status=500)

    finally:
        # Ensure the file is removed after processing
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

def has_been_processed_today():
    """Check if the data has already been processed today."""
    current_date = datetime.now().strftime('%Y-%m-%d')
    # You might want to implement this check against your database
    # For now, we'll just log the check
    logger.info(f"Checking if data was already processed for {current_date}")
    return False  # Replace with actual implementation
