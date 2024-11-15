from django.http import JsonResponse
from api.utils import download_and_decompress_file, load_data_into_database
import os

def download_and_process_view(request):
    download_url = 'https://ton.twimg.com/birdwatch-public-data/2024/11/15/notes/notes-00000.tsv'  # Replace with your actual URL
    local_file_path = '/tmp/notes.tsv'

    try:
        # Step 1: Download and decompress the file
        download_and_decompress_file(download_url, local_file_path)

        # Step 2: Load data into the database
        total_records = load_data_into_database(local_file_path)
        return JsonResponse({"message": f"Successfully processed {total_records} records."})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
    finally:
        # Clean up the local file after processing
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
