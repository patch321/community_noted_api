import gzip
import os
import requests
import django.db.transaction as transaction
import logging
import time

logger = logging.getLogger(__name__)

def debug_log(message, level='info', success=None, error=False):
    """
    Helper function to handle both console printing and logging.
    
    Args:
        message (str): The message to print and log
        level (str): The logging level ('debug', 'info', 'warning', 'error')
        success (bool|None): If True, adds ✓, if False adds ✗, if None adds nothing
        error (bool): If True, prints in error format
    """
    # Add status indicator if specified
    prefix = ''
    if success is True:
        prefix = '✓ '
    elif success is False:
        prefix = '✗ '
    
    # Console output
    console_message = f"{prefix}{message}"
    if error:
        print(f"\033[91m{console_message}\033[0m")  # Red text
    else:
        print(console_message)

    # Logger output
    log_func = getattr(logger, level.lower())
    log_func(message)

def download_and_decompress_file(url, local_file_path):
    """
    Downloads and decompresses a GZipped file.
    """
    debug_log(f"\nDownloading file from {url}")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(local_file_path, 'wb') as local_file:
            with gzip.GzipFile(fileobj=response.raw) as gzip_file:
                local_file.write(gzip_file.read())
        debug_log("File successfully downloaded and decompressed", success=True)
    except Exception as e:
        debug_log(f"Error downloading file: {e}", level='error', success=False, error=True)
        raise

from .models import Note

def parse_note_line(line):
    """
    Parses a single line of TSV into a Note instance.
    """
    fields = line.split('\t')
    if len(fields) != 23:  # Adjust based on your TSV structure
        return None

    try:
        # More aggressive Unicode cleaning for the summary field
        summary = fields[21]
        try:
            # Try to encode and decode to remove problematic characters
            summary = summary.encode('ascii', errors='ignore').decode('ascii')
        except UnicodeError:
            # If that fails, try a more aggressive cleaning
            summary = ''.join(char for char in summary if ord(char) < 128)
        
        logger.debug(f"Processed summary: {summary[:50]}...")  # Log first 50 chars
        
        return Note(
            note_id=int(fields[0]),
            note_author_participant_id=fields[1],
            created_at_millis=int(fields[2]),
            tweet_id=int(fields[3]),
            classification=fields[4],
            believable=fields[5],
            harmful=fields[6],
            validation_difficulty=fields[7],
            misleading_other=fields[8] == "1",
            misleading_factual_error=fields[9] == "1",
            misleading_manipulated_media=fields[10] == "1",
            misleading_outdated_information=fields[11] == "1",
            misleading_missing_important_context=fields[12] == "1",
            misleading_unverified_claim_as_fact=fields[13] == "1",
            misleading_satire=fields[14] == "1",
            not_misleading_other=fields[15] == "1",
            not_misleading_factually_correct=fields[16] == "1",
            not_misleading_outdated_but_not_when_written=fields[17] == "1",
            not_misleading_clearly_satire=fields[18] == "1",
            not_misleading_personal_opinion=fields[19] == "1",
            trustworthy_sources=fields[20] == "1",
            summary=summary,
            is_media_note=fields[22] == "1",
        )
    except Exception as e:
        logger.error(f"Error parsing note line: {e}")
        return None


from .models import Note

def process_batch_with_retry(batch, max_retries=3, retry_delay=5):
    """
    Processes a batch with retry logic for database operations.
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            return process_batch(batch)
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:  # Not the last attempt
                debug_log(f"Database error (attempt {attempt + 1}/{max_retries}). "
                         f"Retrying in {retry_delay} seconds...", 
                         level='warning')
                time.sleep(retry_delay)
            
    # If we get here, all retries failed
    debug_log(f"Failed to process batch after {max_retries} attempts: {last_error}", 
             level='error', success=False, error=True)
    raise last_error

def load_data_into_database(file_path):
    """
    Reads a TSV file and processes the data in batches.
    """
    debug_log(f"\nStarting data load from {file_path}")
    
    total_records_before = Note.objects.count()
    actual_new_count = 0
    actual_update_count = 0
    batch_size = 10000
    batch = []
    line_count = 0

    try:
        with open(file_path, 'r') as file:
            next(file)  # Skip the header
            
            for line in file:
                line_count += 1
                if line_count % 10000 == 0:
                    debug_log(f"Processing line {line_count}...")
                
                note_data = parse_note_line(line.strip())
                if note_data:
                    batch.append(note_data)

                    if len(batch) >= batch_size:
                        new_records, updated_records = process_batch_with_retry(batch)
                        actual_new_count += new_records
                        actual_update_count += updated_records
                        debug_log(f"Batch complete: {new_records} new, {updated_records} updated", 
                                success=True)
                        batch = []

            # Process any remaining records
            if batch:
                new_records, updated_records = process_batch_with_retry(batch)
                actual_new_count += new_records
                actual_update_count += updated_records
                debug_log(f"Final batch complete: {new_records} new, {updated_records} updated", 
                        success=True)

        # Get final count after all processing
        total_records_after = Note.objects.count()
        actual_records_added = total_records_after - total_records_before

        summary = (f"\nData load complete!\n"
                  f"Total lines processed: {line_count:,}\n"
                  f"Total records before: {total_records_before:,}\n"
                  f"Reported new records: {actual_new_count:,}\n"
                  f"Reported updated records: {actual_update_count:,}\n"
                  f"Total records after: {total_records_after:,}\n"
                  f"Actual new records (delta): {actual_records_added:,}\n"
                  f"Average reported updates per batch: {actual_update_count/(line_count/batch_size):,.1f}\n"
                  f"Average reported new per batch: {actual_new_count/(line_count/batch_size):,.1f}\n"
                  f"Records processed vs total: {line_count:,} vs {total_records_after:,}")
        debug_log(summary, success=True)
        
        # Return actual counts based on database changes
        return actual_records_added, actual_update_count

    except Exception as e:
        debug_log(f"Fatal error during data load: {e}", 
                 level='error', success=False, error=True)
        raise


def process_batch(batch):
    """
    Processes a batch of data.
    """
    try:
        new_records = []
        update_records = []
        note_id_map = {note.note_id: note for note in batch}

        # Query existing records for the batch
        existing_notes = Note.objects.filter(note_id__in=note_id_map.keys())
        debug_log(f"Found {existing_notes.count()} existing notes to update")
        
        # Separate records into updates and new inserts
        for existing_note in existing_notes:
            updated_note = note_id_map.pop(existing_note.note_id)  # Remove from map
            
            # Only update if fields have changed
            if (existing_note.summary != updated_note.summary or
                existing_note.classification != updated_note.classification or
                existing_note.believable != updated_note.believable or
                existing_note.harmful != updated_note.harmful or
                existing_note.validation_difficulty != updated_note.validation_difficulty):
                
                # Update fields
                existing_note.note_author_participant_id = updated_note.note_author_participant_id
                existing_note.created_at_millis = updated_note.created_at_millis
                existing_note.tweet_id = updated_note.tweet_id
                existing_note.classification = updated_note.classification
                existing_note.believable = updated_note.believable
                existing_note.harmful = updated_note.harmful
                existing_note.validation_difficulty = updated_note.validation_difficulty
                existing_note.misleading_other = updated_note.misleading_other
                existing_note.misleading_factual_error = updated_note.misleading_factual_error
                existing_note.misleading_manipulated_media = updated_note.misleading_manipulated_media
                existing_note.misleading_outdated_information = updated_note.misleading_outdated_information
                existing_note.misleading_missing_important_context = updated_note.misleading_missing_important_context
                existing_note.misleading_unverified_claim_as_fact = updated_note.misleading_unverified_claim_as_fact
                existing_note.misleading_satire = updated_note.misleading_satire
                existing_note.not_misleading_other = updated_note.not_misleading_other
                existing_note.not_misleading_factually_correct = updated_note.not_misleading_factually_correct
                existing_note.not_misleading_outdated_but_not_when_written = updated_note.not_misleading_outdated_but_not_when_written
                existing_note.not_misleading_clearly_satire = updated_note.not_misleading_clearly_satire
                existing_note.not_misleading_personal_opinion = updated_note.not_misleading_personal_opinion
                existing_note.trustworthy_sources = updated_note.trustworthy_sources
                existing_note.summary = updated_note.summary
                existing_note.is_media_note = updated_note.is_media_note
                update_records.append(existing_note)

        # Remaining records in note_id_map are new inserts
        new_records = list(note_id_map.values())

        debug_log(f"Of {existing_notes.count()} existing notes, {len(update_records)} actually need updates")
        
        with transaction.atomic():
            if update_records:
                debug_log(f"Updating {len(update_records)} changed records...")
                Note.objects.bulk_update(
                    update_records,
                    fields=[
                        'note_author_participant_id',
                        'created_at_millis',
                        'tweet_id',
                        'classification',
                        'believable',
                        'harmful',
                        'validation_difficulty',
                        'misleading_other',
                        'misleading_factual_error',
                        'misleading_manipulated_media',
                        'misleading_outdated_information',
                        'misleading_missing_important_context',
                        'misleading_unverified_claim_as_fact',
                        'misleading_satire',
                        'not_misleading_other',
                        'not_misleading_factually_correct',
                        'not_misleading_outdated_but_not_when_written',
                        'not_misleading_clearly_satire',
                        'not_misleading_personal_opinion',
                        'trustworthy_sources',
                        'summary',
                        'is_media_note',
                    ]
                )

            if new_records:
                debug_log(f"Creating {len(new_records)} new records...")
                Note.objects.bulk_create(new_records, ignore_conflicts=True)

        return len(new_records), len(update_records)
    except Exception as e:
        debug_log(f"Error processing batch: {e}", level='error', success=False, error=True)
        raise

def check_for_new_data(url):
    """
    Checks if there is new data available at the given URL.
    Returns (is_new_data, url) tuple.
    """
    logger.info(f"Checking for new data at {url}")
    try:
        response = requests.head(url)
        if response.status_code == 200:
            logger.info("New data is available")
            return True, url
        logger.info("No new data found")
        return False, url
    except Exception as e:
        logger.error(f"Error checking for new data: {e}")
        return False, url
