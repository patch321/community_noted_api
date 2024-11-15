import gzip
import os
import requests
import django.db.transaction as transaction

def download_and_decompress_file(url, local_file_path):
    """
    Downloads and decompresses a GZipped file.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_file_path, 'wb') as local_file:
        with gzip.GzipFile(fileobj=response.raw) as gzip_file:
            local_file.write(gzip_file.read())

from .models import Note

def parse_note_line(line):
    """
    Parses a single line of TSV into a Note instance.
    """
    fields = line.split('\t')
    if len(fields) != 23:  # Adjust based on your TSV structure
        return None

    try:
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
            summary=fields[21],
            is_media_note=fields[22] == "1",
        )
    except Exception:
        return None


from .models import Note

def load_data_into_database(file_path):
    """
    Reads a TSV file and processes the data in batches.
    Updates existing rows and inserts new rows as needed.
    """
    total_new_records = 0
    total_updated_records = 0
    batch_size = 10000
    batch = []

    with open(file_path, 'r') as file:
        next(file)  # Skip the header
        for line in file:
            note_data = parse_note_line(line.strip())
            if note_data:
                batch.append(note_data)

                if len(batch) >= batch_size:
                    new_records, updated_records = process_batch(batch)
                    total_new_records += new_records
                    total_updated_records += updated_records
                    batch = []  # Clear the batch

        # Process any remaining records
        if batch:
            new_records, updated_records = process_batch(batch)
            total_new_records += new_records
            total_updated_records += updated_records

    return total_new_records, total_updated_records


def process_batch(batch):
    """
    Processes a batch of data.
    Updates existing rows and inserts new rows in bulk.
    """
    new_records = []
    update_records = []
    note_id_map = {note.note_id: note for note in batch}

    # Query existing records for the batch
    existing_notes = Note.objects.filter(note_id__in=note_id_map.keys())

    # Separate records into updates and new inserts
    for existing_note in existing_notes:
        updated_note = note_id_map.pop(existing_note.note_id)  # Remove from map
        # Update fields on the existing note
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

    # Perform batch updates and inserts
    with transaction.atomic():
        if update_records:
            Note.objects.bulk_update(
                update_records,
                [
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
                ],
            )

        if new_records:
            Note.objects.bulk_create(new_records, ignore_conflicts=True)

    return len(new_records), len(update_records)
