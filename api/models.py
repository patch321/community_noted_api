from django.db import models

from django.db import models

class Note(models.Model):
    note_id = models.BigIntegerField(unique=True, primary_key=True)  # Maps to [Key] attribute
    note_author_participant_id = models.CharField(max_length=255, null=True)  # Nullable string
    created_at_millis = models.BigIntegerField()
    tweet_id = models.BigIntegerField()
    classification = models.CharField(max_length=255, null=True)
    believable = models.TextField(null=True)
    harmful = models.TextField(null=True)
    validation_difficulty = models.TextField(null=True)
    misleading_other = models.BooleanField(default=False)
    misleading_factual_error = models.BooleanField(default=False)
    misleading_manipulated_media = models.BooleanField(default=False)
    misleading_outdated_information = models.BooleanField(default=False)
    misleading_missing_important_context = models.BooleanField(default=False)
    misleading_unverified_claim_as_fact = models.BooleanField(default=False)
    misleading_satire = models.BooleanField(default=False)
    not_misleading_other = models.BooleanField(default=False)
    not_misleading_factually_correct = models.BooleanField(default=False)
    not_misleading_outdated_but_not_when_written = models.BooleanField(default=False)
    not_misleading_clearly_satire = models.BooleanField(default=False)
    not_misleading_personal_opinion = models.BooleanField(default=False)
    trustworthy_sources = models.BooleanField(default=False)
    summary = models.TextField(null=True)
    is_media_note = models.BooleanField(default=False)

    class Meta:
        db_table = 'notes'

    def __str__(self):
        return f"Note {self.note_id}"
