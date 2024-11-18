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
    
    def to_dict(self):
        return {
            'note_id': self.note_id,
            'note_author_participant_id': self.note_author_participant_id,
            'created_at_millis': self.created_at_millis,
            'tweet_id': self.tweet_id,
            'classification': self.classification,
            'believable': self.believable,
            'harmful': self.harmful,
            'validation_difficulty': self.validation_difficulty,
            'misleading_other': self.misleading_other,
            'misleading_factual_error': self.misleading_factual_error,
            'misleading_manipulated_media': self.misleading_manipulated_media,
            'misleading_outdated_information': self.misleading_outdated_information,
            'misleading_missing_important_context': self.misleading_missing_important_context,
            'misleading_unverified_claim_as_fact': self.misleading_unverified_claim_as_fact,
            'misleading_satire': self.misleading_satire,
            'not_misleading_other': self.not_misleading_other,
            'not_misleading_factually_correct': self.not_misleading_factually_correct,
            'not_misleading_outdated_but_not_when_written': self.not_misleading_outdated_but_not_when_written,
            'not_misleading_clearly_satire': self.not_misleading_clearly_satire,
            'not_misleading_personal_opinion': self.not_misleading_personal_opinion,
            'trustworthy_sources': self.trustworthy_sources,
            'summary': self.summary,
            'is_media_note': self.is_media_note,
        }
    
    
