from django.db import models
from django.utils import timezone


class OutboxRecord(models.Model):
    class Status(models.TextChoices): 
        PENDING = 'PENDING'
        SENT = 'SENT'

    external_id = models.CharField(
        max_length=255,
        unique=True,
        db_index=True
    )
    user_id = models.IntegerField(db_index=True)
    email = models.EmailField()
    subject = models.CharField(max_length=255)
    message = models.TextField()
    status = models.CharField(
        max_length=10,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    sent_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Outbox Record'
        verbose_name_plural = 'Outbox Records'
        indexes = [
            models.Index(fields=['status', '-created_at']),
        ]

    def __str__(self):
        return f'Outbox Record: {self.external_id} to {self.email} ({self.status})'
    
