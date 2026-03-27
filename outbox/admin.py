from django.contrib import admin
from outbox.models import OutboxRecord


@admin.register(OutboxRecord)
class NewsletterRecordAdmin(admin.ModelAdmin):

    list_display = (
        'external_id',
        'user_id',
        'email',
        'status',
        'created_at',
        'sent_at',
    )
    list_filter = ('status', 'created_at', 'sent_at')
    search_fields = ('external_id', 'email', 'user_id')
    readonly_fields = ('created_at', 'sent_at')
    fieldsets = (
        ('Record Information', {
            'fields': ('external_id', 'user_id')
        }),
        ('Email Details', {
            'fields': ('email', 'subject', 'message')
        }),
        ('Status & Tracking', {
            'fields': ('status', 'created_at', 'sent_at')
        }),
    )
