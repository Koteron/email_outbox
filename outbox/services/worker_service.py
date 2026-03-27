import logging
import time
import os
import django
from django.db import transaction
from django.utils import timezone
from django.db import close_old_connections

from outbox.services.email_service import send_email


SLEEP_TIME = 5
logger = logging.getLogger(__name__)


def worker_loop(worker_id: int, stop_event, batch_size: int):
    # initializing Django before importing the model
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "project.settings") 
    django.setup()
    from outbox.models import OutboxRecord

    def process_pending(batch_size=10):
        processed = 0

        while True:
            with transaction.atomic():
                records = (
                    OutboxRecord.objects
                    .select_for_update(skip_locked=True)
                    .filter(status=OutboxRecord.Status.PENDING)
                    .order_by("created_at")[:batch_size]
                )

                records = list(records)

                if not records:
                    break

                for record in records:
                    try:
                        send_email(record)
                        record.status = OutboxRecord.Status.SENT
                        record.sent_at = timezone.now()
                        record.save(update_fields=["status", "sent_at"])

                    except Exception as e:
                        record.status = OutboxRecord.Status.FAILED
                        record.error_message = str(e)
                        record.save(update_fields=["status", "error_message"])

                    processed += 1

        return processed

    logger.info(f"Worker {worker_id} started")

    try:
        while not stop_event.is_set():
            close_old_connections()

            processed = process_pending()

            if processed == 0:
                for _ in range(5):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
            else:
                logger.info(f"[Worker {worker_id}] processed {processed}")

    except KeyboardInterrupt:
        logger.info(f"Worker {worker_id} interrupted")

    finally:
        logger.info(f"Worker {worker_id} exiting gracefully")
