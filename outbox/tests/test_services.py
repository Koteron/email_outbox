import tempfile
from unittest import mock
from django.db import transaction
from django.test import TestCase, TransactionTestCase
from django.db import IntegrityError
from openpyxl import Workbook

from outbox.models import OutboxRecord
from outbox.services.import_service import import_outbox_records, _flush_batch
from outbox.services.email_service import send_email

def create_xlsx_file(rows):
    wb = Workbook()
    ws = wb.active

    headers = ["external_id", "user_id", "email", "subject", "message"]
    ws.append(headers)

    for row in rows:
        ws.append(row)

    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    wb.save(tmp.name)
    return tmp.name

class ImportServiceTests(TransactionTestCase):

    def test_import_valid_rows(self):
        file_path = create_xlsx_file([
            ["id1", 1, "test1@example.com", "subj", "msg"],
            ["id2", 2, "test2@example.com", "subj", "msg"],
        ])

        result = import_outbox_records(file_path, batch_size=2)

        self.assertEqual(result["total"], 2)
        self.assertEqual(result["created"], 2)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["skipped"], 0)

        self.assertEqual(OutboxRecord.objects.count(), 2)

    def test_invalid_email_is_skipped(self):
        file_path = create_xlsx_file([
            ["id1", 1, "invalid-email", "subj", "msg"],
            ["id2", 2, "valid@example.com", "subj", "msg"],
        ])

        result = import_outbox_records(file_path, batch_size=2)

        self.assertEqual(result["total"], 2)
        self.assertEqual(result["created"], 1)
        self.assertEqual(result["failed"], 1)

        self.assertEqual(OutboxRecord.objects.count(), 1)

    def test_duplicates_are_skipped(self):
        OutboxRecord.objects.create(
            external_id="id1",
            user_id=1,
            email="existing@example.com",
            subject="s",
            message="m",
        )

        file_path = create_xlsx_file([
            ["id1", 1, "test1@example.com", "subj", "msg"],
            ["id2", 2, "test2@example.com", "subj", "msg"],
        ])

        result = import_outbox_records(file_path, batch_size=2)

        self.assertEqual(result["created"], 1)
        self.assertEqual(result["skipped"], 1)

        self.assertEqual(OutboxRecord.objects.count(), 2)

    def test_batching_triggers_flush(self):
        rows = [
            ["id1", 1, "a@example.com", "s", "m"],
            ["id2", 2, "b@example.com", "s", "m"],
            ["id3", 3, "c@example.com", "s", "m"],
        ]

        file_path = create_xlsx_file(rows)

        result = import_outbox_records(file_path, batch_size=2)

        self.assertEqual(result["created"], 3)
        self.assertEqual(OutboxRecord.objects.count(), 3)


class FlushBatchTests(TestCase):

    def test_flush_batch_creates_records(self):
        batch = [
            OutboxRecord(
                external_id="id1",
                user_id=1,
                email="a@example.com",
                subject="s",
                message="m",
            ),
            OutboxRecord(
                external_id="id2",
                user_id=2,
                email="b@example.com",
                subject="s",
                message="m",
            ),
        ]

        created, skipped = _flush_batch(batch)

        self.assertEqual(created, 2)
        self.assertEqual(skipped, 0)
        self.assertEqual(OutboxRecord.objects.count(), 2)

class EmailServiceTests(TestCase):

    @mock.patch("outbox.services.email_service.time.sleep", return_value=None)
    def test_send_email_logs(self, mock_sleep):
        record = OutboxRecord(
            external_id="id1",
            user_id=1,
            email="test@example.com",
            subject="s",
            message="m",
        )

        with self.assertLogs("outbox.services.email_service", level="INFO") as cm:
            send_email(record)

        self.assertTrue(any("Send EMAIL" in msg for msg in cm.output))

class WorkerTests(TransactionTestCase):

    @mock.patch("outbox.services.worker_service.send_email")
    def test_worker_processes_records(self, mock_send_email):
        for i in range(3):
            OutboxRecord.objects.create(
                external_id=f"id{i}",
                user_id=i,
                email=f"test{i}@example.com",
                subject="s",
                message="m",
            )

        from threading import Event
        from outbox.services.worker_service import worker_loop

        stop_event = Event()

        def stop_after_delay(*args):
            stop_event.set()

        with mock.patch("time.sleep", side_effect=stop_after_delay):
            worker_loop(worker_id=1, stop_event=stop_event, batch_size=10)

        self.assertEqual(mock_send_email.call_count, 3)

        for record in OutboxRecord.objects.all():
            self.assertEqual(record.status, OutboxRecord.Status.SENT)
            self.assertIsNotNone(record.sent_at)

