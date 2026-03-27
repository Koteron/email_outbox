from django.core.management.base import BaseCommand, CommandError

from outbox.services.import_service import import_outbox_records


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--file_path",
            type=str,
        )

        parser.add_argument(
            "--batch_size",
            type=int,
            default=500,
        )

    def handle(self, *args, **options):
        file_path = options["file_path"]
        batch_size = options["batch_size"]

        self.stdout.write(f"Starting import from {file_path}...")

        try:
            result = import_outbox_records(
                file_path=file_path,
                batch_size=batch_size,
            )
        except Exception as e:
            raise CommandError(f"Import failed: {e}")

        self.stdout.write(self.style.SUCCESS("Import completed"))
        self.stdout.write(f"Total rows: {result['total']}")
        self.stdout.write(f"Created: {result['created']}")
        self.stdout.write(f"Skipped: {result['skipped']}")
        self.stdout.write(f"Failed: {result['failed']}")
