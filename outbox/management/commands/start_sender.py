import multiprocessing
import signal
from django.core.management.base import BaseCommand

from outbox.services.worker_service import worker_loop


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("--scale", type=int, default=1)
        parser.add_argument("--batch_size", type=int, default=10)

    def handle(self, *args, **options):
        scale = options["scale"]
        batch_size = options["scale"]

        stop_event = multiprocessing.Event()
        processes = []

        def shutdown_handler(signum, frame):
            self.stdout.write("Shutdown signal received...")
            stop_event.set()

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        for i in range(scale):
            p = multiprocessing.Process(target=worker_loop, args=(i, stop_event, batch_size))
            p.start()
            processes.append(p)

        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            stop_event.set()
            for p in processes:
                p.join()