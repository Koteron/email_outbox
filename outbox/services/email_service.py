import time
import random
import logging

logger = logging.getLogger(__name__)


def send_email(record):
    delay = random.randint(5, 20)
    time.sleep(delay)

    logger.info(f"Send EMAIL to {record.email}")