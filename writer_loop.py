# writer_loop.py
import logging
import os
import time
from multiprocessing import Queue
from typing import Any

from buffer_writer import BufferWriter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

STOP_SENTINEL = {"__type__": "STOP"}

def writer_main(queue: Queue) -> None:
    gcs_bucket = os.environ.get("GCS_BUCKET")
    gcs_project = os.environ.get("GCS_PROJECT")

    if not gcs_bucket:
        raise RuntimeError("GCS_BUCKET not set")

    writer = BufferWriter(
        gcs_bucket=gcs_bucket,
        gcs_project=gcs_project,
        buffer_size=int(os.environ.get("BUFFER_SIZE", "200000")),
        buffer_time=int(os.environ.get("BUFFER_TIME", "180")),
    )

    logger.info("Writer process started")

    while True:
        try:
            try:
                item: dict[str, Any] = queue.get(timeout=1.0)
            except Exception:
                item = None

            if item is not None:
                if isinstance(item, dict) and item.get("__type__") == "STOP":
                    logger.info("Writer got STOP signal")
                    break
                writer.add(item)

            if writer.should_flush():
                writer.flush()

        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("Writer loop error")
            time.sleep(1)

    try:
        writer.flush()
    except Exception:
        logger.exception("Writer final flush error")

    logger.info("Writer process exiting")
