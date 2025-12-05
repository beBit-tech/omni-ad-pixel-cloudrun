import io
import logging
import signal
import time
import uuid
from datetime import datetime
from typing import Any, Optional

import gevent
import polars as pl
from gevent.event import Event
from gevent.lock import BoundedSemaphore
from google.cloud import storage

logger = logging.getLogger(__name__)


class BufferWriter:
    def __init__(
        self,
        buffer_size: int = 10000,
        buffer_time: int = 60,
        gcs_bucket: Optional[str] = None,
        gcs_project: Optional[str] = None,
    ):
        self.buffer_size = buffer_size
        self.buffer_time = buffer_time
        self.gcs_bucket = gcs_bucket
        self.gcs_project = gcs_project

        self.buffer: list[dict[str, Any]] = []
        self.buffer_lock = BoundedSemaphore()

        self.total_buffered = 0
        self.total_written = 0
        self.last_write = None

        self.stop_event = Event()
        self.write_greenlet = None
        self.buffer_start_time = None
        self.is_flushing = False

        self.gcs_client = None
        self.bucket = None

        if self.gcs_bucket:
            try:
                self.gcs_client = storage.Client(project=self.gcs_project)
                self.bucket = self.gcs_client.bucket(self.gcs_bucket)
                logger.info(f"GCS initialized: bucket={self.gcs_bucket}")
            except Exception as e:
                logger.error("Failed to initialize GCS client: %s", e)

        logger.info(
            "BufferWriter initialized: buffer_size=%d buffer_time=%ds bucket=%s",
            buffer_size,
            buffer_time,
            gcs_bucket or "None",
        )

    def add(self, data: dict[str, Any]) -> None:
        with self.buffer_lock:
            if not self.buffer:
                self.buffer_start_time = time.time()

            self.buffer.append(data)
            self.total_buffered += 1

    def _flush_buffer(self):
        with self.buffer_lock:
            if not self.buffer or self.is_flushing:
                return

            self.is_flushing = True
            data_to_write = self.buffer.copy()
            self.buffer.clear()
            self.buffer_start_time = None

        try:
            self._write_to_parquet(data_to_write)

            with self.buffer_lock:
                self.total_written += len(data_to_write)
                self.last_write = datetime.utcnow().isoformat()

            logger.info("Successfully wrote %d records to Parquet", len(data_to_write))
        except Exception as e:
            logger.error("Error flushing buffer: %s", e, exc_info=True)
        finally:
            with self.buffer_lock:
                self.is_flushing = False

    def _write_to_parquet(self, data: list[dict[str, Any]], max_retries: int = 3):
        if not data:
            return

        for attempt in range(max_retries):
            try:
                self._do_write_parquet(data)
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 3**attempt
                    logger.warning(
                        "Write failed (attempt %d/%d), retrying in %ds: %s",
                        attempt + 1,
                        max_retries,
                        wait_time,
                        e,
                    )
                    gevent.sleep(wait_time)
                else:
                    logger.error("Write failed after %d attempts, data lost: %s", max_retries, e)
                    raise

    def _do_write_parquet(self, data: list[dict[str, Any]]):
        df = pl.DataFrame(data)

        if "timestamp" in df.columns:
            df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False))
        if "date" in df.columns:
            df = df.with_columns(pl.col("date").str.strptime(pl.Date, strict=False))
        if "date" in df.columns:
            dates = df["date"].unique().to_list()
        else:
            dates = [None]

        for date_val in dates:
            if date_val is None:
                part = df
                prefix = ""
            else:
                part = df.filter(pl.col("date") == date_val)
                prefix = f"date={date_val}/"

            buf = io.BytesIO()
            part.write_parquet(buf)
            parquet_bytes = buf.getvalue()

            blob_name = f"{prefix}{uuid.uuid4().hex}.parquet"
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")

            logger.info(
                "GCS uploaded: gs://%s/%s rows=%d",
                self.gcs_bucket,
                blob_name,
                len(part),
            )

    def _background_flush(self):
        logger.info("Background flush greenlet started")

        while not self.stop_event.is_set():
            try:
                gevent.sleep(1)

                should_flush = False
                reason = ""

                with self.buffer_lock:
                    if self.buffer and self.buffer_start_time:
                        buffer_size = len(self.buffer)
                        time_elapsed = time.time() - self.buffer_start_time

                        if buffer_size >= self.buffer_size:
                            should_flush = True
                            reason = f"Buffer size {buffer_size} reached"
                        elif time_elapsed >= self.buffer_time:
                            should_flush = True
                            reason = f"Buffer time {time_elapsed:.1f}s reached"

                if should_flush:
                    logger.info("%s, triggering flush", reason)
                    self._flush_buffer()

            except Exception as e:
                logger.error("Error in background flush: %s", e, exc_info=True)

        logger.info("Background flush greenlet stopped")

    def _signal_handler(self, signum, frame):
        logger.warning(f"Received signal {signum}, initiating shutdown...")
        self.stop()

    def start(self):
        if self.write_greenlet is None or self.write_greenlet.dead:
            self.stop_event.clear()
            self.write_greenlet = gevent.spawn(self._background_flush)

            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)

            logger.info("BufferWriter started")

    def stop(self):
        logger.info("Stopping BufferWriter...")

        self.stop_event.set()

        if self.write_greenlet and not self.write_greenlet.dead:
            self.write_greenlet.join(timeout=5)

        with self.buffer_lock:
            has_data = len(self.buffer) > 0

        if has_data:
            logger.info("Flushing remaining data...")
            self._flush_buffer()

        logger.info("BufferWriter stopped")
