import io
import logging
import time
import uuid
from datetime import datetime
from threading import Event, Lock, Thread
from typing import Any, Optional

import polars as pl
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
        self.buffer_lock = Lock()

        self.total_buffered = 0
        self.total_written = 0
        self.last_write = None

        self.stop_event = Event()
        self.write_thread = None
        self.last_flush_time = time.time()

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
            self.buffer.append(data)
            self.total_buffered += 1

            if len(self.buffer) >= self.buffer_size:
                logger.info("Buffer size %d reached, flushing...", len(self.buffer))
                self._flush_buffer()

    def _flush_buffer(self):
        if not self.buffer:
            return
        try:
            data_to_write = self.buffer.copy()
            self.buffer.clear()
            self._write_to_parquet(data_to_write)

            self.total_written += len(data_to_write)
            self.last_write = datetime.utcnow().isoformat()
            self.last_flush_time = time.time()

            logger.info("Successfully wrote %d records to Parquet", len(data_to_write))
        except Exception as e:
            logger.error("Error flushing buffer: %s", e, exc_info=True)

    def _write_to_parquet(self, data: list[dict[str, Any]]):
        if not data:
            return

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
        logger.info("Background flush thread started")

        while not self.stop_event.is_set():
            try:
                time.sleep(1)
                time_elapsed = time.time() - self.last_flush_time
                with self.buffer_lock:
                    if self.buffer and time_elapsed >= self.buffer_time:
                        logger.info("Buffer time reached %.1fs, triggering flush", time_elapsed)
                        self._flush_buffer()

            except Exception as e:
                logger.error("Error in background flush: %s", e, exc_info=True)

        logger.info("Background flush thread stopped")

    def start(self):
        if self.write_thread is None or not self.write_thread.is_alive():
            self.stop_event.clear()
            self.write_thread = Thread(target=self._background_flush, daemon=True)
            self.write_thread.start()
            logger.info("BufferWriter started")

    def stop(self):
        logger.info("Stopping BufferWriter...")

        self.stop_event.set()

        if self.write_thread and self.write_thread.is_alive():
            self.write_thread.join(timeout=5)

        with self.buffer_lock:
            if self.buffer:
                logger.info("Flushing remaining %d rows...", len(self.buffer))
                self._flush_buffer()

        logger.info("BufferWriter stopped")
