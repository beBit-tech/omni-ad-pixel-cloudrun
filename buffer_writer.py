import logging
import os
import time
from datetime import datetime
from threading import Event, Lock, Thread
from typing import Any, Dict, List

import polars as pl
from deltalake import write_deltalake
from google.cloud import storage

logger = logging.getLogger(__name__)

class BufferWriter:
    def __init__(self, gcs_bucket: str, gcs_project: str,
                 buffer_size: int = 10000, buffer_time: int = 60):

        self.gcs_bucket = gcs_bucket
        self.gcs_project = gcs_project
        self.buffer_size = buffer_size
        self.buffer_time = buffer_time

        self.buffer: list[dict[str, Any]] = []
        self.buffer_lock = Lock()

        self.total_buffered = 0
        self.total_written = 0
        self.last_write = None

        self.stop_event = Event()
        self.write_thread = None
        self.last_flush_time = time.time()

        self._storage_client = None

        logger.info(f"BufferWriter initialized: buffer_size={buffer_size}, "
                   f"buffer_time={buffer_time}s, bucket={gcs_bucket}")

    @property
    def storage_client(self):
        if self._storage_client is None and self.gcs_project:
            self._storage_client = storage.Client(project=self.gcs_project)
        return self._storage_client

    def add(self, data: dict[str, Any]):
        with self.buffer_lock:
            self.buffer.append(data)
            self.total_buffered += 1

            if len(self.buffer) >= self.buffer_size:
                logger.info(f"Buffer size reached {len(self.buffer)}, triggering flush")
                self._flush_buffer()

    def _flush_buffer(self):
        if not self.buffer:
            return

        is_local = not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') and not self.gcs_bucket

        if not self.gcs_bucket and not is_local:
            logger.warning("GCS_BUCKET not configured and not in local mode, discarding buffer")
            self.buffer.clear()
            return

        try:
            data_to_write = self.buffer.copy()
            self.buffer.clear()
            self._write_to_delta_lake(data_to_write)

            self.total_written += len(data_to_write)
            self.last_write = datetime.utcnow().isoformat()
            self.last_flush_time = time.time()

            logger.info(f"Successfully wrote {len(data_to_write)} records to Delta Lake")

        except Exception as e:
            logger.error(f"Error flushing buffer: {e}", exc_info=True)

    def _write_to_delta_lake(self, data: list[dict[str, Any]]):
        if not data:
            return

        cleaned_data = []
        for record in data:
            clean_record = {}
            for key, value in record.items():
                clean_record[key] = "" if value is None else value
            cleaned_data.append(clean_record)

        df = pl.DataFrame(cleaned_data)

        df = df.with_columns([
            pl.col('timestamp').str.strptime(pl.Datetime, '%Y-%m-%dT%H:%M:%S%.f%z'),
            pl.col('date').str.strptime(pl.Date, '%Y-%m-%d')
        ])

        arrow_table = df.to_arrow()

        if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') and not self.gcs_bucket:
            local_path = "./local_data/delta_lake/"
            os.makedirs(local_path, exist_ok=True)
            delta_path = local_path
            storage_options = None
            logger.info(f"Writing to local Delta Lake: {delta_path}")
        else:
            delta_path = f"gs://{self.gcs_bucket}/pixel-data"
            storage_options = {}
            if self.gcs_project:
                storage_options['project_id'] = self.gcs_project
            logger.info(f"Writing to GCS Delta Lake: {delta_path}")

        write_deltalake(
            delta_path,
            arrow_table,
            storage_options=storage_options,
            mode='append',
            partition_by=['date'],
            schema_mode='merge'
        )

        logger.info(f"Write completed: {len(data)} records")

    def _background_flush(self):
        """背景執行緒:定期檢查並寫入緩衝區"""
        logger.info("Background flush thread started")

        while not self.stop_event.is_set():
            try:
                time.sleep(1)

                time_elapsed = time.time() - self.last_flush_time

                with self.buffer_lock:
                    if self.buffer and time_elapsed >= self.buffer_time:
                        logger.info(f"Buffer time reached {time_elapsed:.1f}s, triggering flush")
                        self._flush_buffer()

            except Exception as e:
                logger.error(f"Error in background flush: {e}", exc_info=True)

        logger.info("Background flush thread stopped")

    def start(self):
        """啟動背景寫入執行緒"""
        if self.write_thread is None or not self.write_thread.is_alive():
            self.stop_event.clear()
            self.write_thread = Thread(target=self._background_flush, daemon=True)
            self.write_thread.start()
            logger.info("BufferWriter started")

    def stop(self):
        """停止背景執行緒並寫入剩餘資料"""
        logger.info("Stopping BufferWriter...")

        self.stop_event.set()

        if self.write_thread and self.write_thread.is_alive():
            self.write_thread.join(timeout=5)

        with self.buffer_lock:
            if self.buffer:
                logger.info(f"Flushing remaining {len(self.buffer)} records")
                self._flush_buffer()

        logger.info("BufferWriter stopped")
