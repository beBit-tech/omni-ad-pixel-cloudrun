import logging
import os
import shutil
import time
from datetime import datetime
from threading import Event, Lock, Thread
from typing import Any, Dict, List

import polars as pl

logger = logging.getLogger(__name__)

class BufferWriter:
    def __init__(self, buffer_size: int = 10000, buffer_time: int = 60, parquet_dir: str = "./local_data/parquet_data"):
        self.buffer_size = buffer_size
        self.buffer_time = buffer_time
        self.parquet_dir = parquet_dir

        self.buffer = []  # type: list[dict[str, Any]]
        self.buffer_lock = Lock()

        self.total_buffered = 0
        # 移除重複 import
        self.total_written = 0
        self.last_write = None
          # 移除未用的 shutil

        self.stop_event = Event()
        self.write_thread = None
        self.last_flush_time = time.time()

        os.makedirs(self.parquet_dir, exist_ok=True)
        logger.info("BufferWriter initialized: buffer_size=%d, buffer_time=%ds, parquet_dir=%s", buffer_size, buffer_time, parquet_dir)

    # 移除 GCS 相關 client

    def add(self, data: dict[str, Any]) -> None:
        with self.buffer_lock:
            self.buffer.append(data)
            self.total_buffered += 1

            if len(self.buffer) >= self.buffer_size:
                logger.info("Buffer size reached %d, triggering flush", len(self.buffer))
                self._flush_buffer()

    def _flush_buffer(self) -> None:
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

    def _write_to_parquet(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        # schema merge: 讀取現有 schema，補齊缺漏欄位
        cleaned_data = []
        for record in data:
            clean_record = {}
            for key, value in record.items():
                clean_record[key] = "" if value is None else value
            cleaned_data.append(clean_record)

        df = pl.DataFrame(cleaned_data)

        # schema merge: 若有現有 parquet，補齊缺漏欄位
        partition_col = 'date'
        if partition_col in df.columns:
            df = df.with_columns([
                pl.col('timestamp').str.strptime(pl.Datetime, '%Y-%m-%dT%H:%M:%S%.f%z', strict=False),
                pl.col('date').str.strptime(pl.Date, '%Y-%m-%d', strict=False)
            ])
            unique_dates = df[partition_col].unique().to_list()
        else:
            unique_dates = [None]

        for date_val in unique_dates:
            if date_val is not None:
                partition_path = os.path.join(self.parquet_dir, f"date={date_val}")
            else:
                partition_path = self.parquet_dir
            os.makedirs(partition_path, exist_ok=True)
            parquet_file = os.path.join(partition_path, f"data_{int(time.time())}.parquet")

            # 只寫入該分區的資料
            if date_val is not None:
                part_df = df.filter(pl.col(partition_col) == date_val)
            else:
                part_df = df

            # schema merge: 若有舊檔案，合併 schema
            old_files = [f for f in os.listdir(partition_path) if f.endswith('.parquet')]
            if old_files:
                try:
                    old_df = pl.read_parquet(os.path.join(partition_path, old_files[-1]))
                    merged_df = old_df.vstack(part_df, rechunk=True).fill_null("")
                    merged_df.write_parquet(parquet_file)
                except Exception as e:
                    logger.warning("Schema merge failed, fallback to new file: %s", e)
                    part_df.write_parquet(parquet_file)
            else:
                part_df.write_parquet(parquet_file)

            logger.info("Write completed: %d records to %s", len(part_df), parquet_file)

    def _background_flush(self) -> None:
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

    def start(self) -> None:
        if self.write_thread is None or not self.write_thread.is_alive():
            self.stop_event.clear()
            self.write_thread = Thread(target=self._background_flush, daemon=True)
            self.write_thread.start()
            logger.info("BufferWriter started")

    def stop(self) -> None:
        logger.info("Stopping BufferWriter...")

        self.stop_event.set()

        if self.write_thread and self.write_thread.is_alive():
            self.write_thread.join(timeout=5)

        with self.buffer_lock:
            if self.buffer:
                logger.info("Flushing remaining %d records", len(self.buffer))
                self._flush_buffer()

        logger.info("BufferWriter stopped")
