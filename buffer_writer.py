import logging
import os
import io
import time
import uuid
from datetime import datetime
from typing import Any, List

import polars as pl
from google.cloud import storage

logger = logging.getLogger(__name__)

class BufferWriter:
    def __init__(
        self,
        gcs_bucket: str,
        gcs_project: str | None = None,
        buffer_size: int = 10000,
        buffer_time: int = 60,
    ) -> None:
        self.gcs_bucket = gcs_bucket
        self.gcs_project = gcs_project
        self.buffer_size = buffer_size
        self.buffer_time = buffer_time

        self.buffer: List[dict[str, Any]] = []
        self.last_flush_time = time.time()

        self.client = storage.Client(project=gcs_project) if gcs_project else storage.Client()
        self.bucket = self.client.bucket(gcs_bucket)

        logger.info(
            "BufferWriter init: bucket=%s project=%s buffer_size=%d buffer_time=%ds",
            gcs_bucket,
            gcs_project,
            buffer_size,
            buffer_time,
        )

    def add(self, record: dict[str, Any]) -> None:
        self.buffer.append(record)

    def should_flush(self) -> bool:
        if not self.buffer:
            return False
        if len(self.buffer) >= self.buffer_size:
            return True
        if time.time() - self.last_flush_time >= self.buffer_time:
            return True
        return False

    def flush(self) -> None:
        if not self.buffer:
            return
        try:
            data = self.buffer
            self.buffer = []

            df = pl.DataFrame(data)

  
            if "timestamp" in df.columns:
                df = df.with_columns(
                    pl.col("timestamp").str.strptime(pl.Datetime, strict=False)
                )
            if "date" in df.columns:
                df = df.with_columns(
                    pl.col("date").str.strptime(pl.Date, strict=False)
                )

            if "date" in df.columns:
                dates = df["date"].unique().to_list()
 

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
                    "GCS write: gs://%s/%s rows=%d",
                    self.gcs_bucket,
                    blob_name,
                    len(part),
                )
            self.last_flush_time = time.time()
        except Exception:
            logger.exception("BufferWriter flush error")
