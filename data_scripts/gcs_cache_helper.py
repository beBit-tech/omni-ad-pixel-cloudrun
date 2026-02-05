#!/usr/bin/env python3
"""
GCS 本地快取幫助模組

功能：
1. 提供本地快取目錄管理
2. 從 GCS 下載 Parquet 檔案並保存到本地
3. 優先從本地讀取，減少 GCS 請求和費用

使用方法：
    from gcs_cache_helper import download_parquet_with_cache

    # 下載並快取檔案
    local_path = download_parquet_with_cache(
        bucket_name="daily-pixel-data-consolidated",
        file_path="date=2024-12-22.parquet",
        project_name="bebit-tech-website",
        force_download=False  # True 時強制重新下載
    )

    # 讀取 parquet
    import polars as pl
    df = pl.read_parquet(local_path)
"""

import logging
import os
from pathlib import Path

from google.cloud import storage


logger = logging.getLogger(__name__)


# 本地快取根目錄
CACHE_ROOT_DIR = "local_cache"


def get_cache_dir(bucket_name):
    """取得指定 bucket 的本地快取目錄

    Args:
        bucket_name: GCS bucket 名稱

    Returns:
        Path: 快取目錄路徑
    """
    cache_dir = Path(CACHE_ROOT_DIR) / bucket_name
    return cache_dir


def ensure_cache_dir(bucket_name):
    """確保快取目錄存在

    Args:
        bucket_name: GCS bucket 名稱

    Returns:
        Path: 快取目錄路徑
    """
    cache_dir = get_cache_dir(bucket_name)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_local_cache_path(bucket_name, file_path):
    """取得檔案的本地快取路徑

    Args:
        bucket_name: GCS bucket 名稱
        file_path: GCS 檔案路徑（相對於 bucket）

    Returns:
        Path: 本地快取檔案路徑
    """
    cache_dir = get_cache_dir(bucket_name)
    # 保持原始檔案結構
    local_path = cache_dir / file_path
    return local_path


def file_exists_in_cache(bucket_name, file_path):
    """檢查檔案是否存在於本地快取

    Args:
        bucket_name: GCS bucket 名稱
        file_path: GCS 檔案路徑

    Returns:
        bool: True 如果檔案存在
    """
    local_path = get_local_cache_path(bucket_name, file_path)
    return local_path.exists()


def download_parquet_with_cache(
    bucket_name,
    file_path,
    project_name,
    force_download=False,
    verbose=True
):
    """下載 Parquet 檔案並使用本地快取

    工作流程：
    1. 檢查本地快取是否存在
    2. 如果存在且 force_download=False，直接返回本地路徑
    3. 如果不存在或 force_download=True，從 GCS 下載並保存到本地

    Args:
        bucket_name: GCS bucket 名稱
        file_path: GCS 檔案路徑（例如 "date=2024-12-22.parquet"）
        project_name: GCP 專案名稱
        force_download: 是否強制重新下載（預設 False）
        verbose: 是否顯示詳細日誌（預設 True）

    Returns:
        Path: 本地檔案路徑，如果下載失敗返回 None
    """
    local_path = get_local_cache_path(bucket_name, file_path)

    # 檢查本地快取
    if local_path.exists() and not force_download:
        if verbose:
            logger.info(f"✓ 從本地快取讀取: {local_path}")
        return local_path

    # 從 GCS 下載
    try:
        if verbose:
            logger.info(f"從 GCS 下載: gs://{bucket_name}/{file_path}")

        # 確保父目錄存在
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # 連接 GCS 並下載
        client = storage.Client(project=project_name)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)

        if not blob.exists():
            if verbose:
                logger.warning(f"檔案不存在於 GCS: gs://{bucket_name}/{file_path}")
            return None

        # 下載檔案
        blob.download_to_filename(str(local_path))

        if verbose:
            file_size_mb = local_path.stat().st_size / (1024 * 1024)
            logger.info(f"✓ 已下載並快取到本地: {local_path} ({file_size_mb:.2f} MB)")

        return local_path

    except Exception as e:
        logger.error(f"下載檔案失敗: gs://{bucket_name}/{file_path}, 錯誤: {e}")
        return None


def read_parquet_with_cache(
    bucket_name,
    file_path,
    project_name,
    force_download=False,
    verbose=True
):
    """下載並讀取 Parquet 檔案（使用本地快取）

    這是一個便利函數，結合下載和讀取

    Args:
        bucket_name: GCS bucket 名稱
        file_path: GCS 檔案路徑
        project_name: GCP 專案名稱
        force_download: 是否強制重新下載
        verbose: 是否顯示詳細日誌

    Returns:
        polars.DataFrame: 讀取的 DataFrame，如果失敗返回 None
    """
    import polars as pl

    local_path = download_parquet_with_cache(
        bucket_name=bucket_name,
        file_path=file_path,
        project_name=project_name,
        force_download=force_download,
        verbose=verbose
    )

    if local_path is None:
        return None

    try:
        df = pl.read_parquet(local_path)
        return df
    except Exception as e:
        logger.error(f"讀取 Parquet 失敗: {local_path}, 錯誤: {e}")
        return None


def clear_cache(bucket_name=None):
    """清除本地快取

    Args:
        bucket_name: 指定要清除的 bucket 快取，None 則清除所有
    """
    import shutil

    if bucket_name:
        cache_dir = get_cache_dir(bucket_name)
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            logger.info(f"已清除快取: {cache_dir}")
        else:
            logger.info(f"快取目錄不存在: {cache_dir}")
    else:
        cache_root = Path(CACHE_ROOT_DIR)
        if cache_root.exists():
            shutil.rmtree(cache_root)
            logger.info(f"已清除所有快取: {cache_root}")
        else:
            logger.info(f"快取根目錄不存在: {cache_root}")


def get_cache_info(bucket_name=None):
    """取得快取資訊

    Args:
        bucket_name: 指定要查詢的 bucket，None 則查詢所有

    Returns:
        dict: 快取資訊
    """
    info = {
        "total_files": 0,
        "total_size_mb": 0,
        "buckets": {}
    }

    cache_root = Path(CACHE_ROOT_DIR)
    if not cache_root.exists():
        return info

    if bucket_name:
        buckets = [bucket_name]
    else:
        # 列出所有 bucket 快取
        buckets = [d.name for d in cache_root.iterdir() if d.is_dir()]

    for bucket in buckets:
        cache_dir = get_cache_dir(bucket)
        if not cache_dir.exists():
            continue

        bucket_files = list(cache_dir.rglob("*.parquet"))
        bucket_size = sum(f.stat().st_size for f in bucket_files)

        info["buckets"][bucket] = {
            "files": len(bucket_files),
            "size_mb": bucket_size / (1024 * 1024)
        }

        info["total_files"] += len(bucket_files)
        info["total_size_mb"] += bucket_size / (1024 * 1024)

    return info


if __name__ == "__main__":
    # 設定日誌
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # 顯示快取資訊
    info = get_cache_info()
    print("\n本地快取資訊:")
    print(f"總檔案數: {info['total_files']}")
    print(f"總大小: {info['total_size_mb']:.2f} MB")

    if info['buckets']:
        print("\n各 Bucket 快取:")
        for bucket, data in info['buckets'].items():
            print(f"  {bucket}: {data['files']} 個檔案, {data['size_mb']:.2f} MB")
    else:
        print("\n目前沒有快取資料")
