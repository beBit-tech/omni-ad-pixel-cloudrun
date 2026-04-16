#!/usr/bin/env python3
"""
共享的 Parquet 數據加載和緩存模組

功能：
1. 從 GCS 載入 Parquet 數據並去重
2. 將處理後的數據保存到本地緩存
3. 智能使用緩存或增量更新數據

使用方法:
    from parquet_data_loader import load_or_cache_parquet_data

    data = load_or_cache_parquet_data(
        bucket_name="daily-pixel-data-consolidated",
        project_name="bebit-tech-website",
        end_date="2026-03-15"
    )
"""

import gc
import logging
from datetime import datetime
from pathlib import Path

import polars as pl
from gcs_cache_helper import get_cache_dir, read_parquet_with_cache
from google.cloud import storage

logger = logging.getLogger(__name__)

# GCS Bucket 配置
DEFAULT_BUCKET_NAME = "daily-pixel-data-consolidated"


def get_preprocessed_cache_path(end_date, bucket_name=DEFAULT_BUCKET_NAME):
    """獲取預處理緩存文件路徑（放在項目根目錄的 local_cache）

    Args:
        end_date: 結束日期，格式 YYYY-MM-DD
        bucket_name: GCS bucket 名稱（保留參數以保持接口一致）

    Returns:
        tuple: (os_cache_path, onead_cache_path)
    """
    # 使用項目根目錄的 local_cache（相對於當前文件的上一層目錄）
    current_file = Path(__file__)
    project_root = current_file.parent.parent  # parquet_data_loader.py 在 data_scripts/ 下
    cache_dir = project_root / "local_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    os_path = cache_dir / f"preprocessed_os_until_{end_date}.parquet"
    onead_path = cache_dir / f"preprocessed_onead_until_{end_date}.parquet"
    return os_path, onead_path


def check_preprocessed_cache(end_date, bucket_name=DEFAULT_BUCKET_NAME):
    """檢查預處理緩存是否存在

    Args:
        end_date: 結束日期，格式 YYYY-MM-DD
        bucket_name: GCS bucket 名稱

    Returns:
        bool: 緩存是否存在
    """
    os_path, onead_path = get_preprocessed_cache_path(end_date, bucket_name)
    exists = os_path.exists() and onead_path.exists()

    if exists:
        os_size = os_path.stat().st_size / (1024 * 1024)  # MB
        onead_size = onead_path.stat().st_size / (1024 * 1024)  # MB
        logger.info(f"✓ 找到預處理緩存 (local cache):")
        logger.info(f"  OS 數據: {os_path.name} ({os_size:.1f} MB)")
        logger.info(f"  OneAD 數據: {onead_path.name} ({onead_size:.1f} MB)")

    return exists


def load_preprocessed_cache(end_date, bucket_name=DEFAULT_BUCKET_NAME):
    """載入預處理緩存

    Args:
        end_date: 結束日期，格式 YYYY-MM-DD
        bucket_name: GCS bucket 名稱

    Returns:
        dict: {
            'df_os': DataFrame with columns [cid, mapping_id],
            'df_onead': DataFrame with columns [mapping_id, cid]
        }
    """
    os_path, onead_path = get_preprocessed_cache_path(end_date, bucket_name)

    logger.info(f"載入預處理緩存（截至 {end_date}）...")

    df_os = pl.read_parquet(os_path)
    logger.info(f"  ✓ OS 數據: {len(df_os):,} 條記錄")

    df_onead = pl.read_parquet(onead_path)
    logger.info(f"  ✓ OneAD 數據: {len(df_onead):,} 條記錄")

    return {
        "df_os": df_os,
        "df_onead": df_onead,
    }


def save_preprocessed_cache(df_os, df_onead, end_date, bucket_name=DEFAULT_BUCKET_NAME):
    """保存預處理緩存

    Args:
        df_os: OS DataFrame
        df_onead: OneAD DataFrame
        end_date: 結束日期，格式 YYYY-MM-DD
        bucket_name: GCS bucket 名稱
    """
    os_path, onead_path = get_preprocessed_cache_path(end_date, bucket_name)

    logger.info(f"保存預處理緩存到 local cache（截至 {end_date}）...")

    df_os.write_parquet(os_path, compression="zstd")
    logger.info(f"  ✓ OS 數據已保存: {os_path}")

    df_onead.write_parquet(onead_path, compression="zstd")
    logger.info(f"  ✓ OneAD 數據已保存: {onead_path}")


def generate_date_range(start_date, end_date):
    """產生日期範圍列表（從 match_service_to_onead.py 複製）"""
    from datetime import timedelta

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return date_list


def list_dates_from_local_cache(bucket_name):
    """從本地緩存目錄列出所有可用的日期（從 match_service_to_onead.py 複製）"""
    cache_dir = get_cache_dir(bucket_name)
    if not cache_dir.exists():
        return []

    date_list = []
    for file_path in cache_dir.glob("date=*.parquet"):
        date_str = file_path.stem.replace("date=", "")
        date_list.append(date_str)

    return sorted(date_list)


def load_parquet_data_from_gcs(
    bucket_name, project_name, start_date=None, end_date=None, force_download=False
):
    """從 GCS 載入 Parquet 數據（優先使用本地快取，批量處理優化版）

    這是從 match_service_to_onead.py 複製並改進的版本

    Args:
        bucket_name: GCS bucket 名稱
        project_name: GCP 專案名稱
        start_date: 開始日期，可選
        end_date: 結束日期，可選
        force_download: 是否強制從 GCS 重新下載，預設 False

    Returns:
        dict: {
            'df_os': DataFrame with columns [cid, mapping_id] for partner=os,
            'df_onead': DataFrame with columns [mapping_id, cid] for partner=OneAD,
            'date_range': {'start': start_date, 'end': end_date, 'days': len(date_list)}
        }
    """
    logger.info(f"連接到 GCS bucket: {bucket_name}")
    client = storage.Client(project=project_name)
    bucket = client.bucket(bucket_name)

    # 確定要查詢的日期範圍
    if start_date and end_date:
        date_list = generate_date_range(start_date, end_date)
        logger.info(f"查詢日期範圍: {start_date} 到 {end_date} ({len(date_list)} 天)")
    elif start_date:
        date_list = [start_date]
        logger.info(f"查詢單日資料: {start_date}")
    else:
        # 查詢所有資料：優先使用本地緩存，再補充 GCS
        logger.info("查詢所有資料")

        # 1. 先從本地緩存列出日期
        local_dates = list_dates_from_local_cache(bucket_name)
        logger.info(f"本地緩存找到 {len(local_dates)} 個日期檔案")

        # 2. 再從 GCS 列出日期
        blobs = bucket.list_blobs()
        gcs_files = [
            blob.name
            for blob in blobs
            if blob.name.startswith("date=") and blob.name.endswith(".parquet")
        ]
        gcs_dates = [f.replace("date=", "").replace(".parquet", "") for f in gcs_files]
        logger.info(f"GCS 找到 {len(gcs_dates)} 個日期檔案")

        # 3. 合併並去重（優先使用本地緩存的順序）
        date_set = set(local_dates)
        for gcs_date in gcs_dates:
            date_set.add(gcs_date)

        date_list = sorted(date_set)
        logger.info(f"總共找到 {len(date_list)} 個唯一日期檔案")

    if not date_list:
        logger.warning("未找到任何檔案")
        return None

    # 優化的讀取策略：延後去重到最後一步
    logger.info("開始讀取數據（Polars 批量處理優化版）...")

    # 臨時批次列表
    os_batch = []
    onead_batch = []
    BATCH_SIZE = 10  # 批次大小（平衡去重效率和記憶體使用）

    total_records = 0
    successful_files = 0
    failed_files = 0
    local_cache_count = 0
    gcs_download_count = 0

    for i, date in enumerate(date_list):
        current_file_num = i + 1
        file_path = f"date={date}.parquet"

        try:
            # 每處理 10 個檔案顯示一次進度
            if current_file_num % 10 == 0 or current_file_num == len(date_list):
                logger.info(f"讀取進度: {current_file_num}/{len(date_list)}")

            # 使用本地快取讀取檔案
            df, source = read_parquet_with_cache(
                bucket_name=bucket_name,
                file_path=file_path,
                project_name=project_name,
                force_download=force_download,
                verbose=False,
                return_source=True,
            )

            if df is None:
                failed_files += 1
                continue

            file_record_count = len(df)
            total_records += file_record_count

            # 處理 partner 欄位
            if "partner" not in df.columns:
                df = df.with_columns(pl.lit("OneAD").alias("partner"))
            else:
                df = df.with_columns(
                    pl.when(pl.col("partner").is_null() | (pl.col("partner") == ""))
                    .then(pl.lit("OneAD"))
                    .otherwise(pl.col("partner"))
                    .alias("partner")
                )

            # 確保有必要的欄位
            if "mapping_id" not in df.columns or "cid" not in df.columns:
                failed_files += 1
                continue

            # 只選取需要的欄位，過濾 null 值，並去重，減少記憶體使用
            df = (
                df.select(["partner", "cid", "mapping_id"])
                .filter(pl.col("cid").is_not_null() & pl.col("mapping_id").is_not_null())
                .unique()
            )

            # 分離 OS 和 OneAD 數據
            df_os_new = df.filter(pl.col("partner") == "os").select(["cid", "mapping_id"])
            df_onead_new = df.filter(pl.col("partner") == "OneAD").select(["mapping_id", "cid"])

            os_count = len(df_os_new)
            onead_count = len(df_onead_new)

            # 加入批次
            if os_count > 0:
                os_batch.append(df_os_new)
            if onead_count > 0:
                onead_batch.append(df_onead_new)

            # 更新統計
            if source == "local":
                local_cache_count += 1
            else:
                gcs_download_count += 1

            successful_files += 1

            # 釋放記憶體
            del df, df_os_new, df_onead_new

            # 每處理 BATCH_SIZE 個檔案時，合併批次並立即去重
            if len(os_batch) >= BATCH_SIZE:
                logger.info(f"  合併 OS 批次 (累積 {len(os_batch)} 個檔案)...")
                batch_merged = pl.concat(os_batch, how="vertical")
                before_count = len(batch_merged)
                del os_batch
                batch_merged = batch_merged.unique()
                after_count = len(batch_merged)
                logger.info(f"    去重後: {after_count:,} 記錄 (-{before_count - after_count:,}) ✓")
                os_batch = [batch_merged]
                del batch_merged
                gc.collect()

            if len(onead_batch) >= BATCH_SIZE:
                logger.info(f"  合併 OneAD 批次 (累積 {len(onead_batch)} 個檔案)...")
                batch_merged = pl.concat(onead_batch, how="vertical")
                before_count = len(batch_merged)
                del onead_batch
                batch_merged = batch_merged.unique()
                after_count = len(batch_merged)
                logger.info(f"    去重後: {after_count:,} 記錄 (-{before_count - after_count:,}) ✓")
                onead_batch = [batch_merged]
                del batch_merged
                gc.collect()

        except Exception as e:
            logger.error(f"處理檔案失敗: {file_path}, 錯誤: {e}")
            failed_files += 1
            continue

    # 最終合併並去重
    logger.info("最終合併去重中...")
    df_os = None
    df_onead = None

    if os_batch:
        batch_count = len(os_batch)
        logger.info(f"  處理 OS 最終批次 ({batch_count} 個批次)...")
        if batch_count > 1:
            df_os = pl.concat(os_batch, how="vertical")
            before = len(df_os)
            del os_batch
            gc.collect()
            df_os = df_os.unique()
            after = len(df_os)
            logger.info(f"    去重後: {after:,} 記錄 (-{before - after:,})")
        else:
            df_os = os_batch[0]
            del os_batch
        logger.info(f"✓ OS 最終唯一記錄數: {len(df_os):,}")
        gc.collect()

    if onead_batch:
        batch_count = len(onead_batch)
        logger.info(f"  處理 OneAD 最終批次 ({batch_count} 個批次)...")
        if batch_count > 1:
            df_onead = pl.concat(onead_batch, how="vertical")
            before = len(df_onead)
            del onead_batch
            gc.collect()
            df_onead = df_onead.unique()
            after = len(df_onead)
            logger.info(f"    去重後: {after:,} 記錄 (-{before - after:,})")
        else:
            df_onead = onead_batch[0]
            del onead_batch
        logger.info(f"✓ OneAD 最終唯一記錄數: {len(df_onead):,}")
        gc.collect()

    logger.info(f"成功讀取 {successful_files} 個檔案，失敗 {failed_files} 個")
    logger.info(f"  └─ 本地快取: {local_cache_count} 個 | GCS 下載: {gcs_download_count} 個")
    logger.info(f"總共 {total_records:,} 條記錄")

    if df_os is None and df_onead is None:
        logger.warning("沒有有效數據")
        return None

    if successful_files == 0:
        logger.warning("沒有成功讀取任何檔案")
        return None

    # 計算實際處理的日期範圍
    actual_start_date = min(date_list) if date_list else None
    actual_end_date = max(date_list) if date_list else None

    return {
        "df_os": df_os,
        "df_onead": df_onead,
        "date_range": {"start": actual_start_date, "end": actual_end_date, "days": len(date_list)},
    }


def load_or_cache_parquet_data(
    bucket_name,
    project_name,
    start_date=None,
    end_date=None,
    force_download=False,
    use_cache=True,
    save_cache=True,
):
    """載入 Parquet 數據（智能使用緩存）

    這個函數會：
    1. 檢查是否有預處理緩存
    2. 如果有且日期匹配，直接載入緩存
    3. 如果沒有或需要更新，從 GCS 載入並保存緩存

    Args:
        bucket_name: GCS bucket 名稱
        project_name: GCP 專案名稱
        start_date: 開始日期，可選
        end_date: 結束日期，可選（建議設為固定值如 "2026-03-15"）
        force_download: 是否強制從 GCS 重新下載
        use_cache: 是否使用預處理緩存，預設 True
        save_cache: 是否保存預處理緩存，預設 True

    Returns:
        dict: {
            'df_os': DataFrame,
            'df_onead': DataFrame,
            'date_range': dict (可選)
        }
    """
    # 如果指定了 end_date 且啟用緩存，檢查是否有預處理緩存
    if use_cache and end_date and not force_download:
        if check_preprocessed_cache(end_date, bucket_name):
            return load_preprocessed_cache(end_date, bucket_name)
        else:
            logger.info(f"未找到截至 {end_date} 的預處理緩存，將從 GCS 載入數據")

    # 從 GCS 載入數據
    data = load_parquet_data_from_gcs(
        bucket_name, project_name, start_date, end_date, force_download
    )

    if data is None:
        return None

    # 如果啟用保存緩存且指定了 end_date，保存預處理緩存
    if save_cache and end_date:
        save_preprocessed_cache(data["df_os"], data["df_onead"], end_date, bucket_name)

    return data


def main():
    """獨立執行：生成預處理緩存"""
    import argparse

    parser = argparse.ArgumentParser(
        description="生成 Parquet 數據的預處理緩存",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  %(prog)s --end-date 2026-03-15                    # 生成截至 2026-03-15 的緩存
  %(prog)s --start-date 2025-12-11 --end-date 2026-03-15  # 指定日期範圍
  %(prog)s --end-date 2026-03-15 --force-download   # 強制重新下載
        """,
    )
    parser.add_argument("--start-date", help="開始日期 (YYYY-MM-DD)，可選")
    parser.add_argument("--end-date", required=True, help="結束日期 (YYYY-MM-DD)，必填")
    parser.add_argument(
        "--force-download", action="store_true", help="強制從 GCS 重新下載"
    )
    parser.add_argument(
        "--bucket", default="daily-pixel-data-consolidated", help="GCS bucket 名稱"
    )
    parser.add_argument(
        "--project", default="bebit-tech-website", help="GCP 專案名稱"
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logger.info("=" * 80)
    logger.info("Parquet 數據預處理緩存生成工具")
    logger.info("=" * 80)

    data = load_or_cache_parquet_data(
        bucket_name=args.bucket,
        project_name=args.project,
        start_date=args.start_date,
        end_date=args.end_date,
        force_download=args.force_download,
        use_cache=False,  # 強制重新生成
        save_cache=True,
    )

    if data:
        logger.info("\n" + "=" * 80)
        logger.info("✓ 預處理緩存生成成功！")
        logger.info("=" * 80)
        logger.info(f"OS 數據: {len(data['df_os']):,} 條記錄")
        logger.info(f"OneAD 數據: {len(data['df_onead']):,} 條記錄")

        os_path, onead_path = get_preprocessed_cache_path(args.end_date, args.bucket)
        logger.info(f"\n緩存位置 (local cache):")
        logger.info(f"  {os_path}")
        logger.info(f"  {onead_path}")
        logger.info("=" * 80)
    else:
        logger.error("預處理緩存生成失敗")


if __name__ == "__main__":
    main()
