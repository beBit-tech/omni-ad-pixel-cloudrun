#!/usr/bin/env python3
"""
每日 Parquet 檔案合併腳本

功能：
1. 讀取指定日期的所有 parquet 檔案
2. 對 (mapping_id, cid) 去重
3. 只保留指定欄位：timestamp, partner, cid, mapping_id, origin, referer
4. 輸出到 daily_consolidated/ 目錄

說明：
由於 app.py 已實現 deferred tracking（延迟追踪）機制，
只有當第三方 cookie 確認可讀時才會寫入數據，
因此不需要再篩選 mapping_id 出現次數 >= 2 的記錄。

使用方法:
    python consolidate_daily_parquet.py                      # 處理昨天的資料
    python consolidate_daily_parquet.py 2024-12-22           # 處理指定日期
    python consolidate_daily_parquet.py 2024-12-22 2024-12-25  # 處理日期範圍
    python consolidate_daily_parquet.py 2024-12-22 --force   # 強制覆蓋已存在檔案
"""

import argparse
import io
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import polars as pl
from google.cloud import storage


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# GCS 配置
GCS_BUCKET = "omni-ad-pixel-parquet-data"  # 原始資料 bucket
GCS_OUTPUT_BUCKET = "daily-pixel-data-consolidated"  # 輸出資料 bucket
GCS_PROJECT = "bebit-tech-website"

# 需要保留的欄位
KEEP_FIELDS = ["timestamp", "partner", "cid", "mapping_id", "origin", "referer"]


def generate_date_range(start_date, end_date):
    """生成日期範圍列表"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return date_list


def list_parquet_files_for_date(bucket, date_str):
    """列出指定日期的所有 parquet 檔案"""
    prefix = f"date={date_str}/"
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob for blob in blobs if blob.name.endswith(".parquet")]
    return files


def check_output_exists(output_bucket, date_str):
    """檢查輸出檔案是否已存在"""
    output_path = f"date={date_str}.parquet"
    blob = output_bucket.blob(output_path)
    return blob.exists()


def generate_summary_log(stats):
    """生成中文摘要日誌

    Args:
        stats: 處理統計資訊字典

    Returns:
        str: 中文摘要日誌內容
    """
    log_lines = [
        "=" * 70,
        "每日資料合併處理摘要",
        "=" * 70,
        "",
        f"處理日期: {stats['date']}",
        f"處理時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "--- 資料統計 ---",
        f"源檔案數量: {stats['source_files']:,} 個",
        f"原始記錄總數: {stats['raw_records']:,} 條",
        f"有效記錄數量: {stats['valid_records']:,} 條 (過濾掉 {stats['raw_records'] - stats['valid_records']:,} 條無效記錄)",
        f"最終記錄數量: {stats['final_records']:,} 條 (去重後)",
        "",
        "--- 資料處理流程 ---",
        "1. 過濾無效記錄 (mapping_id 或 cid 為空)",
        "2. 對 (mapping_id, cid) 組合去重",
        "3. 只保留欄位: timestamp, partner, cid, mapping_id, origin, referer",
        "",
        "--- Deferred Tracking 說明 ---",
        "由於 app.py 已實現延遲追蹤機制，只有當第三方 cookie 確認可讀時才會寫入數據，",
        "因此所有數據已是有效的追蹤記錄，無需額外篩選。",
        "",
        "--- 輸出資訊 ---",
        f"輸出檔案: gs://{GCS_OUTPUT_BUCKET}/{stats['output_path']}",
        f"處理耗時: {stats['elapsed_time']:.2f} 秒",
        "",
        "--- 資料品質 ---",
        f"資料保留率: {(stats['final_records'] / stats['raw_records'] * 100):.2f}%"
        if stats["raw_records"] > 0
        else "N/A",
        f"去重率: {((stats['valid_records'] - stats['final_records']) / stats['valid_records'] * 100):.2f}%"
        if stats["valid_records"] > 0
        else "N/A",
        "",
        "=" * 70,
        "處理完成",
        "=" * 70,
    ]
    return "\n".join(log_lines)


def upload_log_to_gcs(output_bucket, date_str, log_content):
    """上傳日誌檔案到 GCS

    Args:
        output_bucket: GCS 輸出 bucket 物件
        date_str: 日期字串
        log_content: 日誌內容
    """
    log_path = f"logs/date={date_str}.log"
    blob = output_bucket.blob(log_path)
    blob.upload_from_string(log_content, content_type="text/plain; charset=utf-8")
    logger.info(f"日誌檔案已上傳: gs://{GCS_OUTPUT_BUCKET}/{log_path}")


def download_and_read_parquet(blob):
    """下載並讀取單個 Parquet 檔案（用於並行處理）

    Args:
        blob: GCS blob 物件

    Returns:
        tuple: (DataFrame, 檔案名稱) 或 (None, 檔案名稱) 如果失敗
    """
    try:
        content = blob.download_as_bytes()
        df = pl.read_parquet(io.BytesIO(content))
        return df, blob.name
    except Exception as e:
        logger.warning(f"讀取檔案失敗，跳過: {blob.name}, 錯誤: {e}")
        return None, blob.name


def process_single_date(source_bucket, output_bucket, date_str, force=False):
    """處理單個日期的資料

    Args:
        source_bucket: GCS 源資料 bucket 物件
        output_bucket: GCS 輸出 bucket 物件
        date_str: 日期字串 (YYYY-MM-DD)
        force: 是否強制覆蓋已存在檔案

    Returns:
        dict: 處理統計資訊
    """
    logger.info("=" * 70)
    logger.info(f"開始處理日期: {date_str}")
    logger.info("=" * 70)

    start_time = time.time()

    # 檢查輸出檔案是否已存在
    output_path = f"date={date_str}.parquet"
    if check_output_exists(output_bucket, date_str) and not force:
        logger.warning(f"輸出檔案已存在: {output_path}")
        logger.warning("使用 --force 參數強制覆蓋")
        return None

    # 列出所有 parquet 檔案
    logger.info(f"正在列出 date={date_str}/ 下的檔案...")
    parquet_files = list_parquet_files_for_date(source_bucket, date_str)

    if not parquet_files:
        logger.warning(f"未找到任何檔案: date={date_str}/")
        return None

    logger.info(f"找到 {len(parquet_files)} 個 Parquet 檔案")

    # 使用並行下載讀取所有檔案
    logger.info("開始並行下載和處理資料...")

    # 配置並行參數
    # GCS 默認連接池大小為 10，所以使用 10 個線程避免連接池警告
    max_workers = 10  # 並行下載的線程數
    batch_size = 50  # 每批處理的文件數

    all_dataframes = []
    total_raw_records = 0
    successful_downloads = 0
    failed_downloads = 0

    # 分批並行處理，避免一次性占用太多記憶體
    total_batches = (len(parquet_files) + batch_size - 1) // batch_size

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(parquet_files))
        batch_files = parquet_files[start_idx:end_idx]

        logger.info(f"處理批次 {batch_idx + 1}/{total_batches} ({len(batch_files)} 個檔案)...")

        batch_success = 0
        batch_failed = 0

        # 並行下載當前批次
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有下載任務
            future_to_blob = {
                executor.submit(download_and_read_parquet, blob): blob for blob in batch_files
            }

            # 收集結果
            for future in as_completed(future_to_blob):
                df, _ = future.result()
                if df is not None:
                    total_raw_records += len(df)
                    all_dataframes.append(df)
                    successful_downloads += 1
                    batch_success += 1
                else:
                    failed_downloads += 1
                    batch_failed += 1

        logger.info(f"批次 {batch_idx + 1} 完成: 成功 {batch_success}, 失敗 {batch_failed}")

    logger.info(
        f"總下載結果: 成功 {successful_downloads}/{len(parquet_files)} 個檔案, 失敗 {failed_downloads} 個"
    )

    if not all_dataframes:
        logger.error("沒有成功讀取任何檔案")
        return None

    logger.info(f"原始記錄總數: {total_raw_records:,}")

    # 合併所有 DataFrames（使用 diagonal 模式處理欄位不一致的情況）
    logger.info("合併所有資料...")
    df_combined = pl.concat(all_dataframes, how="diagonal")
    del all_dataframes  # 釋放記憶體

    # 步驟1: 過濾無效記錄 (mapping_id 或 cid 為 None/空值)
    logger.info("步驟1: 過濾無效記錄 (mapping_id 或 cid 為空)...")
    df_valid = df_combined.filter(
        pl.col("mapping_id").is_not_null()
        & (pl.col("mapping_id") != "")
        & pl.col("cid").is_not_null()
        & (pl.col("cid") != "")
    )
    valid_count = len(df_valid)
    logger.info(
        f"過濾後有效記錄數: {valid_count:,} (過濾掉 {total_raw_records - valid_count:,} 條)"
    )

    # 步驟2: 選擇需要的欄位，缺失欄位用空字串填充
    logger.info("步驟2: 選擇需要的欄位...")

    # 確保所有需要的欄位都存在，不存在則添加空值
    for field in KEEP_FIELDS:
        if field not in df_valid.columns:
            logger.warning(f"欄位 '{field}' 不存在，將填充為空字串")
            df_valid = df_valid.with_columns(pl.lit("").alias(field))

    # 填充 None 值為空字串（針對 origin 和 referer）
    df_selected = df_valid.select(KEEP_FIELDS)
    df_selected = df_selected.with_columns(
        [pl.col("origin").fill_null(""), pl.col("referer").fill_null("")]
    )

    # 步驟3: 去重 (mapping_id, cid)
    logger.info("步驟3: 對 (mapping_id, cid) 去重...")
    df_final = df_selected.unique(subset=["mapping_id", "cid"], keep="first")
    final_count = len(df_final)
    logger.info(f"去重後最終記錄數: {final_count:,} (去掉 {valid_count - final_count:,} 條重複)")

    # 寫入輸出檔案
    logger.info(f"寫入輸出檔案: gs://{GCS_OUTPUT_BUCKET}/{output_path}")
    buf = io.BytesIO()
    df_final.write_parquet(buf)
    parquet_bytes = buf.getvalue()

    blob = output_bucket.blob(output_path)
    blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")

    elapsed_time = time.time() - start_time
    logger.info(f"✓ 處理完成，耗時: {elapsed_time:.2f} 秒")

    # 返回統計資訊
    stats = {
        "date": date_str,
        "source_files": len(parquet_files),
        "raw_records": total_raw_records,
        "valid_records": valid_count,
        "final_records": final_count,
        "elapsed_time": elapsed_time,
        "output_path": output_path,
    }

    # 生成並上傳中文摘要日誌
    logger.info("生成並上傳處理日誌...")
    log_content = generate_summary_log(stats)
    upload_log_to_gcs(output_bucket, date_str, log_content)

    return stats


def print_summary(results_list):
    """列印處理摘要"""
    if not results_list:
        return

    # 過濾掉 None 結果
    results_list = [r for r in results_list if r is not None]

    if not results_list:
        logger.warning("沒有成功處理任何日期")
        return

    logger.info("\n" + "=" * 70)
    logger.info("處理摘要")
    logger.info("=" * 70)

    total_files = sum(r["source_files"] for r in results_list)
    total_raw = sum(r["raw_records"] for r in results_list)
    total_valid = sum(r["valid_records"] for r in results_list)
    total_final = sum(r["final_records"] for r in results_list)
    total_time = sum(r["elapsed_time"] for r in results_list)

    logger.info(f"處理日期數: {len(results_list)}")
    logger.info(f"總源檔案數: {total_files:,}")
    logger.info(f"總原始記錄數: {total_raw:,}")
    logger.info(f"總有效記錄數: {total_valid:,} (過濾掉 {total_raw - total_valid:,} 條無效)")
    logger.info(f"總最終記錄數: {total_final:,} (去重後)")
    logger.info(f"總處理時間: {total_time:.2f} 秒")
    logger.info("=" * 70)

    logger.info("\n詳細資訊:")
    for r in results_list:
        logger.info(
            f"  {r['date']}: {r['raw_records']:,} -> {r['final_records']:,} 記錄 ({r['elapsed_time']:.2f}s)"
        )


def main():
    parser = argparse.ArgumentParser(
        description="合併每日 Parquet 檔案並進行資料清理",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  %(prog)s                      # 處理昨天的資料
  %(prog)s 2024-12-22           # 處理指定日期
  %(prog)s 2024-12-22 2024-12-25  # 處理日期範圍
  %(prog)s 2024-12-22 --force   # 強制覆蓋已存在檔案
        """,
    )
    parser.add_argument("start_date", nargs="?", help="開始日期 (YYYY-MM-DD)，預設為昨天")
    parser.add_argument("end_date", nargs="?", help="結束日期 (YYYY-MM-DD)，可選")
    parser.add_argument("--force", action="store_true", help="強制覆蓋已存在的輸出檔案")

    args = parser.parse_args()

    # 確定處理日期
    if args.start_date:
        start_date = args.start_date
        end_date = args.end_date if args.end_date else start_date
    else:
        # 預設處理昨天
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.strftime("%Y-%m-%d")
        end_date = start_date
        logger.info(f"未指定日期，預設處理昨天: {start_date}")

    # 生成日期列表
    if start_date == end_date:
        date_list = [start_date]
    else:
        date_list = generate_date_range(start_date, end_date)

    logger.info(f"將處理 {len(date_list)} 個日期: {start_date} 到 {end_date}")

    # 連接到 GCS
    logger.info("連接到 GCS")
    logger.info(f"  源資料 Bucket: {GCS_BUCKET}")
    logger.info(f"  輸出 Bucket: {GCS_OUTPUT_BUCKET}")
    logger.info(f"  Project: {GCS_PROJECT}")
    try:
        client = storage.Client(project=GCS_PROJECT)
        source_bucket = client.bucket(GCS_BUCKET)
        output_bucket = client.bucket(GCS_OUTPUT_BUCKET)
    except Exception as e:
        logger.error(f"連接 GCS 失敗: {e}")
        sys.exit(1)

    # 處理每個日期
    results = []
    for date_str in date_list:
        result = process_single_date(source_bucket, output_bucket, date_str, force=args.force)
        results.append(result)

    # 列印摘要
    print_summary(results)


if __name__ == "__main__":
    main()
