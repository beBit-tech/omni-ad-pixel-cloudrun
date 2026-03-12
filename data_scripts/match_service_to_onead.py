#!/usr/bin/env python3
"""
匹配 Service ID 到 OneAD CID 的脚本

功能：
根据 audience_service_mapping.csv 中的 phone_hash 和 service_id，
在 Parquet 数据中查找对应的 OneAD cid。

使用方法:
    python match_service_to_onead.py [start_date] [end_date]

參數:
    start_date: 開始日期，格式如 '2024-12-22' (可選)
    end_date: 結束日期，格式如 '2024-12-23' (可選)

    - 如果提供兩個參數，將查詢這兩天之間的所有資料（包含起止日期）
    - 如果只提供一個參數，將只查詢該日期的資料
    - 如果不提供參數，將查詢所有資料

範例:
    python match_service_to_onead.py 2024-12-22 2024-12-23  # 查詢兩天資料
    python match_service_to_onead.py 2024-12-22             # 查詢單天資料
    python match_service_to_onead.py                         # 查詢所有資料

處理邏輯：
1. 讀取 CSV 文件，按 phone_hash 分組
2. 從 Parquet 中找 partner=os 且 cid=service_id 的記錄，獲取 mapping_id
3. 用這些 mapping_id 找 partner=OneAD 的記錄，獲取對應的 cid
4. 輸出 CSV 和統計報告
"""

import argparse
import csv
import gc
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import polars as pl
from gcs_cache_helper import get_cache_dir, read_parquet_with_cache
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# 固定配置
CSV_FILE_PATH = "audience_service_mapping.csv"
GCS_BUCKET = "daily-pixel-data-consolidated"
GCS_PROJECT = "bebit-tech-website"


def generate_date_range(start_date, end_date):
    """產生日期範圍列表"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return date_list


def list_dates_from_local_cache(bucket_name):
    """從本地緩存目錄列出所有可用的日期

    Args:
        bucket_name: GCS bucket 名稱

    Returns:
        list: 日期字符串列表（格式：YYYY-MM-DD）
    """
    cache_dir = get_cache_dir(bucket_name)
    if not cache_dir.exists():
        return []

    date_list = []
    for file_path in cache_dir.glob("date=*.parquet"):
        # 從檔名提取日期：date=2024-12-22.parquet -> 2024-12-22
        date_str = file_path.stem.replace("date=", "")
        date_list.append(date_str)

    return sorted(date_list)


def read_csv_and_group_by_phone(csv_path):
    """讀取 CSV 並按 phone_hash 分組

    Args:
        csv_path: CSV 文件路徑

    Returns:
        dict: {
            phone_hash: {
                'org_abbrs': set of org_abbr,
                'audience_ids': set of audience_id,
                'service_ids': set of service_id (排除空值)
            }
        }
    """
    logger.info(f"讀取 CSV 文件: {csv_path}")

    phone_data = defaultdict(
        lambda: {"org_abbrs": set(), "audience_ids": set(), "service_ids": set()}
    )

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row_count = 0

            for row in reader:
                row_count += 1
                phone_hash = (row.get("phone_hash") or "").strip()
                audience_id = (row.get("audience_id") or "").strip()
                service_id = (row.get("service_id") or "").strip()
                org_abbr = (row.get("org_abbr") or "").strip()

                if not phone_hash:
                    continue

                if org_abbr:
                    phone_data[phone_hash]["org_abbrs"].add(org_abbr)
                if audience_id:
                    phone_data[phone_hash]["audience_ids"].add(audience_id)
                if service_id:  # 只添加非空的 service_id
                    phone_data[phone_hash]["service_ids"].add(service_id)

            logger.info(f"成功讀取 {row_count:,} 行數據")
            logger.info(f"唯一 phone_hash 數: {len(phone_data):,}")

            # 統計有 service_id 的 phone_hash
            with_service = sum(1 for data in phone_data.values() if data["service_ids"])
            without_service = len(phone_data) - with_service
            logger.info(f"  有 service_id: {with_service:,}")
            logger.info(f"  無 service_id: {without_service:,}")

            return dict(phone_data)

    except FileNotFoundError:
        logger.error(f"找不到 CSV 文件: {csv_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"讀取 CSV 失敗: {e}")
        sys.exit(1)


def load_parquet_data_from_gcs(
    bucket_name, project_name, start_date=None, end_date=None, force_download=False
):
    """從 GCS 載入 Parquet 數據（優先使用本地快取）

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
            'date_range': {'start': start_date, 'end': end_date}
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
    logger.info("開始讀取數據（優化版）...")

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

            # 顯示處理結果（簡化版）
            if current_file_num % 10 == 0 or current_file_num == len(date_list):
                source_label = "[本地快取]" if source == "local" else "[GCS下載]"
                logger.info(
                    f"✓ {source_label} {date}: {file_record_count:,} 記錄 | "
                    f"OS: {os_count:,} | OneAD: {onead_count:,}"
                )

            # 釋放記憶體
            del df, df_os_new, df_onead_new

            # 每處理 BATCH_SIZE 個檔案時，合併批次並立即去重
            if len(os_batch) >= BATCH_SIZE:
                if os_batch:
                    logger.info(f"  合併 OS 批次 (累積 {len(os_batch)} 個檔案)...")
                    batch_merged = pl.concat(os_batch, how="vertical")
                    before_count = len(batch_merged)
                    logger.info(f"    合併前: {before_count:,} 記錄")
                    del os_batch
                    batch_merged = batch_merged.unique()
                    after_count = len(batch_merged)
                    logger.info(f"    去重後: {after_count:,} 記錄 (-{before_count - after_count:,}, {after_count/before_count*100:.1f}%) ✓")
                    os_batch = [batch_merged]
                    del batch_merged
                    gc.collect()

            if len(onead_batch) >= BATCH_SIZE:
                if onead_batch:
                    logger.info(f"  合併 OneAD 批次 (累積 {len(onead_batch)} 個檔案)...")
                    batch_merged = pl.concat(onead_batch, how="vertical")
                    before_count = len(batch_merged)
                    logger.info(f"    合併前: {before_count:,} 記錄")
                    del onead_batch
                    batch_merged = batch_merged.unique()
                    after_count = len(batch_merged)
                    logger.info(f"    去重後: {after_count:,} 記錄 (-{before_count - after_count:,}, {after_count/before_count*100:.1f}%) ✓")
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
            logger.info(f"    合併前: {before:,} 記錄")
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
            logger.info(f"    合併前: {before:,} 記錄")
            del onead_batch
            gc.collect()
            logger.info("    執行去重操作...")
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


def match_phone_to_onead(phone_data, parquet_data):
    """匹配 phone_hash 到 OneAD cid（純 Polars 方案）

    Args:
        phone_data: CSV 數據（按 phone_hash 分組）
        parquet_data: Parquet 數據（包含 DataFrames）

    Returns:
        tuple: (results, matched_count, unmatched_count, mapping_matched_count,
                mapping_unmatched_count, has_service_count)
    """
    logger.info("開始匹配數據（純 Polars 方案）...")

    df_os = parquet_data["df_os"]
    df_onead = parquet_data["df_onead"]

    # 準備 CSV 數據為 DataFrame
    csv_rows = []
    for phone_hash, data in phone_data.items():
        org_abbrs = sorted(data["org_abbrs"])
        audience_ids = sorted(data["audience_ids"])
        service_ids = sorted(data["service_ids"])

        if not service_ids:
            continue

        csv_rows.append({
            "phone_hash": phone_hash,
            "org_abbr": ";".join(org_abbrs) if org_abbrs else "",
            "audience_ids": ";".join(audience_ids) if audience_ids else "",
            "service_ids": service_ids,  # 保留為列表
        })

    if not csv_rows:
        logger.warning("沒有有效的 service_id 數據")
        return ([], 0, 0, 0, 0, 0)

    logger.info(f"  準備匹配 {len(csv_rows):,} 個 phone_hash...")
    df_csv = pl.DataFrame(csv_rows)

    # 展開 service_ids 列表
    df_csv_exploded = df_csv.explode("service_ids").rename({"service_ids": "cid"})

    # 優化：只保留需要的 cid，減少 join 資料量
    needed_cids = df_csv_exploded.select("cid").unique()
    logger.info(f"  需要匹配的唯一 service_id 數: {len(needed_cids):,}")

    # Step 1: Join with OS data to get mapping_ids（優化版：semi join 過濾）
    logger.info("  步驟1: 匹配 service_id -> mapping_id...")
    df_os_filtered = df_os.join(needed_cids, on="cid", how="semi")
    logger.info(f"  過濾後的 OS 記錄數: {len(df_os_filtered):,}")

    df_with_mapping = df_csv_exploded.join(
        df_os_filtered,
        on="cid",
        how="left"
    )

    # 優化：只保留有 mapping_id 的記錄來做第二次 join
    df_with_mapping_valid = df_with_mapping.filter(pl.col("mapping_id").is_not_null())
    needed_mapping_ids = df_with_mapping_valid.select("mapping_id").unique()
    logger.info(f"  需要匹配的唯一 mapping_id 數: {len(needed_mapping_ids):,}")

    # Step 2: Join with OneAD data to get onead_cids（優化版）
    logger.info("  步驟2: 匹配 mapping_id -> onead_cid...")
    df_onead_filtered = df_onead.join(needed_mapping_ids, on="mapping_id", how="semi")
    logger.info(f"  過濾後的 OneAD 記錄數: {len(df_onead_filtered):,}")

    df_with_onead = df_with_mapping.join(
        df_onead_filtered.rename({"cid": "onead_cid"}),
        on="mapping_id",
        how="left"
    )

    # Step 3: 按 phone_hash 聚合結果
    logger.info("  步驟3: 聚合結果...")
    df_result = (
        df_with_onead
        .group_by(["phone_hash", "org_abbr", "audience_ids"])
        .agg([
            pl.col("cid").unique().alias("os_service_ids"),
            pl.col("mapping_id").unique().drop_nulls().alias("mapping_ids"),
            pl.col("onead_cid").unique().drop_nulls().alias("onead_cids"),
        ])
        .with_columns([
            pl.col("os_service_ids").list.len().alias("service_id_count"),
            pl.col("mapping_ids").list.len().alias("mapping_id_count"),
            pl.col("onead_cids").list.len().alias("onead_cid_count"),
            (pl.col("onead_cids").list.len() > 0).alias("has_onead_cid"),
        ])
    )

    # 轉換為結果格式（完全在 Polars 中處理）
    logger.info("  轉換為輸出格式...")

    # 在 Polars 中完成所有字串轉換和格式化
    df_output = df_result.with_columns([
        pl.col("os_service_ids").list.sort().list.join(";").alias("os_service_ids"),
        pl.col("mapping_ids").list.sort().list.join(";").fill_null("").alias("mapping_ids"),
        pl.col("onead_cids").list.sort().list.join(";").fill_null("").alias("onead_cids"),
        pl.when(pl.col("has_onead_cid")).then(pl.lit("TRUE")).otherwise(pl.lit("FALSE")).alias("has_onead_cid"),
    ]).select([
        "phone_hash",
        "org_abbr",
        "audience_ids",
        "os_service_ids",
        "mapping_ids",
        "mapping_id_count",
        "onead_cids",
        "has_onead_cid",
        "onead_cid_count",
    ])

    # 使用 to_dicts() 一次性轉換（比逐行處理快很多）
    results = df_output.to_dicts()

    # 統計（使用 Polars 聚合，比 Python 循環快）
    has_service_count = len(results)
    mapping_matched_count = df_result.filter(pl.col("mapping_id_count") > 0).height
    mapping_unmatched_count = has_service_count - mapping_matched_count
    matched_count = df_result.filter(pl.col("has_onead_cid")).height
    unmatched_count = has_service_count - matched_count

    # 提取所有唯一的 OneAD CIDs
    logger.info("  提取唯一的 OneAD CIDs...")
    unique_onead_cids = (
        df_result
        .select(pl.col("onead_cids").explode().unique().drop_nulls())
        .to_series()
        .sort()
        .to_list()
    )

    logger.info(
        f"匹配完成: 有 service_id 的 phone_hash {has_service_count:,}, "
        f"找到 mapping_id {mapping_matched_count:,}, 找到 OneAD cid {matched_count:,}, "
        f"唯一 OneAD CID 數: {len(unique_onead_cids):,}"
    )

    return (
        results,
        matched_count,
        unmatched_count,
        mapping_matched_count,
        mapping_unmatched_count,
        has_service_count,
        unique_onead_cids,
    )


def write_results_to_csv(results, output_path):
    """寫入結果到 CSV

    Args:
        results: 匹配結果列表
        output_path: 輸出文件路徑
    """
    logger.info(f"寫入結果到: {output_path}")

    fieldnames = [
        "phone_hash",
        "org_abbr",
        "audience_ids",
        "os_service_ids",
        "mapping_ids",
        "mapping_id_count",
        "onead_cids",
        "has_onead_cid",
        "onead_cid_count",
    ]

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        logger.info(f"✓ 成功寫入 {len(results):,} 行數據")

    except Exception as e:
        logger.error(f"寫入 CSV 失敗: {e}")
        sys.exit(1)


def write_unique_cids_to_csv(unique_cids, output_path):
    """寫入唯一的 OneAD CIDs 到 CSV

    Args:
        unique_cids: 唯一 CID 列表
        output_path: 輸出文件路徑
    """
    logger.info(f"寫入唯一 OneAD CIDs 到: {output_path}")

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["onead_cids"])  # 寫入標題
            for cid in unique_cids:
                writer.writerow([cid])

        logger.info(f"✓ 成功寫入 {len(unique_cids):,} 個唯一 CID")

    except Exception as e:
        logger.error(f"寫入唯一 CID CSV 失敗: {e}")
        sys.exit(1)


def print_summary(
    total_count,
    matched_count,
    unmatched_count,
    mapping_matched_count=None,
    mapping_unmatched_count=None,
    has_service_count=None,
    date_range=None,
):
    """打印統計摘要

    Args:
        total_count: 總行數（所有 phone_hash）
        matched_count: OneAD cid 匹配成功數
        unmatched_count: OneAD cid 未匹配數
        mapping_matched_count: mapping_id 匹配成功數
        mapping_unmatched_count: mapping_id 未匹配數
        has_service_count: 有 service_id 的 phone_hash 數量（用於計算百分比）
        date_range: 日期範圍 dict，包含 'start', 'end', 'days'
    """
    # 使用 has_service_count 作為基準計算百分比
    base_count = has_service_count if has_service_count is not None else total_count

    matched_pct = (matched_count / base_count * 100) if base_count > 0 else 0
    unmatched_pct = (unmatched_count / base_count * 100) if base_count > 0 else 0

    print("\n" + "=" * 80)
    print("匹配統計報告")
    print("=" * 80)

    # 顯示日期範圍
    if date_range:
        start = date_range.get("start")
        end = date_range.get("end")
        days = date_range.get("days", 0)
        if start and end:
            if start == end:
                print(f"\n數據日期: {start}")
            else:
                print(f"\n數據日期範圍: {start} 到 {end} (共 {days} 天)")
        print()

    # 顯示總數（有 service_id 的 phone_hash）
    if has_service_count is not None:
        print(f"總 phone_hash 數 (有 service_id): {base_count:,}")
    else:
        print(f"總 phone_hash 數: {total_count:,}")

    # 顯示 mapping_id 統計
    if mapping_matched_count is not None and mapping_unmatched_count is not None:
        mapping_matched_pct = (mapping_matched_count / base_count * 100) if base_count > 0 else 0
        mapping_unmatched_pct = (
            (mapping_unmatched_count / base_count * 100) if base_count > 0 else 0
        )
        print(f"找到 mapping_id: {mapping_matched_count:,} ({mapping_matched_pct:.2f}%)")
        print(f"未找到 mapping_id: {mapping_unmatched_count:,} ({mapping_unmatched_pct:.2f}%)")

    print(f"找到 OneAD cid: {matched_count:,} ({matched_pct:.2f}%)")
    print(f"未找到 OneAD cid: {unmatched_count:,} ({unmatched_pct:.2f}%)")
    print("\n" + "=" * 80)


def main():
    # 解析命令列參數
    parser = argparse.ArgumentParser(
        description="匹配 Service ID 到 OneAD CID 的脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  %(prog)s 2024-12-22 2024-12-23     # 查詢兩天資料
  %(prog)s 2024-12-22                # 查詢單天資料
  %(prog)s                            # 查詢所有資料
  %(prog)s 2024-12-22 --force-download  # 強制從 GCS 重新下載
        """,
    )
    parser.add_argument("start_date", nargs="?", help="開始日期 (YYYY-MM-DD)，可選")
    parser.add_argument("end_date", nargs="?", help="結束日期 (YYYY-MM-DD)，可選")
    parser.add_argument(
        "--force-download", action="store_true", help="強制從 GCS 重新下載，不使用本地快取"
    )

    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    force_download = args.force_download

    if force_download:
        logger.info("使用 --force-download，將從 GCS 重新下載所有檔案")
    else:
        logger.info("優先使用本地快取（如果存在）")

    logger.info("開始處理...")

    # 步驟1: 讀取 CSV
    phone_data = read_csv_and_group_by_phone(CSV_FILE_PATH)

    # 步驟2: 讀取 Parquet 數據
    parquet_data = load_parquet_data_from_gcs(
        GCS_BUCKET, GCS_PROJECT, start_date, end_date, force_download
    )

    if parquet_data is None:
        logger.error("沒有 Parquet 數據可供分析")
        sys.exit(1)

    # 步驟3: 匹配數據
    (
        results,
        matched_count,
        unmatched_count,
        mapping_matched_count,
        mapping_unmatched_count,
        has_service_count,
        unique_onead_cids,
    ) = match_phone_to_onead(phone_data, parquet_data)

    # 步驟4: 寫入 CSV
    date_suffix = ""
    if start_date and end_date:
        date_suffix = f"_{start_date}_{end_date}"
    elif start_date:
        date_suffix = f"_{start_date}"

    # 寫入匹配結果
    output_path = f"service_to_onead_mapping{date_suffix}.csv"
    write_results_to_csv(results, output_path)

    # 寫入唯一的 OneAD CIDs
    unique_cids_path = f"unique_onead_cids{date_suffix}.csv"
    write_unique_cids_to_csv(unique_onead_cids, unique_cids_path)

    # 步驟5: 打印統計（包含日期範圍）
    date_range = parquet_data.get("date_range")
    print_summary(
        len(results),
        matched_count,
        unmatched_count,
        mapping_matched_count,
        mapping_unmatched_count,
        has_service_count,
        date_range,
    )


if __name__ == "__main__":
    main()
