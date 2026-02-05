#!/usr/bin/env python3
"""
匹配 Purchase Intent Service IDs 到 OneAD CID 的脚本

功能：
根据 purchase_intent_users.csv 中的 service_ids，
在 Parquet 数据中查找对应的 OneAD cid。

使用方法:
    python match_purchase_intent_to_onead.py [options]

參數:
    --input: 輸入 CSV 路徑（默認: purchase_intent_users.csv）
    --output-dir: OneID 文件輸出目錄（默認: purchase_intent_output/）
    --summary: 統計 CSV 輸出路徑（默認: purchase_intent_summary.csv）
    --force-download: 強制從 GCS 重新下載
    --start-date: 開始日期（可選，格式: YYYY-MM-DD）
    --end-date: 結束日期（可選，格式: YYYY-MM-DD）

範例:
    python match_purchase_intent_to_onead.py
    python match_purchase_intent_to_onead.py --start-date 2024-12-22 --end-date 2024-12-23
    python match_purchase_intent_to_onead.py --force-download

處理邏輯：
1. 讀取 purchase_intent_users.csv
2. 從 Parquet 中找 partner=os 且 cid=service_id 的記錄，獲取 mapping_id
3. 用這些 mapping_id 找 partner=OneAD 的記錄，獲取對應的 cid
4. 輸出統計 CSV 和 OneID CSV 文件
"""

import argparse
import csv
import logging
import sys
from collections import defaultdict
from pathlib import Path

from gcs_cache_helper import read_parquet_with_cache
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# 固定配置
DEFAULT_INPUT_CSV = "purchase_intent_users.csv"
DEFAULT_OUTPUT_DIR = "purchase_intent_output"
DEFAULT_SUMMARY_CSV = "purchase_intent_summary.csv"
GCS_BUCKET = "daily-pixel-data-consolidated"
GCS_PROJECT = "bebit-tech-website"


def read_purchase_intent_csv(csv_path):
    """讀取 purchase_intent_users.csv

    Args:
        csv_path: CSV 文件路徑

    Returns:
        list: [
            {
                'category_id': '1',
                'category_zh': '/藝術與娛樂/視覺藝術與設計',
                'group1_service_ids': ['id1', 'id2', ...],
                'group2_service_ids': ['id3', 'id4', ...],
            },
            ...
        ]
    """
    logger.info(f"讀取 CSV 文件: {csv_path}")

    # 增加 CSV 字段大小限制以處理長的 service_ids 列表
    csv.field_size_limit(10000000)  # 10MB

    categories = []

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row_count = 0

            for row in reader:
                row_count += 1
                category_id = (row.get("category_id") or "").strip()
                category_zh = (row.get("category_zh") or "").strip()
                group1_service_ids_str = (row.get("group1_service_ids") or "").strip()
                group2_service_ids_str = (row.get("group2_service_ids") or "").strip()
                group1_count = (row.get("group1_count") or "0").strip()
                group2_count = (row.get("group2_count") or "0").strip()

                if not category_id or not category_zh:
                    logger.warning(f"第 {row_count} 行缺少 category_id 或 category_zh，跳過")
                    continue

                # 解析 service IDs（逗號分隔）
                group1_service_ids = (
                    [sid.strip() for sid in group1_service_ids_str.split(",") if sid.strip()]
                    if group1_service_ids_str
                    else []
                )
                group2_service_ids = (
                    [sid.strip() for sid in group2_service_ids_str.split(",") if sid.strip()]
                    if group2_service_ids_str
                    else []
                )

                categories.append(
                    {
                        "category_id": category_id,
                        "category_zh": category_zh,
                        "group1_service_ids": group1_service_ids,
                        "group2_service_ids": group2_service_ids,
                        "group1_count": group1_count,
                        "group2_count": group2_count,
                    }
                )

            logger.info(f"成功讀取 {row_count} 行數據")
            logger.info(f"有效類別數: {len(categories)}")

            return categories

    except FileNotFoundError:
        logger.error(f"找不到 CSV 文件: {csv_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"讀取 CSV 失敗: {e}")
        sys.exit(1)


def load_parquet_data(
    bucket_name, project_name, start_date=None, end_date=None, force_download=False
):
    """從 GCS 載入 Parquet 數據（優先使用本地快取）

    這個函數從 match_service_to_onead.py 複製並簡化

    Args:
        bucket_name: GCS bucket 名稱
        project_name: GCP 專案名稱
        start_date: 開始日期，可選
        end_date: 結束日期，可選
        force_download: 是否強制從 GCS 重新下載

    Returns:
        dict: {
            'os_cid_to_mappings': {cid: set(mapping_ids)} for partner=os,
            'mapping_to_onead_cids': {mapping_id: set(cids)} for partner=OneAD
        }
    """
    # 導入必要的函數
    import polars as pl
    from match_service_to_onead import generate_date_range, list_dates_from_local_cache

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

        # 3. 合併並去重
        date_set = set(local_dates)
        for gcs_date in gcs_dates:
            date_set.add(gcs_date)

        date_list = sorted(date_set)
        logger.info(f"總共找到 {len(date_list)} 個唯一日期檔案")

    if not date_list:
        logger.warning("未找到任何檔案")
        return None

    # 用於存儲映射關係
    os_cid_to_mappings = defaultdict(set)  # OS: cid -> set(mapping_ids)
    mapping_to_onead_cids = defaultdict(set)  # OneAD: mapping_id -> set(cids)
    total_records = 0
    successful_files = 0
    failed_files = 0

    # 逐個日期讀取
    for i, date in enumerate(date_list):
        current_file_num = i + 1
        file_path = f"date={date}.parquet"

        try:
            if current_file_num % 10 == 0 or current_file_num == len(date_list):
                logger.info(f"處理進度: {current_file_num}/{len(date_list)}")

            # 使用本地快取讀取檔案
            df = read_parquet_with_cache(
                bucket_name=bucket_name,
                file_path=file_path,
                project_name=project_name,
                force_download=force_download,
                verbose=False,
            )

            if df is None:
                logger.warning(f"讀取檔案失敗，跳過: {file_path}")
                failed_files += 1
                continue

            total_records += len(df)

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
                logger.warning(f"檔案 {file_path} 缺少必要欄位，跳過")
                failed_files += 1
                continue

            # 分別處理 OS 和 OneAD 數據
            # OS: 建立 cid -> mapping_ids 映射
            df_os = df.filter(pl.col("partner") == "os")
            if len(df_os) > 0:
                os_pairs = df_os.select(["cid", "mapping_id"]).unique()
                for row in os_pairs.iter_rows():
                    cid, mapping_id = row
                    if cid and mapping_id:
                        os_cid_to_mappings[cid].add(mapping_id)

            # OneAD: 建立 mapping_id -> cids 映射
            df_onead = df.filter(pl.col("partner") == "OneAD")
            if len(df_onead) > 0:
                onead_pairs = df_onead.select(["mapping_id", "cid"]).unique()
                for row in onead_pairs.iter_rows():
                    mapping_id, cid = row
                    if mapping_id and cid:
                        mapping_to_onead_cids[mapping_id].add(cid)

            successful_files += 1

            # 釋放記憶體
            del df

        except Exception as e:
            logger.error(f"讀取檔案失敗: {file_path}, 錯誤: {e}")
            failed_files += 1
            continue

    logger.info(f"成功處理 {successful_files} 個檔案，失敗 {failed_files} 個")
    logger.info(f"總共處理 {total_records:,} 條記錄")
    logger.info(f"OS 唯一 cid 數: {len(os_cid_to_mappings):,}")
    logger.info(f"OneAD 唯一 mapping_id 數: {len(mapping_to_onead_cids):,}")

    if successful_files == 0:
        logger.warning("沒有成功讀取任何檔案")
        return None

    return {
        "os_cid_to_mappings": dict(os_cid_to_mappings),
        "mapping_to_onead_cids": dict(mapping_to_onead_cids),
    }


def match_service_ids_to_onead(service_ids, parquet_data):
    """將一組 service IDs 匹配到 OneAD IDs

    Args:
        service_ids: list of service IDs
        parquet_data: {
            'os_cid_to_mappings': {cid: set(mapping_ids)},
            'mapping_to_onead_cids': {mapping_id: set(cids)}
        }

    Returns:
        set: 去重後的 OneAD IDs
    """
    os_cid_to_mappings = parquet_data["os_cid_to_mappings"]
    mapping_to_onead_cids = parquet_data["mapping_to_onead_cids"]

    # 步驟1: 找出所有 service_id 對應的 mapping_ids
    all_mapping_ids = set()
    for service_id in service_ids:
        mapping_ids = os_cid_to_mappings.get(service_id, set())
        all_mapping_ids.update(mapping_ids)

    # 步驟2: 用這些 mapping_ids 找 OneAD 的 cids
    onead_cids = set()
    for mapping_id in all_mapping_ids:
        cids = mapping_to_onead_cids.get(mapping_id, set())
        onead_cids.update(cids)

    return onead_cids


def write_onead_csv(category_id, category_zh, group_name, onead_ids, output_dir):
    """寫入 OneID 到 CSV 文件

    Args:
        category_id: 類別 ID（用於數字前綴）
        category_zh: 類別中文名稱（用作資料夾名稱）
        group_name: 'group1' or 'group2'
        onead_ids: set of OneAD IDs
        output_dir: 輸出根目錄

    Output:
        {output_dir}/{category_id_prefix} {category_zh}/{group_name}.csv
    """
    # 創建帶數字前綴的資料夾（扁平結構，不創建子資料夾）
    # 例如: "001 藝術與娛樂_視覺藝術與設計_視覺藝術與設計教育"
    category_id_str = str(category_id).zfill(3)  # 補零到 3 位數

    # 將 "/" 替換為 "_" 以避免創建子資料夾
    safe_category_name = category_zh.replace("/", "_").lstrip("_")

    # 組合數字前綴和類別名稱
    folder_name = f"{category_id_str} {safe_category_name}"

    category_path = Path(output_dir) / folder_name
    category_path.mkdir(parents=True, exist_ok=True)

    # 輸出文件路徑
    output_file = category_path / f"{group_name}.csv"

    # 排序 OneAD IDs
    sorted_ids = sorted(onead_ids)

    # 寫入 CSV
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # 寫入 header
            writer.writerow(["one_id"])
            # 寫入數據
            for onead_id in sorted_ids:
                writer.writerow([onead_id])

    except Exception as e:
        logger.error(f"寫入 CSV 失敗: {output_file}, 錯誤: {e}")
        raise


def write_summary_csv(results, output_path):
    """寫入統計摘要 CSV

    Args:
        results: [
            {
                'category_id': '1',
                'category_zh': '/藝術與娛樂/...',
                'group1_one_id_count': 5678,
                'group2_one_id_count': 6789
            },
            ...
        ]
        output_path: 輸出文件路徑
    """
    logger.info(f"寫入統計 CSV: {output_path}")

    fieldnames = [
        "category_id",
        "category_zh",
        "group1_one_id_count",
        "group2_one_id_count",
    ]

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        logger.info(f"✓ 成功寫入 {len(results)} 行統計數據")

    except Exception as e:
        logger.error(f"寫入統計 CSV 失敗: {e}")
        sys.exit(1)


def main():
    # 解析命令列參數
    parser = argparse.ArgumentParser(
        description="匹配 Purchase Intent Service IDs 到 OneAD CID 的脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  %(prog)s                                      # 使用默認設置
  %(prog)s --start-date 2024-12-22              # 指定開始日期
  %(prog)s --force-download                     # 強制重新下載
  %(prog)s --output-dir ./my_output             # 自定義輸出目錄
        """,
    )
    parser.add_argument(
        "--input", default=DEFAULT_INPUT_CSV, help=f"輸入 CSV 路徑（默認: {DEFAULT_INPUT_CSV}）"
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"OneID 文件輸出目錄（默認: {DEFAULT_OUTPUT_DIR}）",
    )
    parser.add_argument(
        "--summary",
        default=DEFAULT_SUMMARY_CSV,
        help=f"統計 CSV 輸出路徑（默認: {DEFAULT_SUMMARY_CSV}）",
    )
    parser.add_argument("--start-date", help="開始日期 (YYYY-MM-DD)，可選")
    parser.add_argument("--end-date", help="結束日期 (YYYY-MM-DD)，可選")
    parser.add_argument(
        "--force-download", action="store_true", help="強制從 GCS 重新下載，不使用本地快取"
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Purchase Intent Users → OneAD 匹配腳本")
    logger.info("=" * 80)

    # 步驟1: 讀取 CSV
    categories = read_purchase_intent_csv(args.input)

    # 步驟2: 載入 Parquet 數據
    parquet_data = load_parquet_data(
        GCS_BUCKET, GCS_PROJECT, args.start_date, args.end_date, args.force_download
    )

    if parquet_data is None:
        logger.error("沒有 Parquet 數據可供分析")
        sys.exit(1)

    # 步驟3: 處理每個類別
    logger.info(f"\n開始處理 {len(categories)} 個類別...")
    summary_results = []

    for i, category in enumerate(categories):
        category_id = category["category_id"]
        category_zh = category["category_zh"]
        group1_service_ids = category["group1_service_ids"]
        group2_service_ids = category["group2_service_ids"]
        group1_count = category["group1_count"]
        group2_count = category["group2_count"]

        if (i + 1) % 20 == 0 or (i + 1) == len(categories):
            logger.info(
                f"處理進度: {i + 1}/{len(categories)} ({(i + 1) / len(categories) * 100:.1f}%)"
            )

        # 處理 group1
        group1_onead_ids = match_service_ids_to_onead(group1_service_ids, parquet_data)
        write_onead_csv(category_id, category_zh, "group1", group1_onead_ids, args.output_dir)

        # 處理 group2
        group2_onead_ids = match_service_ids_to_onead(group2_service_ids, parquet_data)
        write_onead_csv(category_id, category_zh, "group2", group2_onead_ids, args.output_dir)

        # 記錄統計
        summary_results.append(
            {
                "category_id": category_id,
                "category_zh": category_zh,
                "group1_one_id_count": len(group1_onead_ids),
                "group2_one_id_count": len(group2_onead_ids),
            }
        )

        # 輸出詳細日誌（每個類別）
        if (i + 1) % 50 == 0:
            logger.info(
                f"  類別 {category_id} - {category_zh[:50]}...: "
                f"Group1={len(group1_service_ids)}→{len(group1_onead_ids)}, "
                f"Group2={len(group2_service_ids)}→{len(group2_onead_ids)}"
            )

    # 步驟4: 寫入統計 CSV
    write_summary_csv(summary_results, args.summary)

    # 完成報告
    logger.info("\n" + "=" * 80)
    logger.info("處理完成!")
    logger.info("=" * 80)
    logger.info(f"輸出統計 CSV: {args.summary}")
    logger.info(f"輸出 OneID 文件目錄: {args.output_dir}/")
    logger.info(f"總類別數: {len(categories)}")

    # 統計總 OneID 數量
    total_group1_ids = sum(r["group1_one_id_count"] for r in summary_results)
    total_group2_ids = sum(r["group2_one_id_count"] for r in summary_results)
    logger.info(f"Group1 總 OneID 數: {total_group1_ids:,}")
    logger.info(f"Group2 總 OneID 數: {total_group2_ids:,}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
