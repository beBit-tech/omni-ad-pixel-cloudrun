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
from pathlib import Path

from parquet_data_loader import load_or_cache_parquet_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# 固定配置
DEFAULT_INPUT_CSV = "purchase_intent_users_20260316.csv"
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


def build_service_to_onead_mapping(all_service_ids, parquet_data):
    """批量建立 service_id → onead_ids 的映射表（性能優化版）

    這個函數一次性處理所有 service_ids，避免多次 join 操作

    Args:
        all_service_ids: list of all service IDs
        parquet_data: {
            'df_os': DataFrame with columns [cid, mapping_id],
            'df_onead': DataFrame with columns [mapping_id, cid]
        }

    Returns:
        dict: {service_id: set(onead_ids)}
    """
    import polars as pl

    df_os = parquet_data["df_os"]
    df_onead = parquet_data["df_onead"]

    if not all_service_ids or len(all_service_ids) == 0:
        return {}

    logger.info(f"    建立 service_ids DataFrame...")
    # 創建 service_ids 的 DataFrame
    df_service_ids = pl.DataFrame({"service_id": list(set(all_service_ids))})

    logger.info(f"    Join OS 數據 (service_id → mapping_id)...")
    # Join with OS data: service_id → mapping_id
    df_with_mapping = df_service_ids.join(
        df_os.rename({"cid": "service_id"}),
        on="service_id",
        how="left"
    )

    # 過濾掉沒有 mapping_id 的記錄
    df_with_mapping_valid = df_with_mapping.filter(pl.col("mapping_id").is_not_null())

    if len(df_with_mapping_valid) == 0:
        logger.warning("    沒有找到任何 mapping_id")
        return {}

    logger.info(f"    找到 {len(df_with_mapping_valid):,} 個 service_id → mapping_id 映射")

    # 提取唯一的 mapping_ids 用於過濾
    unique_mapping_ids = df_with_mapping_valid.select("mapping_id").unique()
    logger.info(f"    唯一 mapping_id 數: {len(unique_mapping_ids):,}")

    logger.info(f"    Join OneAD 數據 (mapping_id → onead_cid)...")
    # Join with OneAD data: mapping_id → onead_cid
    df_onead_filtered = df_onead.join(unique_mapping_ids, on="mapping_id", how="semi")
    logger.info(f"    過濾後的 OneAD 記錄數: {len(df_onead_filtered):,}")

    df_complete = df_with_mapping_valid.join(
        df_onead_filtered.rename({"cid": "onead_cid"}),
        on="mapping_id",
        how="left"
    )

    # 過濾掉沒有 onead_cid 的記錄
    df_complete_valid = df_complete.filter(pl.col("onead_cid").is_not_null())

    if len(df_complete_valid) == 0:
        logger.warning("    沒有找到任何 OneAD IDs")
        return {}

    logger.info(f"    找到 {len(df_complete_valid):,} 個完整映射記錄")

    # 按 service_id 分組，聚合 onead_cids
    logger.info(f"    聚合結果...")
    df_grouped = (
        df_complete_valid
        .group_by("service_id")
        .agg(pl.col("onead_cid").unique().alias("onead_cids"))
    )

    # 轉換為 dict: {service_id: set(onead_ids)}
    logger.info(f"    轉換為映射表...")
    service_to_onead_map = {}
    for row in df_grouped.iter_rows(named=True):
        service_id = row["service_id"]
        onead_cids = set(row["onead_cids"])
        service_to_onead_map[service_id] = onead_cids

    logger.info(f"    ✓ 映射表建立完成，包含 {len(service_to_onead_map):,} 個 service_ids")

    return service_to_onead_map


def match_service_ids_to_onead(service_ids, parquet_data):
    """將一組 service IDs 匹配到 OneAD IDs（Polars 優化版）

    Args:
        service_ids: list of service IDs
        parquet_data: {
            'df_os': DataFrame with columns [cid, mapping_id],
            'df_onead': DataFrame with columns [mapping_id, cid]
        }

    Returns:
        set: 去重後的 OneAD IDs
    """
    import polars as pl

    df_os = parquet_data["df_os"]
    df_onead = parquet_data["df_onead"]

    if not service_ids or len(service_ids) == 0:
        return set()

    # 步驟1: 創建 service_ids 的 DataFrame（去重以減少資料量）
    unique_service_ids = list(set(service_ids))
    df_service_ids = pl.DataFrame({"cid": unique_service_ids})

    # 步驟2: 使用 semi join 先過濾 OS 資料（只保留需要的 cid）
    df_os_filtered = df_os.join(df_service_ids, on="cid", how="semi")

    if len(df_os_filtered) == 0:
        return set()

    # 步驟3: 提取唯一的 mapping_ids
    unique_mapping_ids = df_os_filtered.select("mapping_id").unique()

    if len(unique_mapping_ids) == 0:
        return set()

    # 步驟4: 使用 semi join 過濾 OneAD 資料（只保留需要的 mapping_id）
    df_onead_filtered = df_onead.join(unique_mapping_ids, on="mapping_id", how="semi")

    if len(df_onead_filtered) == 0:
        return set()

    # 步驟5: 提取唯一的 OneAD IDs
    onead_cids = (
        df_onead_filtered
        .select(pl.col("cid"))
        .unique()
        .to_series()
        .to_list()
    )

    return set(onead_cids)


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

    # 步驟2: 載入 Parquet 數據（使用共享緩存）
    parquet_data = load_or_cache_parquet_data(
        bucket_name=GCS_BUCKET,
        project_name=GCS_PROJECT,
        start_date=args.start_date,
        end_date=args.end_date,
        force_download=args.force_download,
        use_cache=True,
        save_cache=True,
    )

    if parquet_data is None:
        logger.error("沒有 Parquet 數據可供分析")
        sys.exit(1)

    # 步驟3: 批量處理所有類別（性能優化：一次性 join 所有 service_ids）
    logger.info(f"\n開始批量處理 {len(categories)} 個類別...")

    # 3.1: 收集所有唯一的 service_ids
    logger.info("  收集所有 service_ids...")
    all_service_ids = set()
    for category in categories:
        all_service_ids.update(category["group1_service_ids"])
        all_service_ids.update(category["group2_service_ids"])

    logger.info(f"  總共有 {len(all_service_ids):,} 個唯一 service_ids")

    # 3.2: 一次性匹配所有 service_ids → onead_ids（關鍵優化！）
    logger.info("  一次性匹配所有 service_ids → OneAD IDs（這可能需要幾分鐘）...")
    service_to_onead_map = build_service_to_onead_mapping(list(all_service_ids), parquet_data)
    logger.info(f"  ✓ 建立映射完成，有映射的 service_ids: {len(service_to_onead_map):,}")

    # 3.3: 處理每個類別（現在只需要查表，非常快）
    logger.info("\n開始寫入各類別文件...")
    summary_results = []

    for i, category in enumerate(categories):
        category_id = category["category_id"]
        category_zh = category["category_zh"]
        group1_service_ids = category["group1_service_ids"]
        group2_service_ids = category["group2_service_ids"]

        # 更頻繁地顯示進度
        if (i + 1) % 20 == 0 or (i + 1) == len(categories):
            logger.info(
                f"寫入進度: {i + 1}/{len(categories)} ({(i + 1) / len(categories) * 100:.1f}%)"
            )

        # 處理 group1：從映射表中查找
        group1_onead_ids = set()
        for service_id in group1_service_ids:
            if service_id in service_to_onead_map:
                group1_onead_ids.update(service_to_onead_map[service_id])

        write_onead_csv(category_id, category_zh, "group1", group1_onead_ids, args.output_dir)

        # 處理 group2：從映射表中查找
        group2_onead_ids = set()
        for service_id in group2_service_ids:
            if service_id in service_to_onead_map:
                group2_onead_ids.update(service_to_onead_map[service_id])

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
