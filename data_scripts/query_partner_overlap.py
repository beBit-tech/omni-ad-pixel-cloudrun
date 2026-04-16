#!/usr/bin/env python3
"""
查詢 partner 來自不同來源 (OneAD 和 OS) 的重疊分析

使用方法:
    python query_partner_overlap.py [start_date] [end_date]

參數:
    start_date: 開始日期，格式如 '2024-12-22' (可選)
    end_date: 結束日期，格式如 '2024-12-23' (可選)

    - 如果提供兩個參數，將查詢這兩天之間的所有資料（包含起止日期）
    - 如果只提供一個參數，將只查詢該日期的資料
    - 如果不提供參數，將查詢所有資料

範例:
    python query_partner_overlap.py 2024-12-22 2024-12-23  # 查詢兩天資料
    python query_partner_overlap.py 2024-12-22             # 查詢單天資料
    python query_partner_overlap.py                         # 查詢所有資料

說明:
    本腳本從 daily-pixel-data-consolidated bucket 讀取已整合的每日 parquet 檔案，
    分析不同 partner (OneAD 和 OS) 之間的客戶重疊情況。

    分析方法：
    1. 使用 mapping_id (cookie) 識別跨來源的使用者/裝置
    2. 將跨來源的 mapping_id 轉換為對應的 cid (客戶 ID)
    3. 以各 partner 的總客戶數 (cid) 為分母計算重疊比例

    這樣可以準確識別「同一個人」(透過 mapping_id)，
    同時以客戶數為基準進行比例計算，更具商業意義。
"""

import argparse
import gc
import logging
import sys
from datetime import datetime, timedelta

import polars as pl
from google.cloud import storage

from gcs_cache_helper import read_parquet_with_cache


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


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


def load_and_aggregate_from_gcs(bucket_name, project_name, start_date=None, end_date=None, force_download=False):
    """從 GCS 載入 Parquet 檔案並進行批次聚合（優化版，使用 Polars 向量化操作）

    從 daily-pixel-data-consolidated bucket 讀取已整合的每日 parquet 檔案
    使用批次處理和 Polars 向量化操作，大幅提升效能：
    - 每 10 個檔案合併一次並去重
    - 使用 Polars 的 group_by + agg 代替逐行迭代
    - 定期執行 gc.collect() 釋放記憶體

    Args:
        bucket_name: GCS bucket 名稱
        project_name: GCP 專案名稱
        start_date: 開始日期 (格式: YYYY-MM-DD)，可選
        end_date: 結束日期 (格式: YYYY-MM-DD)，可選
        force_download: 是否強制從 GCS 重新下載，預設 False

    Returns:
        dict: {
            'mapping_partners': mapping_id 對應的 partner 集合
            'mapping_to_cid': mapping_id 對應的 cid
            'total_records': 總記錄數
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
        # 查詢所有資料 - 需要列出所有 date=*.parquet 檔案
        logger.info("查詢所有資料")
        blobs = bucket.list_blobs()
        all_files = [
            blob.name
            for blob in blobs
            if blob.name.startswith("date=") and blob.name.endswith(".parquet")
        ]
        # 從檔案名提取日期 (date=YYYY-MM-DD.parquet -> YYYY-MM-DD)
        date_list = [f.replace("date=", "").replace(".parquet", "") for f in all_files]
        logger.info(f"找到 {len(date_list)} 個日期檔案")

    if not date_list:
        logger.warning("未找到任何檔案")
        return None

    # 使用批次處理：收集 DataFrame 後批次合併
    data_batch = []
    BATCH_SIZE = 10  # 每 10 個檔案合併一次

    total_records = 0
    successful_files = 0
    failed_files = 0
    local_cache_count = 0
    gcs_download_count = 0

    # 逐個日期讀取和批次處理
    for i, date in enumerate(date_list):
        current_file_num = i + 1
        file_path = f"date={date}.parquet"

        try:
            if current_file_num % 10 == 0 or current_file_num == len(date_list):
                logger.info(f"讀取進度: {current_file_num}/{len(date_list)}")

            # 使用本地快取讀取檔案
            df, source = read_parquet_with_cache(
                bucket_name=bucket_name,
                file_path=file_path,
                project_name=project_name,
                force_download=force_download,
                verbose=False,  # 不顯示每個檔案的詳細日誌
                return_source=True
            )

            if df is None:
                logger.warning(f"讀取檔案失敗，跳過: {file_path}")
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

            # 檢查必要欄位
            if "mapping_id" not in df.columns or "cid" not in df.columns:
                logger.warning(f"檔案 {file_path} 缺少必要欄位 (mapping_id 或 cid)，跳過")
                failed_files += 1
                continue

            # 只選取需要的欄位，過濾 null 值，並去重
            df_clean = (
                df.select(["mapping_id", "cid", "partner"])
                .filter(pl.col("mapping_id").is_not_null() & pl.col("cid").is_not_null())
                .unique()
            )

            # 加入批次
            if len(df_clean) > 0:
                data_batch.append(df_clean)

            # 更新統計
            if source == "local":
                local_cache_count += 1
            else:
                gcs_download_count += 1

            successful_files += 1

            # 釋放原始 DataFrame 記憶體
            del df, df_clean

            # 每處理 BATCH_SIZE 個檔案時，合併批次並立即去重
            if len(data_batch) >= BATCH_SIZE:
                logger.info(f"  合併批次 (累積 {len(data_batch)} 個檔案)...")
                batch_merged = pl.concat(data_batch, how="vertical")
                before_count = len(batch_merged)
                del data_batch
                batch_merged = batch_merged.unique()
                after_count = len(batch_merged)
                logger.info(f"    去重後: {after_count:,} 記錄 (-{before_count - after_count:,}) ✓")
                data_batch = [batch_merged]
                del batch_merged
                gc.collect()

        except Exception as e:
            logger.error(f"讀取檔案失敗: {file_path}, 錯誤: {e}")
            failed_files += 1
            continue

    # 最終合併所有批次
    logger.info("最終合併去重中...")
    df_all = None

    if data_batch:
        batch_count = len(data_batch)
        logger.info(f"  處理最終批次 ({batch_count} 個批次)...")
        if batch_count > 1:
            df_all = pl.concat(data_batch, how="vertical")
            before = len(df_all)
            del data_batch
            gc.collect()
            logger.info(f"    合併前: {before:,} 記錄")
            df_all = df_all.unique()
            after = len(df_all)
            logger.info(f"    去重後: {after:,} 記錄 (-{before - after:,})")
        else:
            df_all = data_batch[0]
            del data_batch
        gc.collect()

    if df_all is None or len(df_all) == 0:
        logger.warning("沒有有效數據")
        return None

    logger.info(f"✓ 最終唯一記錄數: {len(df_all):,}")

    # 使用 Polars 向量化操作建立映射（比 iter_rows 快 10-100 倍）
    logger.info("建立 mapping_id -> partners 映射...")

    # 步驟1: 使用 group_by + agg 聚合 partners
    df_partners = (
        df_all
        .group_by("mapping_id")
        .agg([
            pl.col("partner").unique().alias("partners"),
            pl.col("cid").first().alias("cid")  # 一個 mapping_id 只對應一個 cid
        ])
    )

    # 統計資訊
    unique_cids = df_partners.select("cid").unique()

    logger.info(f"成功處理 {successful_files} 個檔案，失敗 {failed_files} 個")
    logger.info(f"  └─ 本地快取: {local_cache_count} 個 | GCS 下載: {gcs_download_count} 個")
    logger.info(f"總共處理 {total_records:,} 條記錄")
    logger.info(f"唯一 mapping_id 數: {len(df_partners):,}")
    logger.info(f"唯一 cid 數: {len(unique_cids):,}")

    if successful_files == 0:
        logger.warning("沒有成功讀取任何檔案")
        return None

    # 釋放不需要的 DataFrame
    del df_all, unique_cids
    gc.collect()

    # 返回 Polars DataFrame 而非字典（快很多！）
    return {
        "df_partners": df_partners,  # DataFrame with columns: mapping_id, partners (list), cid
        "total_records": total_records,
    }


def analyze_partner_overlap(df_partners):
    """分析 partner 重疊情況（純 Polars 向量化版本）

    分析方法：
    1. 使用 mapping_id 識別跨來源的使用者（同時出現在 OneAD 和 OS）
    2. 將跨來源的 mapping_id 轉換為對應的 cid
    3. 以各 partner 的總客戶數 (cid) 為分母計算重疊比例

    Args:
        df_partners: Polars DataFrame with columns [mapping_id, partners (list), cid]

    Returns:
        dict: 分析結果
    """
    logger.info("開始分析 partner 重疊情況（純 Polars 方案）...")

    # 步驟1: 標記各 partner 類型（使用向量化操作）
    logger.info("  步驟1: 識別各 partner 類型...")
    df_tagged = df_partners.with_columns([
        pl.col("partners").list.contains("OneAD").alias("has_onead"),
        pl.col("partners").list.contains("os").alias("has_os"),
    ]).with_columns([
        (pl.col("has_onead") & pl.col("has_os")).alias("is_cross")
    ])

    # 統計基本數據
    total_unique_mapping_ids = len(df_tagged)
    total_unique_cids = df_tagged.select("cid").unique().height

    # OneAD 和 OS 的 mapping_id 數量
    onead_mapping_count = df_tagged.filter(pl.col("has_onead")).height
    os_mapping_count = df_tagged.filter(pl.col("has_os")).height
    cross_mapping_count = df_tagged.filter(pl.col("is_cross")).height

    logger.info(f"    OneAD mapping_ids: {onead_mapping_count:,}")
    logger.info(f"    OS mapping_ids: {os_mapping_count:,}")
    logger.info(f"    跨來源 mapping_ids: {cross_mapping_count:,}")

    # 步驟2: 計算各類型的唯一 cid 數量
    logger.info("  步驟2: 計算唯一 cid 數量...")

    # OneAD 的所有 cid
    onead_cids = df_tagged.filter(pl.col("has_onead")).select("cid").unique()
    onead_cid_count = len(onead_cids)

    # OS 的所有 cid
    os_cids = df_tagged.filter(pl.col("has_os")).select("cid").unique()
    os_cid_count = len(os_cids)

    # 跨來源的 cid
    cross_cids = df_tagged.filter(pl.col("is_cross")).select("cid").unique()
    cross_cid_count = len(cross_cids)

    logger.info(f"    OneAD 唯一 cids: {onead_cid_count:,}")
    logger.info(f"    OS 唯一 cids: {os_cid_count:,}")
    logger.info(f"    跨來源唯一 cids: {cross_cid_count:,}")

    # 步驟3: 計算獨占客戶（使用 anti join）
    logger.info("  步驟3: 計算獨占客戶...")

    # OneAD 獨占 = OneAD - OS
    onead_only_cids = onead_cids.join(os_cids, on="cid", how="anti")
    onead_only_count = len(onead_only_cids)

    # OS 獨占 = OS - OneAD
    os_only_cids = os_cids.join(onead_cids, on="cid", how="anti")
    os_only_count = len(os_only_cids)

    logger.info(f"    OneAD 獨占: {onead_only_count:,}")
    logger.info(f"    OS 獨占: {os_only_count:,}")

    # 步驟4: 計算比例
    logger.info("  步驟4: 計算比例...")
    onead_cross_pct = (cross_cid_count / onead_cid_count * 100) if onead_cid_count > 0 else 0
    os_cross_pct = (cross_cid_count / os_cid_count * 100) if os_cid_count > 0 else 0
    total_cross_pct = (cross_cid_count / total_unique_cids * 100) if total_unique_cids > 0 else 0

    onead_exclusive_pct = (onead_only_count / onead_cid_count * 100) if onead_cid_count > 0 else 0
    os_exclusive_pct = (os_only_count / os_cid_count * 100) if os_cid_count > 0 else 0

    # 步驟5: 準備跨來源 cid 樣本（只取前 20 個）
    logger.info("  步驟5: 準備跨來源樣本...")
    cross_cid_details = []

    if cross_cid_count > 0:
        # 取得跨來源的 cid 及其 partners（限制 20 個以避免轉換太慢）
        df_cross_sample = (
            df_tagged
            .filter(pl.col("is_cross"))
            .select(["cid", "partners"])
            .unique(subset="cid")
            .limit(20)
        )

        # 只轉換這 20 個樣本
        for row in df_cross_sample.to_dicts():
            cross_cid_details.append({
                "cid": row["cid"],
                "partners": row["partners"]
            })

    logger.info("✓ 分析完成")

    return {
        "total_unique_mapping_ids": total_unique_mapping_ids,
        "total_unique_cids": total_unique_cids,
        "cross_mapping_id_count": cross_mapping_count,
        "cross_cid_count": cross_cid_count,
        "onead_total_cids": onead_cid_count,
        "os_total_cids": os_cid_count,
        "onead_only_count": onead_only_count,
        "os_only_count": os_only_count,
        "onead_cross_pct": onead_cross_pct,
        "os_cross_pct": os_cross_pct,
        "total_cross_pct": total_cross_pct,
        "onead_exclusive_pct": onead_exclusive_pct,
        "os_exclusive_pct": os_exclusive_pct,
        "cross_cid_details": cross_cid_details,
    }


def print_results(results):
    """列印分析結果"""
    print("\n" + "=" * 80)
    print("Partner 跨來源分析報告")
    print("=" * 80)

    # 說明
    print("\n【分析方法說明】")
    print("1. 使用 mapping_id (cookie) 識別跨來源的使用者/裝置")
    print("2. 將跨來源的 mapping_id 轉換為對應的 cid (客戶 ID)")
    print("3. 以各 partner 的總客戶數 (cid) 為分母計算重疊比例")

    # 基本統計
    print("\n【基本統計】")
    print(f"總唯一 mapping_id 數:              {results['total_unique_mapping_ids']:,}")
    print(f"總唯一 cid 數:                     {results['total_unique_cids']:,}")
    print(f"OneAD 唯一客戶數 (cid):            {results['onead_total_cids']:,}")
    print(f"OS 唯一客戶數 (cid):               {results['os_total_cids']:,}")

    # 重疊分析
    print("\n【重疊分析】")
    print(f"跨來源 mapping_id 數量:            {results['cross_mapping_id_count']:,}")
    print("  (同時出現在 OneAD 和 OS 的 mapping_id)")
    print(f"對應的唯一客戶數 (cid):            {results['cross_cid_count']:,}")
    print("  (將跨來源 mapping_id 轉換為 cid 後去重)")
    print(f"\n跨來源客戶佔 OneAD 的比例:         {results['onead_cross_pct']:.2f}%")
    print(f"  ({results['cross_cid_count']:,} / {results['onead_total_cids']:,})")
    print(f"跨來源客戶佔 OS 的比例:            {results['os_cross_pct']:.2f}%")
    print(f"  ({results['cross_cid_count']:,} / {results['os_total_cids']:,})")
    print(f"跨來源客戶佔所有客戶的比例:       {results['total_cross_pct']:.2f}%")
    print(f"  ({results['cross_cid_count']:,} / {results['total_unique_cids']:,})")

    # 獨占分析
    print("\n【獨占分析】")
    print(f"僅在 OneAD 出現的客戶:             {results['onead_only_count']:,}")
    print(f"  (佔 OneAD 的比例: {results['onead_exclusive_pct']:.2f}%)")
    print(f"僅在 OS 出現的客戶:                {results['os_only_count']:,}")
    print(f"  (佔 OS 的比例: {results['os_exclusive_pct']:.2f}%)")

    # 深度洞察
    print("\n【深度洞察】")

    # 計算客戶重疊度
    if results["onead_total_cids"] > 0 and results["os_total_cids"] > 0:
        overlap_coefficient = (
            results["cross_cid_count"]
            / min(results["onead_total_cids"], results["os_total_cids"])
            * 100
        )
        print(f"重疊係數 (Overlap Coefficient):    {overlap_coefficient:.2f}%")
        print("  (跨來源客戶數 / 較小的 partner 客戶數)")

        jaccard_index = (
            results["cross_cid_count"]
            / (
                results["onead_total_cids"]
                + results["os_total_cids"]
                - results["cross_cid_count"]
            )
            * 100
        )
        print(f"Jaccard 相似度指數:                {jaccard_index:.2f}%")
        print("  (交集 / 聯集，衡量兩個 partner 客戶群的相似度)")

    # OneAD 客戶組成分析
    if results["onead_total_cids"] > 0:
        print("\nOneAD 客戶組成:")
        print(
            f"  - 獨占客戶: {results['onead_only_count']:,} "
            f"({results['onead_exclusive_pct']:.2f}%)"
        )
        print(
            f"  - 跨來源客戶: {results['cross_cid_count']:,} "
            f"({results['onead_cross_pct']:.2f}%)"
        )

    # OS 客戶組成分析
    if results["os_total_cids"] > 0:
        print("\nOS 客戶組成:")
        print(
            f"  - 獨占客戶: {results['os_only_count']:,} "
            f"({results['os_exclusive_pct']:.2f}%)"
        )
        print(
            f"  - 跨來源客戶: {results['cross_cid_count']:,} "
            f"({results['os_cross_pct']:.2f}%)"
        )

    print("\n" + "=" * 80)

    # 顯示跨來源的 cid 樣本
    if results["cross_cid_count"] > 0:
        print("\n【跨來源客戶樣本】(前 20 個)")
        for i, item in enumerate(results["cross_cid_details"][:20]):
            partners_str = ", ".join(sorted(item["partners"]))
            print(f"  {i + 1:2d}. cid={item['cid']}: [{partners_str}]")

        if results["cross_cid_count"] > 20:
            print(f"\n  ... 還有 {results['cross_cid_count'] - 20:,} 個跨來源客戶")

    print("\n" + "=" * 80)


def main():
    # 解析命令列參數
    parser = argparse.ArgumentParser(
        description="查詢 partner 來自不同來源 (OneAD 和 OS) 的重疊分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
  %(prog)s 2024-12-22 2024-12-23     # 查詢兩天資料
  %(prog)s 2024-12-22                # 查詢單天資料
  %(prog)s                            # 查詢所有資料
  %(prog)s 2024-12-22 --force-download  # 強制從 GCS 重新下載
        """
    )
    parser.add_argument('start_date', nargs='?', help='開始日期 (YYYY-MM-DD)，可選')
    parser.add_argument('end_date', nargs='?', help='結束日期 (YYYY-MM-DD)，可選')
    parser.add_argument('--force-download', action='store_true',
                        help='強制從 GCS 重新下載，不使用本地快取')

    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    force_download = args.force_download

    # 固定配置
    bucket_name = "daily-pixel-data-consolidated"
    project_name = "bebit-tech-website"

    if force_download:
        logger.info("使用 --force-download，將從 GCS 重新下載所有檔案")
    else:
        logger.info("優先使用本地快取（如果存在）")

    logger.info(f"開始查詢 - Bucket: {bucket_name}, Project: {project_name}")

    # 載入並聚合資料
    data = load_and_aggregate_from_gcs(bucket_name, project_name, start_date, end_date, force_download)

    if data is None or len(data["df_partners"]) == 0:
        logger.error("沒有資料可供分析")
        sys.exit(1)

    df_partners = data["df_partners"]

    # 顯示資料概況
    logger.info("\n資料概況:")
    logger.info(f"  總記錄數: {data['total_records']:,}")
    logger.info(f"  唯一 mapping_id 數: {len(df_partners):,}")
    logger.info(f"  唯一 cid 數: {df_partners.select('cid').unique().height:,}")

    # 獲取所有唯一的 partner 值（使用 Polars 操作）
    all_partners = (
        df_partners
        .select(pl.col("partners").explode().unique())
        .to_series()
        .sort()
        .to_list()
    )
    logger.info(f"  唯一 partner 列表: {all_partners}")

    # 分析重疊
    results = analyze_partner_overlap(df_partners)

    # 列印結果
    print_results(results)


if __name__ == "__main__":
    main()
