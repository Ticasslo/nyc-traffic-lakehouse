import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Azure Blob config
CONNECTION_STRING = """"""
CONTAINER_NAME = "nyc-traffic-lakehouse"

# Gold prediction path
# Bảng này có predicted_congestion + confidence
GOLD_PREFIX = "gold/predictions/"

# Test trước cho nhẹ. Nếu OK rồi có thể đổi thành None.
MAX_FILES = 100

# Connect Azure Blob
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service.get_container_client(CONTAINER_NAME)

parquet_blobs = []

for blob in container_client.list_blobs(name_starts_with=GOLD_PREFIX):
    name = blob.name

    if name.endswith(".parquet"):
        parquet_blobs.append(name)

parquet_blobs = sorted(parquet_blobs, reverse=True)

if MAX_FILES is not None:
    parquet_blobs = parquet_blobs[:MAX_FILES]

# Read parquet files
if len(parquet_blobs) == 0:
    df_gold = pd.DataFrame({
        "link_id": [],
        "borough": [],
        "link_name": [],
        "timestamp": [],
        "target_time": [],
        "data_as_of": [],
        "hour": [],
        "day_of_week": [],
        "month": [],
        "year": [],
        "is_weekend": [],
        "is_holiday": [],
        "predicted_congestion": [],
        "confidence": [],
        "error": [f"Không tìm thấy parquet trong prefix: {GOLD_PREFIX}"]
    })
else:
    dfs = []

    for blob_name in parquet_blobs:
        blob_client = container_client.get_blob_client(blob_name)
        data = blob_client.download_blob().readall()

        temp_df = pd.read_parquet(io.BytesIO(data))
        dfs.append(temp_df)

    df_gold = pd.concat(dfs, ignore_index=True)



    needed_cols = [
        "link_id",
        "borough",
        "link_name",
        "timestamp",
        "target_time",
        "data_as_of",
        "hour",
        "day_of_week",
        "month",
        "year",
        "is_weekend",
        "is_holiday",
        "predicted_congestion",
        "confidence"
    ]

    # Cột nào chưa có thì tạo rỗng
    for col in needed_cols:
        if col not in df_gold.columns:
            df_gold[col] = pd.NA

    # link_id để dạng Text cho dễ merge với df_coords và df_lookup
    df_gold["link_id"] = df_gold["link_id"].astype(str)

    # Convert timestamp
    df_gold["timestamp"] = pd.to_datetime(
        df_gold["timestamp"],
        errors="coerce"
    )

    # Convert target_time
    df_gold["target_time"] = pd.to_datetime(
        df_gold["target_time"],
        errors="coerce"
    )

    # Nếu data_as_of chưa có thì dùng timestamp
    df_gold["data_as_of"] = pd.to_datetime(
        df_gold["data_as_of"],
        errors="coerce"
    )

    df_gold["data_as_of"] = df_gold["data_as_of"].fillna(df_gold["timestamp"])

    base_time = df_gold["data_as_of"]

    df_gold["hour"] = pd.to_numeric(df_gold["hour"], errors="coerce")
    df_gold["hour"] = df_gold["hour"].fillna(base_time.dt.hour)

    df_gold["day_of_week"] = pd.to_numeric(df_gold["day_of_week"], errors="coerce")
    # Map giống Job B: Python weekday() Monday=0..Sunday=6 → Spark dayofweek() Sunday=1..Saturday=7
    _dow_map = {0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 1}
    df_gold["day_of_week"] = df_gold["day_of_week"].fillna(
        base_time.dt.dayofweek.map(_dow_map)
    )

    df_gold["month"] = pd.to_numeric(df_gold["month"], errors="coerce")
    df_gold["month"] = df_gold["month"].fillna(base_time.dt.month)

    df_gold["year"] = pd.to_numeric(df_gold["year"], errors="coerce")
    df_gold["year"] = df_gold["year"].fillna(base_time.dt.year)

    # Convert predicted_congestion
    df_gold["predicted_congestion"] = pd.to_numeric(
        df_gold["predicted_congestion"],
        errors="coerce"
    )

    # Confidence
    df_gold["confidence"] = pd.to_numeric(
        df_gold["confidence"],
        errors="coerce"
    )

    # Weekend / Holiday
    df_gold["is_weekend"] = pd.to_numeric(
        df_gold["is_weekend"],
        errors="coerce"
    )

    df_gold["is_weekend"] = df_gold["is_weekend"].fillna(
        df_gold["day_of_week"].isin([1, 7]).astype("int")
    )

    df_gold["is_holiday"] = pd.to_numeric(
        df_gold["is_holiday"],
        errors="coerce"
    )

    df_gold["is_holiday"] = df_gold["is_holiday"].fillna(0)

    # Đưa các cột số về kiểu phù hợp
    int_cols = [
        "hour",
        "day_of_week",
        "month",
        "year",
        "is_weekend",
        "is_holiday",
        "predicted_congestion",
    ]

    for col in int_cols:
        df_gold[col] = df_gold[col].astype("Int64")

    df_gold = df_gold.sort_values("timestamp", ascending=False).reset_index(drop=True)
    df_gold["is_latest"] = ~df_gold.duplicated(subset=["link_id"], keep="first")

    ordered_cols = [
        "link_id",
        "borough",
        "link_name",
        "timestamp",
        "target_time",
        "data_as_of",
        "hour",
        "day_of_week",
        "month",
        "year",
        "is_weekend",
        "is_holiday",
        "predicted_congestion",
        "confidence",
        "is_latest"
    ]

    df_gold = df_gold[ordered_cols]

df_gold