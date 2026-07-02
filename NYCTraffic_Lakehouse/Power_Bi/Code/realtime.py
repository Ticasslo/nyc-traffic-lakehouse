import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
import pytz

# Azure Blob config (để load active_link_ids.json)
CONNECTION_STRING = """"""
CONTAINER_NAME = "nyc-traffic-lakehouse"

# NYC DOT real-time feed (linkdata.nyctmc.org) — TSV, <2 phút
# 13 cột: id, Speed, TravelTime, Status, DataAsOf, linkId,
# linkPoints, EncodedPolyLine, EncodedPolyLineLvls, Owner,
# Transcom_id, Borough, linkName
REALTIME_URL = "https://linkdata.nyctmc.org/data/LinkSpeedQuery.txt"

col_names = [
    "id", "Speed", "TravelTime", "Status", "DataAsOf", "linkId",
    "linkPoints", "EncodedPolyLine", "EncodedPolyLineLvls", "Owner",
    "Transcom_id", "Borough", "linkName"
]

needed_cols = ["link_id", "link_name", "borough", "speed", "data_as_of"]

# Load active_link_ids — nếu Azure lỗi thì bỏ qua filter, không crash
active_link_ids = None
try:
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    for blob in container_client.list_blobs(name_starts_with="artifacts/"):
        if "active_link_ids" in blob.name and blob.name.endswith(".json"):
            blob_client = container_client.get_blob_client(blob.name)
            data = blob_client.download_blob().readall()
            import json
            active_link_ids = set(str(x) for x in json.loads(data))
            break
except Exception:
    active_link_ids = None  # Azure lỗi thì vẫn chạy tiếp, chỉ là không filter active link

try:
    df_realtime = pd.read_csv(
        REALTIME_URL,
        sep="\t",
        header=None,
        names=col_names,
        usecols=["linkId", "Speed", "DataAsOf", "Borough", "linkName"],
        encoding="utf-8",
        engine="python",
        on_bad_lines="skip"
    )

    df_realtime = df_realtime.rename(columns={
        "linkId": "link_id",
        "Speed": "speed",
        "DataAsOf": "data_as_of",
        "Borough": "borough",
        "linkName": "link_name"
    })

    df_realtime["link_id"] = df_realtime["link_id"].astype(str)

    # Ép speed về numeric — nếu không ép, cột có thể ở dạng string và so sánh > 0 sẽ lỗi
    df_realtime["speed"] = pd.to_numeric(df_realtime["speed"], errors="coerce")

    df_realtime = df_realtime[(df_realtime["speed"] > 0) & (df_realtime["speed"] < 100)]
    df_realtime["data_as_of"] = pd.to_datetime(df_realtime["data_as_of"], errors="coerce")

    # Feed trả về giờ Eastern nhưng chuỗi gốc không có offset -> parse ra naive datetime.
    # Phải localize về America/New_York trước khi so sánh, không được lấy giờ hệ thống local.
    eastern = pytz.timezone("America/New_York")

    if df_realtime["data_as_of"].dt.tz is None:
        df_realtime["data_as_of"] = df_realtime["data_as_of"].dt.tz_localize(
            eastern, ambiguous="NaT", nonexistent="NaT"
        )
    else:
        df_realtime["data_as_of"] = df_realtime["data_as_of"].dt.tz_convert(eastern)

    # Loại link "chết" — DataAsOf cũ hơn 120 phút, đồng nhất với logic CASE 1 của Job A
    now_ts = pd.Timestamp.now(tz=eastern)
    cutoff = now_ts - pd.Timedelta(minutes=120)
    df_realtime = df_realtime[df_realtime["data_as_of"] >= cutoff]

    if active_link_ids is not None:
        df_realtime = df_realtime[df_realtime["link_id"].isin(active_link_ids)]

    df_realtime = (
        df_realtime
        .sort_values("data_as_of", ascending=False)
        .drop_duplicates(subset=["link_id"], keep="first")
        .reset_index(drop=True)
    )

    df_realtime = df_realtime[needed_cols]

    if len(df_realtime) == 0:
        df_realtime = pd.DataFrame({col: [] for col in needed_cols})

except Exception as e:
    df_realtime = pd.DataFrame({col: [] for col in needed_cols})
    df_realtime["error"] = [f"Lỗi khi gọi real-time feed: {str(e)}"]

df_realtime