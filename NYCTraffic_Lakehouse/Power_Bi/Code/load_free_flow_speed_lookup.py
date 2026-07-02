import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Azure Blob config
CONNECTION_STRING = """"""
CONTAINER_NAME = "nyc-traffic-lakehouse"

# Tìm file free_flow_speed_lookup.parquet
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service.get_container_client(CONTAINER_NAME)

matched_blobs = []

for blob in container_client.list_blobs(name_starts_with="artifacts/"):
    name = blob.name

    if "free_flow_speed_lookup" in name and name.endswith(".parquet"):
        matched_blobs.append(name)

if len(matched_blobs) == 0:
    df_lookup = pd.DataFrame({
        "error": ["Không tìm thấy file free_flow_speed_lookup.parquet trong Azure Blob"]
    })
else:
    dfs = []

    for blob_name in matched_blobs:
        blob_client = container_client.get_blob_client(blob_name)
        data = blob_client.download_blob().readall()

        df_part = pd.read_parquet(io.BytesIO(data))
        dfs.append(df_part)

    df_lookup = pd.concat(dfs, ignore_index=True)

    if "link_id" in df_lookup.columns:
        df_lookup["link_id"] = df_lookup["link_id"].astype(str)
    if "free_flow_speed" in df_lookup.columns:
        df_lookup["free_flow_speed"] = pd.to_numeric(df_lookup["free_flow_speed"], errors="coerce")

df_lookup
