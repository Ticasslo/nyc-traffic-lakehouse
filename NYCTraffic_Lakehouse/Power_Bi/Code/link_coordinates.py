import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Azure Blob config
CONNECTION_STRING = """"""
CONTAINER_NAME = "nyc-traffic-lakehouse"

# Tìm file link_coordinates.parquet
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service.get_container_client(CONTAINER_NAME)

matched_blobs = []

for blob in container_client.list_blobs():
    name = blob.name

    if "link_coordinates" in name and name.endswith(".parquet"):
        matched_blobs.append(name)

if len(matched_blobs) == 0:
    df_coords = pd.DataFrame({
        "error": ["Không tìm thấy file link_coordinates.parquet trong Azure Blob"]
    })
else:
    dfs = []

    for blob_name in matched_blobs:
        blob_client = container_client.get_blob_client(blob_name)
        data = blob_client.download_blob().readall()

        df_part = pd.read_parquet(io.BytesIO(data))
        dfs.append(df_part)

    df_coords = pd.concat(dfs, ignore_index=True)

    # Ép kiểu link_id cho dễ join trong Power BI
    if "link_id" in df_coords.columns:
        df_coords["link_id"] = df_coords["link_id"].astype(str)

df_coords
