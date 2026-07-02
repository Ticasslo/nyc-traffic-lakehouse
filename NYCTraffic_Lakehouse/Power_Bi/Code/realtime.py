import pandas as pd
import requests

# NYC Open Data - DOT Traffic Speeds
APP_TOKEN = ""

API_URL = ""

headers = {
    "X-App-Token": APP_TOKEN
}

params = {
    "$select": "link_id,link_name,borough,speed,travel_time,data_as_of",
    "$order": "data_as_of DESC",
    "$limit": 50000
}

response = requests.get(
    API_URL,
    headers=headers,
    params=params,
    timeout=60
)

response.raise_for_status()

data = response.json()

df_realtime = pd.DataFrame(data)

if len(df_realtime) > 0:
    df_realtime["link_id"] = df_realtime["link_id"].astype(str)
    df_realtime["speed"] = pd.to_numeric(df_realtime["speed"], errors="coerce")
    df_realtime["travel_time"] = pd.to_numeric(df_realtime["travel_time"], errors="coerce")
    df_realtime["data_as_of"] = pd.to_datetime(df_realtime["data_as_of"], errors="coerce")

    # Lấy bản ghi mới nhất cho mỗi link_id
    df_realtime = (
        df_realtime
        .sort_values("data_as_of", ascending=False)
        .drop_duplicates(subset=["link_id"], keep="first")
        .reset_index(drop=True)
    )

df_realtime
