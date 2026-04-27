import requests
import re
import zipfile
import io
import os
import pandas as pd

STATION_ID = "012068"
OBS_CODE = 136  # daily rainfall
OUTPUT_DIR = "."

def get_p_c_token(session, station_id):
    url = (
        f"http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av"
        f"?p_nccObsCode={OBS_CODE}&p_display_type=dailyDataFile"
        f"&p_startYear=&p_c=&p_stn_num={station_id}"
    )
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    match = re.search(r'p_c=(-?\d+)', resp.text)
    if not match:
        raise RuntimeError("Could not find p_c token on BOM page. The page structure may have changed.")
    return match.group(1)

def download_rainfall(station_id=STATION_ID, output_dir=OUTPUT_DIR):
    station_id = str(station_id).zfill(6)
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    print(f"Fetching token for station {station_id}...")
    p_c = get_p_c_token(session, station_id)
    print(f"  token: {p_c}")

    download_url = (
        f"http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av"
        f"?p_nccObsCode={OBS_CODE}&p_display_type=dailyDataFile"
        f"&p_startYear=&p_c={p_c}&p_stn_num={station_id}"
    )
    print("Downloading data zip...")
    resp = session.get(download_url, timeout=60)
    resp.raise_for_status()

    if resp.headers.get("Content-Type", "").startswith("text/html"):
        raise RuntimeError("Got HTML instead of zip — token may be invalid or station has no data.")

    os.makedirs(output_dir, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        z.extractall(output_dir)
        print(f"Extracted: {csv_files}")

    if not csv_files:
        raise RuntimeError("No CSV found in the downloaded zip.")

    csv_path = os.path.join(output_dir, csv_files[0])
    df = pd.read_csv(csv_path)
    print(f"\nLoaded {len(df)} rows from {csv_files[0]}")
    print(df.head())
    return df, csv_path

if __name__ == "__main__":
    df, path = download_rainfall()
    print(f"\nSaved to: {os.path.abspath(path)}")
