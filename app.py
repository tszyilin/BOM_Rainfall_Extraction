import streamlit as st
import streamlit.components.v1 as components
import requests
import re
import zipfile
import io
import pandas as pd
import plotly.express as px

OBS_CODE = 136  # daily rainfall

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "http://www.bom.gov.au/climate/data/",
}

st.set_page_config(layout="wide")
st.title("BOM Daily Rainfall Downloader")
st.caption("Data sourced from Bureau of Meteorology Climate Data Online")

st.markdown("""
<style>
small.st-emotion-cache-1gulkj5, .st-emotion-cache-1gulkj5 { display: none; }
[data-testid="InputInstructions"] { display: none; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Search")
    with st.form("search_form"):
        station_input = st.text_input("Station ID", value="012068", help="Enter a BOM station number (e.g. 012068)")
        search = st.form_submit_button("Search Rainfall Station", use_container_width=True)
    debug_mode = st.checkbox("Show debug info")
    distribute = st.checkbox("Distribute accumulated rainfall evenly", value=True,
                             help="Splits multi-day accumulated readings across preceding days so missing days aren't overestimated")

    st.divider()
    st.subheader("Export")
    st.markdown("**CSV**")
    export_csv = st.checkbox("Save as CSV", value=True)

    st.markdown("**XLSX sheets to include:**")
    inc_raw    = st.checkbox("Daily Rainfall (raw)", value=True)
    inc_annual = st.checkbox("Annual Summary", value=True)
    inc_pivot  = st.checkbox("Monthly Pivot", value=True)


def make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get("https://www.bom.gov.au/climate/data/", timeout=15)
    return session


def strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s).replace("&deg;", "°").replace("&nbsp;", " ").strip()

def parse_station_info(html: str, debug: bool = False) -> dict:
    # Strip tags → plain text
    plain = re.sub(r"<[^>]+>", " ", html)
    plain = plain.replace("&deg;", "°").replace("&nbsp;", " ").replace("&amp;", "&")
    plain = re.sub(r"\s+", " ", plain)

    # Anchor on "Number: NNNNN" to find the real station info block
    anchor = re.search(r"Number:\s*\d+", plain, re.IGNORECASE)
    if not anchor:
        block = plain
    else:
        # grab 300 chars before the number and 600 after
        start = max(0, anchor.start() - 300)
        end   = min(len(plain), anchor.end() + 600)
        block = plain[start:end]

    if debug:
        st.expander("Debug: station info block (plain text)").code(block)

    STOP = r"(?=\s*(?:Number|Opened|Now|Lat|Lon|Elevation|Station|Details)\s*:)"

    def field(label, default="N/A"):
        m = re.search(rf"{label}\s*:\s*(.*?){STOP}", block, re.IGNORECASE)
        return m.group(1).strip() if m else default

    name   = field("Station")
    number = field("Number")
    opened = field("Opened")
    now    = field("Now")
    lat    = field("Lat")
    lon    = field("Lon")
    elev_m = re.search(r"Elevation\s*:\s*([\d.]+)\s*m", block, re.IGNORECASE)
    elev   = elev_m.group(1) if elev_m else "N/A"

    is_open = ("closed" not in now.lower()) if now != "N/A" else None

    if debug:
        st.expander("Debug: parsed fields").json({
            "name": name, "number": number, "lat": lat, "lon": lon,
            "opened": opened, "now": now, "elevation": elev,
        })

    return {
        "name": name, "number": number,
        "lat": lat, "lon": lon,
        "opened": opened, "now": now,
        "is_open": is_open, "elevation": elev,
    }


def get_download_url_and_html(session, station_id, debug=False):
    url = (
        f"https://www.bom.gov.au/jsp/ncc/cdio/weatherData/av"
        f"?p_nccObsCode={OBS_CODE}&p_display_type=dailyDataFile"
        f"&p_startYear=&p_c=&p_stn_num={station_id}"
    )
    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    if debug:
        idx = resp.text.lower().find("station:")
        snippet = resp.text[max(0, idx-200):idx+1500] if idx != -1 else resp.text[:3000]
        st.expander("Debug: HTML around 'Station:' label").code(snippet)

    match = re.search(
        r'p_display_type=dailyZippedDataFile&amp;p_stn_num=\d+&amp;p_c=(-?\d+)&amp;p_nccObsCode=\d+&amp;p_startYear=(\d+)',
        resp.text,
    )
    if not match:
        if debug:
            st.info(f"All p_c values: {re.findall(r'p_c=([-\\d]+)', resp.text)}")
        raise RuntimeError(
            "Could not find the 'All years of data' link. "
            "Station may not exist or have no rainfall data."
        )

    p_c, start_year = match.group(1), match.group(2)
    download_url = (
        f"https://www.bom.gov.au/jsp/ncc/cdio/weatherData/av"
        f"?p_display_type=dailyZippedDataFile&p_stn_num={station_id}"
        f"&p_c={p_c}&p_nccObsCode={OBS_CODE}&p_startYear={start_year}"
    )
    return download_url, resp.text


@st.cache_data(show_spinner=False)
def fetch_rainfall(station_id: str, debug: bool = False):
    station_id = station_id.strip().zfill(6)
    session = make_session()

    download_url, page_html = get_download_url_and_html(session, station_id, debug=debug)
    station_info = parse_station_info(page_html, debug=debug)

    if debug:
        st.info(f"Download URL: `{download_url}`")

    resp = session.get(download_url, timeout=60)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    if debug:
        st.info(f"Content-Type: `{content_type}` | Size: {len(resp.content):,} bytes")

    if "html" in content_type:
        if debug:
            st.expander("Debug: download response").code(resp.text[:3000])
        raise RuntimeError("BOM returned HTML instead of a zip — blocked or session expired.")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            raise RuntimeError("No CSV found in the downloaded zip.")
        with z.open(csv_files[0]) as f:
            df = pd.read_csv(f)

    return df, station_info


# ── Main ──────────────────────────────────────────────────────────────────────

if search:
    with st.spinner("Fetching data from BOM..."):
        try:
            df, info = fetch_rainfall(station_input, debug=debug_mode)
        except Exception as e:
            st.error(str(e))
            st.stop()

    df.columns = df.columns.str.strip()
    rain_col = next((c for c in df.columns if "rainfall" in c.lower()), None)

    # Parse lat/lon from the scraped info strings e.g. "30.93 ° S" → -30.93
    def parse_coord(s, neg_dir):
        if not s or s == "N/A":
            return None
        num = re.search(r"[\d.]+", s)
        if not num:
            return None
        val = float(num.group())
        if neg_dir.upper() in s.upper():
            val = -val
        return val

    csv_lat = parse_coord(info.get("lat"), "S")
    csv_lon = parse_coord(info.get("lon"), "W")

    # Pre-compute for downloads
    base = None
    annual = None
    pivot = None
    period_col = next((c for c in df.columns if "period" in c.lower()), None)

    if rain_col:
        base = df[["Year", "Month", "Day", rain_col] + ([period_col] if period_col else [])].copy()
        base[rain_col] = pd.to_numeric(base[rain_col], errors="coerce")
        base["Date"] = pd.to_datetime(base[["Year", "Month", "Day"]], errors="coerce")

        if period_col:
            base[period_col] = pd.to_numeric(base[period_col], errors="coerce").fillna(1).clip(lower=1)
            acc_mask = base[period_col] > 1
            base["Accumulated"] = acc_mask

            if distribute:
                # Distribute accumulated readings across preceding days
                base = base.set_index("Date").sort_index()
                rain_series = base[rain_col].copy()
                for date, row in base[acc_mask].iterrows():
                    p = int(row[period_col])
                    r = row[rain_col]
                    if pd.isna(r):
                        continue
                    daily = round(r / p, 1)
                    for i in range(p):
                        d = date - pd.Timedelta(days=i)
                        if d in rain_series.index:
                            rain_series[d] = daily
                base[rain_col] = rain_series
                base = base.reset_index()
                base["Year"]  = base["Date"].dt.year
                base["Month"] = base["Date"].dt.month
        else:
            base["Accumulated"] = False

        # Annual summary
        annual = (
            base.groupby("Year")
            .agg(
                Accumulated_Readings=("Accumulated", "sum"),
                Missing_Days=(rain_col, lambda x: x.isna().sum()),
                Annual_Rainfall_mm=(rain_col, "sum"),
            )
            .reset_index()
        )
        annual["Annual_Rainfall_mm"] = annual["Annual_Rainfall_mm"].round(1)
        annual["Accumulated_Readings"] = annual["Accumulated_Readings"].astype(int)

        monthly = base.groupby(["Year", "Month"])[rain_col].sum().reset_index()
        pivot = monthly.pivot(index="Year", columns="Month", values=rain_col)
        month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                       7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        pivot.rename(columns=month_names, inplace=True)
        pivot = pivot.reindex(columns=list(month_names.values())).round(1)

    # Store in session state so sidebar download buttons survive reruns
    st.session_state["df"]       = df
    st.session_state["info"]     = info
    st.session_state["annual"]   = annual
    st.session_state["pivot"]    = pivot
    st.session_state["rain_col"] = rain_col
    st.session_state["station_id"] = station_input.zfill(6)
    st.session_state["csv_lat"] = csv_lat
    st.session_state["csv_lon"] = csv_lon

# Render from session state (persists after search)
if "df" in st.session_state:
    df       = st.session_state["df"]
    info     = st.session_state["info"]
    annual   = st.session_state["annual"]
    pivot    = st.session_state["pivot"]
    rain_col = st.session_state["rain_col"]
    stn_id   = st.session_state["station_id"]
    csv_lat  = st.session_state.get("csv_lat")
    csv_lon  = st.session_state.get("csv_lon")

    # ── Sidebar downloads ─────────────────────────────────────────────────────
    with st.sidebar:
        st.divider()
        st.subheader("Download")
        if export_csv:
            st.download_button(
                label="Download CSV",
                data=df.to_csv(index=False).encode(),
                file_name=f"bom_rainfall_{stn_id}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        if rain_col and (inc_raw or inc_annual or inc_pivot):
            xlsx_buf = io.BytesIO()
            with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
                if inc_raw:
                    df.to_excel(writer, sheet_name="Daily Rainfall", index=False)
                if inc_annual and annual is not None:
                    annual.to_excel(writer, sheet_name="Annual Summary", index=False)
                if inc_pivot and pivot is not None:
                    pivot.to_excel(writer, sheet_name="Monthly Pivot")
            xlsx_buf.seek(0)
            st.download_button(
                label="Download XLSX",
                data=xlsx_buf,
                file_name=f"bom_rainfall_{stn_id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    # ── Station card ──────────────────────────────────────────────────────────
    dates      = pd.to_datetime(df[["Year", "Month", "Day"]], errors="coerce").dropna()
    start_date = dates.min().strftime("%d %b %Y")
    end_date   = dates.max().strftime("%d %b %Y")

    if info["is_open"] is True:
        badge = '<span style="background:#1a9e3f;color:white;padding:3px 10px;border-radius:12px;font-size:0.85em">● Open</span>'
    elif info["is_open"] is False:
        badge = '<span style="background:#c0392b;color:white;padding:3px 10px;border-radius:12px;font-size:0.85em">● Closed</span>'
    else:
        badge = '<span style="background:#888;color:white;padding:3px 10px;border-radius:12px;font-size:0.85em">Unknown</span>'

    col_info, col_map = st.columns([1, 1])

    with col_info:
        st.markdown(f"""
<div style="background:#f8f9fa;border:1px solid #dee2e6;border-radius:10px;padding:20px 28px;height:100%">
  <div style="font-size:1.4em;font-weight:700;margin-bottom:4px">{info['name']} &nbsp; {badge}</div>
  <div style="color:#555;font-size:0.95em;margin-bottom:14px">Station {info['number']}</div>
  <div style="display:flex;gap:32px;flex-wrap:wrap;font-size:0.95em">
    <div><span style="color:#888">Latitude</span><br><b>{info['lat']}</b></div>
    <div><span style="color:#888">Longitude</span><br><b>{info['lon']}</b></div>
    <div><span style="color:#888">Elevation</span><br><b>{info['elevation']} m</b></div>
    <div><span style="color:#888">Opened</span><br><b>{info['opened']}</b></div>
    <div><span style="color:#888">Status</span><br><b>{info['now']}</b></div>
    <div><span style="color:#888">Data Range</span><br><b>{start_date} – {end_date}</b></div>
  </div>
</div>
""", unsafe_allow_html=True)

    with col_map:
        if csv_lat is not None and csv_lon is not None:
            st.map(
                pd.DataFrame({"lat": [csv_lat], "lon": [csv_lon]}),
                zoom=7,
                height=380,
            )
        else:
            st.info("No coordinates available for this station.")

    st.success(f"Loaded {len(df):,} rows")

    st.subheader("Preview")
    st.dataframe(df, use_container_width=True)

    if rain_col:
        st.subheader("Daily Rainfall Chart")
        plot_df = base[["Date", rain_col, "Accumulated"]].dropna(subset=["Date", rain_col]).copy()
        plot_df["Type"] = plot_df["Accumulated"].map(
            {True: "Accumulated (distributed)" if distribute else "Accumulated", False: "Daily"}
        )
        fig = px.line(plot_df, x="Date", y=rain_col,
                      color="Type",
                      color_discrete_map={
                          "Daily": "#1f77b4",
                          "Accumulated (distributed)": "#ff7f0e",
                          "Accumulated": "#ff7f0e",
                      },
                      labels={"Date": "Year", rain_col: "Rainfall (mm)", "Type": ""})
        fig.update_traces(
            hovertemplate="<b>%{x|%d %b %Y}</b><br>Rainfall: %{y} mm<extra></extra>"
        )
        fig.update_xaxes(tickformat="%d %b %Y")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Annual Summary")
        st.dataframe(annual, use_container_width=True, hide_index=True)

        st.subheader("Monthly Rainfall Pivot (mm)")
        st.dataframe(pivot, use_container_width=True)
