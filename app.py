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

st.set_page_config(page_title="BOM Rainfall Downloader", layout="wide")
st.session_state.setdefault("station_id_input", "012068")
st.title("BOM Rainfall Downloader")
st.caption("Data sourced from Bureau of Meteorology Climate Data Online")

st.markdown("""
<style>
small.st-emotion-cache-1gulkj5, .st-emotion-cache-1gulkj5 { display: none; }
[data-testid="InputInstructions"] { display: none; }
/* Hide the built-in dataframe download button (multiple selector variants) */
[data-testid="stElementToolbarButton"][title="Download as CSV"] { display: none; }
[data-testid="stElementToolbarButton"][aria-label="Download as CSV"] { display: none; }
[data-testid="stElementToolbarButton"]:has(svg path[d*="M4 16v1a3"]) { display: none; }
button[title="Download as CSV"] { display: none; }
button[aria-label="Download as CSV"] { display: none; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Options")
    debug_mode = st.checkbox("Show debug info")
    distribute = st.toggle("Distribute accumulated rainfall evenly", value=True,
                           help="Splits multi-day accumulated readings across preceding days so missing days aren't overestimated")
    show_monthly_bar = st.toggle("Show monthly rainfall bar chart", value=True)

    st.divider()
    st.subheader("Export")
    st.markdown("**CSV**")
    export_csv = st.checkbox("Save as CSV (raw)", value=True)

    st.markdown("**XLSX sheets to include:**")
    inc_raw         = st.checkbox("Daily Rainfall (raw)", value=True)
    inc_dist        = st.checkbox("Daily Rainfall (distributed)", value=False) if distribute else False
    inc_annual      = st.checkbox("Annual Summary", value=True)
    inc_pivot            = st.checkbox("Monthly Pivot", value=True)
    inc_miss_pivot_raw   = st.checkbox("Missing Days Pivot (raw)", value=True)
    inc_miss_pivot_dist  = st.checkbox("Missing Days Pivot (distributed)", value=False) if distribute else False


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


@st.cache_data(show_spinner=False)
def load_stations():
    try:
        df = pd.read_parquet("data/stations.parquet")
        df["SITE_ID_STR"] = df["SITE_ID"].astype(int).astype(str).str.zfill(6)
        return df
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def geocode_postcode(postcode: str):
    """Return (lat, lon, display_name) or None. Uses Nominatim via requests."""
    resp = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": f"{postcode}, Australia", "format": "json", "limit": 1, "countrycodes": "au"},
        headers={"User-Agent": "BOM-Rainfall-App/1.0 (github.com/tszyilin/BOM_Rainfall_Extraction)"},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json()
    if not results:
        return None
    r = results[0]
    return float(r["lat"]), float(r["lon"]), r.get("display_name", postcode)


def _show_station_card(cd):
    sel_id    = str(cd[0]).strip().zfill(6)
    sel_name  = str(cd[1]).strip()
    sel_start = str(cd[2]).strip() if len(cd) > 2 else "?"
    sel_end   = str(cd[3]).strip() if len(cd) > 3 else "?"
    sel_pct   = str(cd[4]).strip() if len(cd) > 4 else "?"
    try:
        total_days = (int(sel_end) - int(sel_start) + 1) * 365
        recorded   = round(total_days * float(sel_pct) / 100)
        pct_label  = f"{sel_pct}% ({recorded:,} / {total_days:,} days recorded)"
    except Exception:
        pct_label  = f"{sel_pct}%"
    c1, c2, c3 = st.columns([3, 0.7, 1.3])
    with c1:
        st.info(
            f"**{sel_name}**  \n"
            f"Station ID: `{sel_id}` &nbsp;|&nbsp; "
            f"Record: {sel_start}–{sel_end} &nbsp;|&nbsp; "
            f"Completeness: {pct_label}"
        )
    with c2:
        st.write("")
        st.write("")
        if st.button("Use this Station", type="primary", use_container_width=True,
                     key=f"use_stn_{sel_id}"):
            st.session_state["_pending_station_id"] = sel_id
            st.session_state["_auto_search"] = True
            st.rerun()


def _completeness_color(pct):
    if pct >= 90:
        return "#2ecc71"
    if pct >= 70:
        return "#f39c12"
    return "#e74c3c"


def _render_station_map(disp, center, zoom, map_key, postcode_marker=None):
    """Render an interactive folium station map; return [id, name, start, end, pct] or None."""
    import folium
    from folium.plugins import MarkerCluster, FastMarkerCluster
    from streamlit_folium import st_folium

    m = folium.Map(location=[center["lat"], center["lon"]], zoom_start=zoom, tiles="OpenStreetMap")

    MAX_INTERACTIVE = 500
    if len(disp) > MAX_INTERACTIVE:
        color_callback = """
            function(row) {
                var pct = row[2];
                var color = pct >= 90 ? '#2ecc71' : pct >= 70 ? '#f39c12' : '#e74c3c';
                return L.circleMarker(
                    new L.LatLng(row[0], row[1]),
                    {radius: 5, color: 'white', weight: 1,
                     fillColor: color, fillOpacity: 0.85}
                );
            }
        """
        FastMarkerCluster(
            data=disp[["LAT", "LONG", "PC_COMPLET"]].values.tolist(),
            callback=color_callback,
        ).add_to(m)
        st.caption(f"{len(disp):,} stations shown — refine your search to fewer than 500 to enable station selection.")
        selection_enabled = False
    else:
        mc = MarkerCluster().add_to(m)
        for _, row in disp.iterrows():
            color = _completeness_color(row["PC_COMPLET"])
            popup_html = (
                f"<b>{row['SITE_ID_STR']}</b><br>"
                f"{row['SITE_NAME']}<br>"
                f"{row['START_Y']}–{row['END_Y']}"
            )
            folium.CircleMarker(
                location=[row["LAT"], row["LONG"]],
                radius=7,
                color="white", weight=1,
                fill=True, fill_color=color, fill_opacity=0.9,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{row['SITE_ID_STR']} | {row['SITE_NAME']} | {row['START_Y']}–{row['END_Y']}",
            ).add_to(mc)
        selection_enabled = True

    if postcode_marker:
        folium.Marker(
            location=[postcode_marker[0], postcode_marker[1]],
            tooltip=postcode_marker[2],
            icon=folium.Icon(color="red", icon="map-marker", prefix="fa"),
        ).add_to(m)

    # last_object_clicked fires on marker click (reliable through MarkerCluster).
    # Excluding bounds/zoom/center prevents scroll/pan from triggering reruns and snapping the map back.
    map_data = st_folium(m, width="100%", height=450, key=map_key,
                         returned_objects=["last_object_clicked"])

    if selection_enabled and map_data:
        clicked = map_data.get("last_object_clicked")
        if clicked and isinstance(clicked, dict):
            import numpy as np
            click_lat = clicked.get("lat")
            click_lng = clicked.get("lng")
            if click_lat is not None and click_lng is not None:
                dists = haversine_km(click_lat, click_lng,
                                     disp["LAT"].values, disp["LONG"].values)
                nearest_idx = int(np.argmin(dists))
                if dists[nearest_idx] < 0.5:
                    r = disp.iloc[nearest_idx]
                    start = str(int(r["START_Y"])) if pd.notna(r["START_Y"]) else "?"
                    end   = str(int(r["END_Y"]))   if pd.notna(r["END_Y"])   else "?"
                    return [r["SITE_ID_STR"], r["SITE_NAME"], start, end, str(r["PC_COMPLET"])]
    return None




def haversine_km(lat1, lon1, lat2_arr, lon2_arr):
    """Vectorised haversine distance (km) from a point to an array of points."""
    import numpy as np
    R = 6371.0
    dlat = np.radians(lat2_arr - lat1)
    dlon = np.radians(lon2_arr - lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2_arr)) * np.sin(dlon / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


# ── Search Tabs ───────────────────────────────────────────────────────────────
search = False

# Apply pending station ID from map selection before the widget renders
if "_pending_station_id" in st.session_state:
    st.session_state["station_id_input"] = st.session_state.pop("_pending_station_id")

station_input = st.session_state.get("station_id_input", "012068")

if st.session_state.pop("_auto_search", False):
    search = True

tab_sid, tab_name, tab_loc, tab_pc, tab_ll = st.tabs([
    "Search by Station ID",
    "Search by Station Name",
    "Search by Location",
    "Search by Postcode",
    "Search by Lat / Long",
])

with tab_sid:
    col_in, col_btn, col_pad = st.columns([1, 0.3, 2.7])
    with col_in:
        station_input = st.text_input(
            "Station ID",
            key="station_id_input",
            placeholder="e.g. 012068",
            label_visibility="collapsed",
            help="Enter a BOM station number",
        )
    with col_btn:
        if st.button("Search", type="primary", use_container_width=True, key="search_btn_sid"):
            search = True

with tab_name:
    stations_df = load_stations()
    if stations_df is not None:
        col_nq, col_na = st.columns([3, 1])
        with col_nq:
            name_q = st.text_input("Station name", placeholder="e.g. Brisbane, Cairns", key="map_name_q")
        with col_na:
            st.write("")
            only_active_n = st.checkbox("Active only", key="map_only_active_n")
        if name_q.strip():
            disp_n = stations_df.copy()
            disp_n = disp_n[disp_n["SITE_NAME"].str.contains(name_q.strip(), case=False, na=False)]
            if only_active_n:
                max_yr = int(pd.to_numeric(stations_df["END_Y"], errors="coerce").max())
                disp_n = disp_n[pd.to_numeric(disp_n["END_Y"], errors="coerce") >= max_yr - 1]
            st.caption(f"{len(disp_n):,} stations — click to select")
            cd = _render_station_map(disp_n, {"lat": -25, "lon": 133}, 3, "map_name")
            if cd:
                _show_station_card(cd)
        else:
            st.info("Enter a station name above to see matching stations on the map.")

with tab_loc:
    stations_df = load_stations()
    if stations_df is not None:
        col_lq, col_lkm, col_la = st.columns([2, 2, 1])
        with col_lq:
            loc_q = st.text_input(
                "Location name",
                placeholder="e.g. Eastwood, Brisbane CBD, Darwin",
                key="map_loc_q",
            )
        with col_lkm:
            loc_radius = st.slider(
                "Radius (km)", min_value=5, max_value=300, value=50,
                step=5, key="map_loc_radius",
            )
        with col_la:
            st.write("")
            only_active_loc = st.checkbox("Active only", key="map_only_active_loc")
        disp_loc = stations_df.copy()
        if only_active_loc:
            max_yr = int(pd.to_numeric(stations_df["END_Y"], errors="coerce").max())
            disp_loc = disp_loc[pd.to_numeric(disp_loc["END_Y"], errors="coerce") >= max_yr - 1]
        loc_marker = None
        map_c_loc, map_z_loc = {"lat": -25, "lon": 133}, 3
        if loc_q.strip():
            try:
                with st.spinner(f"Looking up '{loc_q}'..."):
                    geo_loc = geocode_postcode(loc_q.strip())
            except Exception:
                st.warning("Location lookup failed — the geocoding service may be temporarily unavailable. Try again shortly.")
                geo_loc = None
            if geo_loc is None:
                st.warning(f"Location '{loc_q}' not found. Try a more specific name.")
            else:
                loc_lat, loc_lon, loc_name = geo_loc
                dists_loc = haversine_km(loc_lat, loc_lon, disp_loc["LAT"].values, disp_loc["LONG"].values)
                disp_loc = disp_loc[dists_loc <= loc_radius].copy()
                map_c_loc = {"lat": loc_lat, "lon": loc_lon}
                map_z_loc = max(3, min(10, int(11 - loc_radius / 30)))
                loc_marker = (loc_lat, loc_lon, loc_name)
                st.info(f"📍 **{loc_name}**")
        if loc_marker:
            st.caption(f"{len(disp_loc):,} stations within {loc_radius} km — click to select")
            cd = _render_station_map(disp_loc, map_c_loc, map_z_loc, "map_loc", postcode_marker=loc_marker)
            if cd:
                _show_station_card(cd)
        else:
            st.info("Enter a location name above to see nearby stations on the map.")

with tab_pc:
    stations_df = load_stations()
    if stations_df is not None:
        col_pc, col_km, col_pa = st.columns([2, 2, 1])
        with col_pc:
            postcode_q = st.text_input("Australian postcode", placeholder="e.g. 4000",
                                       max_chars=4, key="map_postcode_q")
        with col_km:
            radius_km = st.slider("Radius (km)", min_value=5, max_value=300, value=50,
                                  step=5, key="map_radius_km")
        with col_pa:
            st.write("")
            only_active_p = st.checkbox("Active only", key="map_only_active_p")
        disp_p = stations_df.copy()
        if only_active_p:
            max_yr = int(pd.to_numeric(stations_df["END_Y"], errors="coerce").max())
            disp_p = disp_p[pd.to_numeric(disp_p["END_Y"], errors="coerce") >= max_yr - 1]
        pc_marker = None
        map_c, map_z = {"lat": -25, "lon": 133}, 3
        if postcode_q.strip() and len(postcode_q.strip()) == 4 and postcode_q.strip().isdigit():
            try:
                with st.spinner(f"Looking up postcode {postcode_q}..."):
                    geo = geocode_postcode(postcode_q.strip())
            except Exception:
                st.warning("Postcode lookup failed — the geocoding service may be temporarily unavailable. Try again shortly.")
                geo = None
            if geo is None:
                st.warning(f"Postcode {postcode_q} not found.")
            else:
                pc_lat, pc_lon, pc_name = geo
                dists = haversine_km(pc_lat, pc_lon, disp_p["LAT"].values, disp_p["LONG"].values)
                disp_p = disp_p[dists <= radius_km].copy()
                map_c = {"lat": pc_lat, "lon": pc_lon}
                map_z = max(3, min(10, int(11 - radius_km / 30)))
                pc_marker = (pc_lat, pc_lon, pc_name)
                st.info(f"📍 **{pc_name}**")
        if pc_marker:
            st.caption(f"{len(disp_p):,} stations within {radius_km} km — click to select")
            cd = _render_station_map(disp_p, map_c, map_z, "map_pc", postcode_marker=pc_marker)
            if cd:
                _show_station_card(cd)
        else:
            st.info("Enter a postcode above to see nearby stations on the map.")

with tab_ll:
    stations_df = load_stations()
    if stations_df is not None:
        col_lat, col_lon, col_r, col_al = st.columns([2, 2, 2, 1])
        with col_lat:
            ll_lat = st.number_input("Latitude", value=-33.87, min_value=-44.0,
                                     max_value=-10.0, step=0.01, format="%.4f", key="map_ll_lat")
        with col_lon:
            ll_lon = st.number_input("Longitude", value=151.21, min_value=113.0,
                                     max_value=154.0, step=0.01, format="%.4f", key="map_ll_lon")
        with col_r:
            ll_radius = st.slider("Radius (km)", min_value=5, max_value=300, value=50,
                                  step=5, key="map_ll_radius")
        with col_al:
            st.write("")
            only_active_l = st.checkbox("Active only", key="map_only_active_l")
        ll_search = st.button("Search", type="primary", key="ll_search_btn")
        if ll_search:
            st.session_state["ll_searched"] = True
        if st.session_state.get("ll_searched"):
            disp_l = stations_df.copy()
            if only_active_l:
                max_yr = int(pd.to_numeric(stations_df["END_Y"], errors="coerce").max())
                disp_l = disp_l[pd.to_numeric(disp_l["END_Y"], errors="coerce") >= max_yr - 1]
            dists_l = haversine_km(ll_lat, ll_lon, disp_l["LAT"].values, disp_l["LONG"].values)
            disp_l = disp_l[dists_l <= ll_radius].copy()
            ll_marker = (ll_lat, ll_lon, f"{ll_lat:.4f}, {ll_lon:.4f}")
            st.caption(f"{len(disp_l):,} stations within {ll_radius} km — click to select")
            cd = _render_station_map(disp_l, {"lat": ll_lat, "lon": ll_lon},
                                     max(3, min(10, int(11 - ll_radius / 30))),
                                     "map_ll", postcode_marker=ll_marker)
            if cd:
                _show_station_card(cd)
        else:
            st.info("Enter coordinates and click Search to see nearby stations on the map.")


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

    # Store only raw data — base/annual/pivot computed at render time
    st.session_state["df"]         = df
    st.session_state["info"]       = info
    st.session_state["rain_col"]   = rain_col
    st.session_state["station_id"] = station_input.zfill(6)
    st.session_state["csv_lat"]    = csv_lat
    st.session_state["csv_lon"]    = csv_lon


def build_base(df, rain_col, distribute):
    """Build working dataframe, optionally distributing accumulated readings."""
    period_col = next((c for c in df.columns if "period" in c.lower()), None)
    cols = ["Year", "Month", "Day", rain_col] + ([period_col] if period_col else [])
    base = df[cols].copy()
    base[rain_col] = pd.to_numeric(base[rain_col], errors="coerce")
    base["Date"] = pd.to_datetime(base[["Year", "Month", "Day"]], errors="coerce")

    if period_col:
        base[period_col] = pd.to_numeric(base[period_col], errors="coerce").fillna(1).clip(lower=1)
        base["Accumulated"] = base[period_col] > 1

        if distribute:
            base = base.set_index("Date").sort_index()
            rain_series = base[rain_col].copy()
            distributed_dates = set()
            for date, row in base[base[period_col] > 1].iterrows():
                p = int(row[period_col])
                r = row[rain_col]
                if pd.isna(r):
                    continue
                daily = round(r / p, 1)
                for i in range(p):
                    d = date - pd.Timedelta(days=i)
                    if d in rain_series.index:
                        rain_series[d] = daily
                        distributed_dates.add(d)
            base[rain_col] = rain_series
            base["Accumulated"] = base.index.isin(distributed_dates)
            base = base.reset_index()
            base["Year"]  = base["Date"].dt.year
            base["Month"] = base["Date"].dt.month
    else:
        period_col = None
        base["Accumulated"] = False

    # Raw missing days (before any distribution) — NaN count from raw df
    _raw = df[["Year", rain_col]].copy()
    _raw[rain_col] = pd.to_numeric(_raw[rain_col], errors="coerce")
    raw_missing = _raw.groupby("Year")[rain_col].apply(lambda x: int(x.isna().sum())).reset_index()
    raw_missing.columns = ["Year", "Missing Days (Before)"]

    # Annual summary
    annual = (
        base.groupby("Year")
        .agg(
            Accumulated_Readings=("Accumulated", "sum"),
            Annual_Rainfall_mm=(rain_col, "sum"),
        )
        .reset_index()
    )
    annual["Annual_Rainfall_mm"] = annual["Annual_Rainfall_mm"].round(1)
    annual["Accumulated_Readings"] = annual["Accumulated_Readings"].astype(int)

    if distribute:
        # After distribution: NaN count in distributed base
        _after = base.groupby("Year")[rain_col].apply(lambda x: int(x.isna().sum())).reset_index()
        _after.columns = ["Year", "Missing Days (After)"]
        after_missing = _after
        annual = annual.merge(raw_missing, on="Year", how="left")
        annual = annual.merge(after_missing, on="Year", how="left")
        annual = annual[["Year", "Missing Days (Before)", "Missing Days (After)", "Annual_Rainfall_mm"]]
    else:
        # Without distribution: subtract covered days from raw NaN count
        if period_col:
            covered = (
                base[base[period_col] > 1]
                .groupby("Year")[period_col]
                .apply(lambda x: int((x - 1).sum()))
                .reset_index()
            )
            covered.columns = ["Year", "Covered_Days"]
            raw_missing = raw_missing.merge(covered, on="Year", how="left")
            raw_missing["Covered_Days"] = raw_missing["Covered_Days"].fillna(0).astype(int)
            raw_missing["Missing_Days"] = (
                raw_missing["Missing Days (Before)"] - raw_missing["Covered_Days"]
            ).clip(lower=0)
            raw_missing = raw_missing[["Year", "Missing_Days"]]
        else:
            raw_missing = raw_missing.rename(columns={"Missing Days (Before)": "Missing_Days"})
        annual = annual.merge(raw_missing, on="Year", how="left")
        annual = annual[["Year", "Missing_Days", "Annual_Rainfall_mm"]]

    month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                   7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    monthly = base.groupby(["Year", "Month"])[rain_col].sum().reset_index()
    pivot = monthly.pivot(index="Year", columns="Month", values=rain_col)
    pivot.rename(columns=month_names, inplace=True)
    pivot = pivot.reindex(columns=list(month_names.values())).round(1)

    # Missing days pivot (before distribution — from raw df)
    raw_df_num = df[["Year", "Month", rain_col]].copy()
    raw_df_num[rain_col] = pd.to_numeric(raw_df_num[rain_col], errors="coerce")
    _mb = raw_df_num.groupby(["Year", "Month"])[rain_col].apply(lambda x: int(x.isna().sum())).reset_index()
    _mb.columns = ["Year", "Month", "Missing"]
    miss_pivot_before = _mb.pivot(index="Year", columns="Month", values="Missing")
    miss_pivot_before.rename(columns=month_names, inplace=True)
    miss_pivot_before = miss_pivot_before.reindex(columns=list(month_names.values()))

    # Missing days pivot (after distribution — from distributed base)
    _ma = base.groupby(["Year", "Month"])[rain_col].apply(lambda x: int(x.isna().sum())).reset_index()
    _ma.columns = ["Year", "Month", "Missing"]
    miss_pivot_after = _ma.pivot(index="Year", columns="Month", values="Missing")
    miss_pivot_after.rename(columns=month_names, inplace=True)
    miss_pivot_after = miss_pivot_after.reindex(columns=list(month_names.values()))

    return base, annual, pivot, miss_pivot_before, miss_pivot_after


# Render from session state (persists after search)
if "df" in st.session_state:
    df       = st.session_state["df"]
    info     = st.session_state["info"]
    rain_col = st.session_state["rain_col"]
    stn_id   = st.session_state["station_id"]
    csv_lat  = st.session_state.get("csv_lat")
    csv_lon  = st.session_state.get("csv_lon")

    # Recompute base/annual/pivot live based on distribute toggle
    if rain_col:
        base, annual, pivot, miss_pivot_before, miss_pivot_after = build_base(df, rain_col, distribute)
    else:
        base = annual = pivot = miss_pivot_before = miss_pivot_after = None

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
        if rain_col and (inc_raw or inc_dist or inc_annual or inc_pivot or inc_miss_pivot_raw or inc_miss_pivot_dist):
            xlsx_buf = io.BytesIO()
            with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
                if inc_raw:
                    df.to_excel(writer, sheet_name="Daily Rainfall", index=False)
                if inc_dist and base is not None:
                    dist_base, _, _, _, _ = build_base(df, rain_col, distribute=True)
                    dist_base.to_excel(writer, sheet_name="Daily Rainfall (Dist)", index=False)
                if inc_annual and annual is not None:
                    annual.to_excel(writer, sheet_name="Annual Summary", index=False)
                if inc_pivot and pivot is not None:
                    pivot.to_excel(writer, sheet_name="Monthly Pivot")
                if inc_miss_pivot_raw and miss_pivot_before is not None:
                    miss_pivot_before.to_excel(writer, sheet_name="Missing Days (Raw)")
                if inc_miss_pivot_dist and miss_pivot_after is not None:
                    miss_pivot_after.to_excel(writer, sheet_name="Missing Days (Distributed)")
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
<div style="background:var(--secondary-background-color);border:1px solid var(--border-color, #dee2e6);border-radius:10px;padding:20px 28px;height:100%">
  <div style="font-size:1.4em;font-weight:700;margin-bottom:4px;color:var(--text-color)">{info['name']} &nbsp; {badge}</div>
  <div style="color:var(--text-color);opacity:0.6;font-size:0.95em;margin-bottom:14px">Station {info['number']}</div>
  <div style="display:flex;gap:32px;flex-wrap:wrap;font-size:0.95em">
    <div><span style="color:var(--text-color);opacity:0.6">Latitude</span><br><b style="color:var(--text-color)">{info['lat']}</b></div>
    <div><span style="color:var(--text-color);opacity:0.6">Longitude</span><br><b style="color:var(--text-color)">{info['lon']}</b></div>
    <div><span style="color:var(--text-color);opacity:0.6">Elevation</span><br><b style="color:var(--text-color)">{info['elevation']} m</b></div>
    <div><span style="color:var(--text-color);opacity:0.6">Opened</span><br><b style="color:var(--text-color)">{info['opened']}</b></div>
    <div><span style="color:var(--text-color);opacity:0.6">Status</span><br><b style="color:var(--text-color)">{info['now']}</b></div>
    <div><span style="color:var(--text-color);opacity:0.6">Data Range</span><br><b style="color:var(--text-color)">{start_date} – {end_date}</b></div>
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
    if distribute and base is not None:
        tab_raw, tab_dist = st.tabs(["Raw Data", "Distributed Data"])
        with tab_raw:
            st.dataframe(df, use_container_width=True)
        with tab_dist:
            st.dataframe(base, use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)

    if rain_col:
        st.subheader("Daily Rainfall Chart")
        plot_df = base[["Date", rain_col]].dropna(subset=["Date", rain_col]).copy()
        fig = px.line(plot_df, x="Date", y=rain_col,
                      labels={"Date": "Year", rain_col: "Rainfall (mm)"})
        fig.update_traces(
            hovertemplate="<b>%{x|%d %b %Y}</b><br>Rainfall: %{y} mm<extra></extra>"
        )
        fig.update_xaxes(tickformat="%d %b %Y")
        st.plotly_chart(fig, use_container_width=True)

        if show_monthly_bar:
            month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                           7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
            all_years = sorted(base["Year"].dropna().unique().astype(int).tolist())

            import plotly.graph_objects as go

            # ── Chart mode selector ───────────────────────────────────────────
            chart_mode = st.radio(
                "View",
                ["Monthly summary for a year", "Single month across all years"],
                horizontal=True,
                key="bar_chart_mode",
            )

            if chart_mode == "Monthly summary for a year":
                st.subheader("Monthly Rainfall — Single Year")
                col_y1, col_y2 = st.columns(2)
                with col_y1:
                    sel_year = st.selectbox("Select year", options=all_years,
                                            index=len(all_years) - 1, key="bar_sel_year")
                with col_y2:
                    st.markdown("**Show bars**")
                    sw_sel_year   = st.toggle("Selected year",          value=True,  key="sw_sel_year")
                    sw_mean_all   = st.toggle("Mean (all years)",       value=False, key="sw_mean_all")
                    sw_median_all = st.toggle("Median (all years)",     value=False, key="sw_median_all")
                    show_bars = (
                        (["Selected year"]          if sw_sel_year   else []) +
                        (["Mean (all years)"]       if sw_mean_all   else []) +
                        (["Median (all years)"]     if sw_median_all else [])
                    )
                bar_base = base[base["Year"] == sel_year].copy()

                # Monthly totals for selected year
                monthly_tot = (bar_base.groupby("Month")[rain_col]
                               .sum().round(1).reset_index()
                               .rename(columns={rain_col: "Rainfall_mm"}))


                # Mean & Median across ALL years per month
                all_yr_monthly = (base.groupby(["Year", "Month"])[rain_col]
                                  .sum().reset_index())
                mean_med = (all_yr_monthly.groupby("Month")["Rainfall_mm" if "Rainfall_mm" in all_yr_monthly.columns else rain_col]
                            .agg(Mean="mean", Median="median").reset_index())
                # fix column name after groupby sum
                all_yr_monthly2 = (base.groupby(["Year", "Month"])[rain_col]
                                   .sum().rename("Rainfall_mm").reset_index())
                mean_med = (all_yr_monthly2.groupby("Month")["Rainfall_mm"]
                            .agg(Mean="mean", Median="median").reset_index())
                mean_med["Mean"]   = mean_med["Mean"].round(1)
                mean_med["Median"] = mean_med["Median"].round(1)

                # Missing days (raw) for selected year
                raw_yr = df[["Year", "Month", rain_col]].copy()
                raw_yr[rain_col] = pd.to_numeric(raw_yr[rain_col], errors="coerce")
                raw_yr = raw_yr[raw_yr["Year"] == sel_year]
                miss_raw = (raw_yr.groupby("Month")[rain_col]
                            .apply(lambda x: int(x.isna().sum())).reset_index()
                            .rename(columns={rain_col: "Missing_Raw"}))

                # Missing days (after distribute) for selected year
                miss_dist = (bar_base.groupby("Month")[rain_col]
                             .apply(lambda x: int(x.isna().sum())).reset_index()
                             .rename(columns={rain_col: "Missing_Dist"}))

                agg = monthly_tot.merge(mean_med, on="Month", how="left")

                agg = agg.merge(miss_raw, on="Month", how="left")
                agg = agg.merge(miss_dist, on="Month", how="left")
                agg["Month_Name"] = pd.Categorical(
                    agg["Month"].map(month_names),
                    categories=list(month_names.values()), ordered=True)
                agg = agg.sort_values("Month_Name")

                if distribute:
                    miss_label = "Missing (raw): %{customdata[0]}<br>Missing (after dist): %{customdata[1]}"
                else:
                    miss_label = "Missing days: %{customdata[0]}"

                fig_bar = go.Figure()
                if "Selected year" in show_bars:
                    fig_bar.add_trace(go.Bar(
                        name=str(sel_year),
                        x=agg["Month_Name"].tolist(),
                        y=agg["Rainfall_mm"].tolist(),
                        customdata=agg[["Missing_Raw", "Missing_Dist"]].values,
                        hovertemplate=(
                            f"<b>%{{x}} {sel_year}</b><br>Rainfall: %{{y}} mm<br>" +
                            miss_label + "<extra></extra>"
                        ),
                        text=agg["Missing_Raw"].apply(lambda v: f"⚠ {int(v)}" if v > 0 else ""),
                        textposition="outside",
                    ))
                if "Mean (all years)" in show_bars:
                    fig_bar.add_trace(go.Bar(
                        name="Mean (all years)",
                        x=agg["Month_Name"].tolist(),
                        y=agg["Mean"].tolist(),
                        hovertemplate="<b>%{x} — Mean</b><br>Rainfall: %{y} mm<extra></extra>",
                    ))
                if "Median (all years)" in show_bars:
                    fig_bar.add_trace(go.Bar(
                        name="Median (all years)",
                        x=agg["Month_Name"].tolist(),
                        y=agg["Median"].tolist(),
                        hovertemplate="<b>%{x} — Median</b><br>Rainfall: %{y} mm<extra></extra>",
                    ))
                if not show_bars:
                    st.info("Select at least one bar to display.")
                else:
                    fig_bar.update_layout(
                        barmode="group",
                        xaxis_title="Month", yaxis_title="Rainfall (mm)",
                        bargap=0.15, bargroupgap=0.05)
                    st.plotly_chart(fig_bar, use_container_width=True)
                    st.caption("⚠ number above bar = missing days (raw) for that month in the selected year")

            else:
                st.subheader("Monthly Rainfall Across All Years")
                col_m1, col_m2 = st.columns(2)
                with col_m1:
                    sel_month = st.selectbox(
                        "Select month",
                        options=list(month_names.keys()),
                        format_func=lambda m: month_names[m],
                        key="bar_sel_month",
                    )
                with col_m2:
                    st.markdown("**Reference line**")
                    sw_ref_mean   = st.toggle("Mean line",   value=True,  key="sw_ref_mean")
                    sw_ref_median = st.toggle("Median line", value=False, key="sw_ref_median")

                bar_base = base[base["Month"] == sel_month].copy()

                # Annual total for that month per year
                yr_tot = (bar_base.groupby("Year")[rain_col]
                          .sum().reset_index()
                          .rename(columns={rain_col: "Rainfall_mm"}))
                yr_tot["Rainfall_mm"] = yr_tot["Rainfall_mm"].round(1)

                # Missing days per year for that month (raw)
                raw_m = df[["Year", "Month", rain_col]].copy()
                raw_m[rain_col] = pd.to_numeric(raw_m[rain_col], errors="coerce")
                raw_m = raw_m[raw_m["Month"] == sel_month]
                _mr = raw_m.groupby("Year")[rain_col].apply(lambda x: int(x.isna().sum())).reset_index()
                _mr.columns = ["Year", "Missing_Raw"]

                # Missing days per year (after distribute)
                _md = bar_base.groupby("Year")[rain_col].apply(lambda x: int(x.isna().sum())).reset_index()
                _md.columns = ["Year", "Missing_Dist"]

                yr_agg = yr_tot.merge(_mr, on="Year", how="left")
                yr_agg = yr_agg.merge(_md, on="Year", how="left")

                mean_val   = round(yr_agg["Rainfall_mm"].mean(), 1)
                median_val = round(yr_agg["Rainfall_mm"].median(), 1)

                if distribute:
                    miss_label = "Missing (raw): %{customdata[0]}<br>Missing (after dist): %{customdata[1]}"
                else:
                    miss_label = "Missing days: %{customdata[0]}"

                fig_bar = go.Figure()

                fig_bar.add_trace(go.Bar(
                    name="Annual total",
                    x=yr_agg["Year"].tolist(),
                    y=yr_agg["Rainfall_mm"].tolist(),
                    customdata=yr_agg[["Missing_Raw", "Missing_Dist"]].values,
                    hovertemplate="<b>%{x}</b><br>Rainfall: %{y} mm<br>" + miss_label + "<extra></extra>",
                    text=yr_agg["Missing_Raw"].apply(lambda v: f"⚠ {int(v)}" if v > 0 else ""),
                    textposition="outside",
                ))

                if sw_ref_mean:
                    fig_bar.add_hline(y=mean_val, line_dash="dash", line_color="tomato", line_width=2)
                if sw_ref_median:
                    fig_bar.add_hline(y=median_val, line_dash="dot", line_color="mediumseagreen", line_width=2)
                if sw_ref_mean or sw_ref_median:
                    parts = []
                    if sw_ref_mean:
                        parts.append(f'<span style="color:tomato"><b>Mean: {mean_val} mm</b></span>')
                    if sw_ref_median:
                        parts.append(f'<span style="color:mediumseagreen"><b>Median: {median_val} mm</b></span>')
                    fig_bar.add_annotation(
                        x=0, xref="paper", y=1, yref="paper",
                        text=" &nbsp;|&nbsp; ".join(parts),
                        showarrow=False, xanchor="left", yanchor="top",
                        font=dict(size=15),
                        bgcolor="rgba(255,255,255,0.7)",
                        bordercolor="grey", borderwidth=1, borderpad=5,
                    )

                fig_bar.update_layout(
                    xaxis_title="Year",
                    yaxis_title="Rainfall (mm)",
                    bargap=0.15,
                )
                st.plotly_chart(fig_bar, use_container_width=True)
                st.caption("⚠ number above bar = missing days (raw) for that month/year")

        st.subheader("Annual Summary")
        st.dataframe(annual, use_container_width=True, hide_index=True)
        st.download_button(
            label="Download Annual Summary",
            data=annual.to_csv(index=False).encode(),
            file_name=f"{stn_id}_Annual Summary.csv",
            mime="text/csv",
        )

        st.subheader("Monthly Rainfall Pivot (mm)")
        st.dataframe(pivot, use_container_width=True)
        st.download_button(
            label="Download Monthly Rainfall Pivot",
            data=pivot.to_csv().encode(),
            file_name=f"{stn_id}_Monthly Rainfall Pivot.csv",
            mime="text/csv",
        )

        st.subheader("Missing Days Pivot")
        if distribute and miss_pivot_before is not None and miss_pivot_after is not None:
            tab_before, tab_after = st.tabs(["Missing Days (Before)", "Missing Days (After)"])
            with tab_before:
                st.dataframe(miss_pivot_before, use_container_width=True)
                st.download_button(
                    label="Download Missing Days (Before)",
                    data=miss_pivot_before.to_csv().encode(),
                    file_name=f"{stn_id}_Missing Days (Before).csv",
                    mime="text/csv",
                    key="dl_miss_before",
                )
            with tab_after:
                st.dataframe(miss_pivot_after, use_container_width=True)
                st.download_button(
                    label="Download Missing Days (After)",
                    data=miss_pivot_after.to_csv().encode(),
                    file_name=f"{stn_id}_Missing Days (After).csv",
                    mime="text/csv",
                    key="dl_miss_after",
                )
        elif miss_pivot_before is not None:
            st.dataframe(miss_pivot_before, use_container_width=True)
            st.download_button(
                label="Download Missing Days Pivot",
                data=miss_pivot_before.to_csv().encode(),
                file_name=f"{stn_id}_Missing Days.csv",
                mime="text/csv",
            )
