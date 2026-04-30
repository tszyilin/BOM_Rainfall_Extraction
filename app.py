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
st.title("BOM Rainfall Downloader")
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

    # Raw missing days (before any distribution) — NaN count from raw base
    raw_missing = (
        df[["Year", rain_col]].copy()
        .assign(**{rain_col: pd.to_numeric(df[rain_col], errors="coerce")})
        .groupby("Year")[rain_col]
        .apply(lambda x: x.isna().sum())
        .reset_index()
        .rename(columns={rain_col: "Before Distributing"})
    )

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
        after_missing = (
            base.groupby("Year")[rain_col]
            .apply(lambda x: x.isna().sum())
            .reset_index()
            .rename(columns={rain_col: "After Distributing"})
        )
        annual = annual.merge(raw_missing, on="Year", how="left")
        annual = annual.merge(after_missing, on="Year", how="left")
        annual = annual[["Year", "Accumulated_Readings",
                          "Before Distributing", "After Distributing", "Annual_Rainfall_mm"]]
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
                raw_missing["Before Distributing"] - raw_missing["Covered_Days"]
            ).clip(lower=0)
            raw_missing = raw_missing[["Year", "Missing_Days"]]
        else:
            raw_missing = raw_missing.rename(columns={"Before Distributing": "Missing_Days"})
        annual = annual.merge(raw_missing, on="Year", how="left")
        annual = annual[["Year", "Accumulated_Readings", "Missing_Days", "Annual_Rainfall_mm"]]

    month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                   7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    monthly = base.groupby(["Year", "Month"])[rain_col].sum().reset_index()
    pivot = monthly.pivot(index="Year", columns="Month", values=rain_col)
    pivot.rename(columns=month_names, inplace=True)
    pivot = pivot.reindex(columns=list(month_names.values())).round(1)

    # Missing days pivot (before distribution — from raw df)
    raw_df_num = df[["Year", "Month", rain_col]].copy()
    raw_df_num[rain_col] = pd.to_numeric(raw_df_num[rain_col], errors="coerce")
    miss_before_monthly = (raw_df_num.groupby(["Year", "Month"])[rain_col]
                           .apply(lambda x: x.isna().sum()).reset_index())
    miss_pivot_before = miss_before_monthly.pivot(index="Year", columns="Month", values=rain_col)
    miss_pivot_before.rename(columns=month_names, inplace=True)
    miss_pivot_before = miss_pivot_before.reindex(columns=list(month_names.values()))

    # Missing days pivot (after distribution — from distributed base)
    miss_after_monthly = (base.groupby(["Year", "Month"])[rain_col]
                          .apply(lambda x: x.isna().sum()).reset_index())
    miss_pivot_after = miss_after_monthly.pivot(index="Year", columns="Month", values=rain_col)
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
                    show_bars = st.multiselect(
                        "Show bars",
                        options=["Selected year", "Median (selected year)", "Mean (all years)", "Median (all years)"],
                        default=["Selected year"],
                        key="bar_show_bars",
                    )
                bar_base = base[base["Year"] == sel_year].copy()

                # Monthly totals for selected year
                monthly_tot = (bar_base.groupby("Month")[rain_col]
                               .sum().reset_index()
                               .rename(columns={rain_col: "Rainfall_mm"}))
                monthly_tot["Rainfall_mm"] = monthly_tot["Rainfall_mm"].round(1)

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

                # Median of monthly totals across all years per month (for selected year comparison)
                median_yr = (all_yr_monthly2.groupby("Month")["Rainfall_mm"]
                             .median().round(1).reset_index()
                             .rename(columns={"Rainfall_mm": "Median_yr"}))

                # Median of monthly totals for selected year only (one value per month = the total itself,
                # so we use median of daily values within each month for the selected year)
                sel_yr_daily_med = (bar_base.groupby("Month")[rain_col]
                                    .median().round(1).reset_index()
                                    .rename(columns={rain_col: "Median_sel"}))

                agg = monthly_tot.merge(mean_med, on="Month", how="left")
                agg = agg.merge(sel_yr_daily_med, on="Month", how="left")
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
                if "Median (selected year)" in show_bars:
                    fig_bar.add_trace(go.Bar(
                        name=f"Median daily ({sel_year})",
                        x=agg["Month_Name"].tolist(),
                        y=agg["Median_sel"].tolist(),
                        hovertemplate=f"<b>%{{x}} — Median daily {sel_year}</b><br>Rainfall: %{{y}} mm<extra></extra>",
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
                    ref_stat = st.selectbox(
                        "Reference line",
                        options=["Mean", "Median"],
                        index=0,
                        key="bar_ref_stat",
                    )
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
                miss_raw_yr = (raw_m.groupby("Year")[rain_col]
                               .apply(lambda x: int(x.isna().sum())).reset_index()
                               .rename(columns={rain_col: "Missing_Raw"}))

                # Missing days per year (after distribute)
                miss_dist_yr = (bar_base.groupby("Year")[rain_col]
                                .apply(lambda x: int(x.isna().sum())).reset_index()
                                .rename(columns={rain_col: "Missing_Dist"}))

                yr_agg = yr_tot.merge(miss_raw_yr, on="Year", how="left")
                yr_agg = yr_agg.merge(miss_dist_yr, on="Year", how="left")

                mean_val   = round(yr_agg["Rainfall_mm"].mean(), 1)
                median_val = round(yr_agg["Rainfall_mm"].median(), 1)
                ref_val    = mean_val if ref_stat == "Mean" else median_val

                if distribute:
                    miss_label = "Missing (raw): %{customdata[0]}<br>Missing (after dist): %{customdata[1]}"
                else:
                    miss_label = "Missing days: %{customdata[0]}"

                fig_bar = go.Figure()

                # Individual year bars
                fig_bar.add_trace(go.Bar(
                    name="Annual total",
                    x=yr_agg["Year"].tolist(),
                    y=yr_agg["Rainfall_mm"].tolist(),
                    customdata=yr_agg[["Missing_Raw", "Missing_Dist"]].values,
                    hovertemplate=(
                        "<b>%{x}</b><br>Rainfall: %{y} mm<br>" +
                        miss_label + "<extra></extra>"
                    ),
                    text=yr_agg["Missing_Raw"].apply(lambda v: f"⚠ {int(v)}" if v > 0 else ""),
                    textposition="outside",
                    marker_color="steelblue",
                ))

                # Selected reference line
                fig_bar.add_hline(y=ref_val, line_dash="dash", line_color="tomato", line_width=2,
                                  annotation_text=f"{ref_stat}: {ref_val} mm",
                                  annotation_position="top left",
                                  annotation_font=dict(size=15, color="tomato"),
                                  annotation_bgcolor="rgba(255,255,255,0.7)",
                                  annotation_bordercolor="tomato",
                                  annotation_borderwidth=1,
                                  annotation_borderpad=5)

                fig_bar.update_layout(
                    xaxis_title="Year",
                    yaxis_title="Rainfall (mm)",
                    bargap=0.15,
                )
                st.plotly_chart(fig_bar, use_container_width=True)
                st.caption("⚠ number above bar = missing days (raw) for that month/year")

        st.subheader("Annual Summary")
        st.dataframe(annual, use_container_width=True, hide_index=True)

        st.subheader("Monthly Rainfall Pivot (mm)")
        st.dataframe(pivot, use_container_width=True)

        st.subheader("Missing Days Pivot")
        if distribute and miss_pivot_before is not None and miss_pivot_after is not None:
            tab_before, tab_after = st.tabs(["Before Distributing", "After Distributing"])
            with tab_before:
                st.dataframe(miss_pivot_before, use_container_width=True)
            with tab_after:
                st.dataframe(miss_pivot_after, use_container_width=True)
        elif miss_pivot_before is not None:
            st.dataframe(miss_pivot_before, use_container_width=True)
