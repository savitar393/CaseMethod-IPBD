import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
engine = create_engine(DATABASE_URL)


@st.cache_data
def load_data():
    query = """
        SELECT *
        FROM v_food_price_analysis
        ORDER BY price_date
    """
    df = pd.read_sql(query, engine)
    df["price_date"] = pd.to_datetime(df["price_date"])
    return df

@st.cache_data
def load_news_data():
    query = """
        SELECT
            title,
            url,
            source_name,
            published_at,
            query_keyword,
            category,
            scraped_at
        FROM fact_food_news
        ORDER BY published_at DESC NULLS LAST
        LIMIT 100
    """
    news_df = pd.read_sql(query, engine)
    news_df["published_at"] = pd.to_datetime(news_df["published_at"], errors="coerce")
    return news_df


st.set_page_config(
    page_title="PanganWatch Indonesia",
    page_icon="🌾",
    layout="wide"
)

st.title("🌾 PanganWatch Indonesia")
st.caption("Sistem Monitoring Harga Pangan Strategis Berbasis Data Pipeline")

raw_df = load_data()

raw_df["percentage_change"] = pd.to_numeric(raw_df["percentage_change"], errors="coerce")
raw_df["alert_status"] = raw_df["alert_status"].fillna("No Previous Data")

st.sidebar.header("Filter Sumber Data")

available_sources = sorted(raw_df["source"].dropna().unique().tolist())

selected_sources = st.sidebar.multiselect(
    "Pilih Sumber Data",
    available_sources,
    default=available_sources
)

df = raw_df[raw_df["source"].isin(selected_sources)].copy()

if df.empty:
    st.warning("Tidak ada data untuk sumber yang dipilih.")
    st.stop()

st.sidebar.header("Filter Analisis")

commodity_group_options = sorted(df["commodity_group"].dropna().unique().tolist())
selected_groups = st.sidebar.multiselect(
    "Pilih Kelompok Komoditas",
    commodity_group_options,
    default=commodity_group_options
)

region_level_options = sorted(df["region_level"].dropna().unique().tolist())
selected_region_levels = st.sidebar.multiselect(
    "Pilih Level Wilayah",
    region_level_options,
    default=region_level_options
)

source_type_options = sorted(df["source_type"].dropna().unique().tolist())
selected_source_types = st.sidebar.multiselect(
    "Pilih Tipe Sumber",
    source_type_options,
    default=source_type_options
)

df = df[
    df["commodity_group"].isin(selected_groups)
    & df["region_level"].isin(selected_region_levels)
    & df["source_type"].isin(selected_source_types)
].copy()

if df.empty:
    st.warning("Tidak ada data setelah filter analisis diterapkan.")
    st.stop()

st.sidebar.caption("Database source")
safe_database_url = DATABASE_URL.replace("user123", "******")
st.sidebar.code(safe_database_url)

st.sidebar.caption("Loaded rows from PostgreSQL")
st.sidebar.write(f"{len(df):,} rows")
st.sidebar.caption("Latest data date")
st.sidebar.write(df["price_date"].max().date())
with st.sidebar.expander("Source Breakdown"):
    source_summary = (
        df.groupby(["source", "source_type"])
        .agg(
            rows=("price", "count"),
            commodities=("commodity_name", "nunique"),
            regions=("province_name", "nunique"),
            min_date=("price_date", "min"),
            max_date=("price_date", "max"),
        )
        .reset_index()
        .sort_values("rows", ascending=False)
    )

    st.dataframe(source_summary, width="stretch")

# Sidebar filters
st.sidebar.header("Filter Data")

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Choose source first
available_sources = sorted(df["source"].dropna().unique())

selected_source = st.sidebar.selectbox(
    "Pilih Sumber Utama Analisis",
    available_sources,
    index=available_sources.index("PIHPS Grid Backfill")
    if "PIHPS Grid Backfill" in available_sources else 0
)

source_df = df[df["source"] == selected_source].copy()

# Commodity options only from selected source
commodity_options = sorted(source_df["commodity_name"].dropna().unique())

selected_commodity = st.sidebar.selectbox(
    "Pilih Komoditas",
    commodity_options
)

commodity_df = source_df[source_df["commodity_name"] == selected_commodity].copy()

# Region options only from selected source + selected commodity
province_options = sorted(commodity_df["province_name"].dropna().unique())

selected_region = st.sidebar.selectbox(
    "Pilih Wilayah",
    province_options
)

filtered = commodity_df[
    commodity_df["province_name"] == selected_region
].copy()

min_date = source_df["price_date"].min()
max_date = source_df["price_date"].max()

selected_date_range = st.sidebar.date_input(
    "Pilih Rentang Tanggal",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
    filtered = filtered[
        (filtered["price_date"] >= pd.to_datetime(start_date)) &
        (filtered["price_date"] <= pd.to_datetime(end_date))
    ]

latest_date = source_df["price_date"].max()
latest_df = source_df[source_df["price_date"] == latest_date].copy()

st.subheader("Ringkasan Data Terpilih")

col1, col2, col3, col4 = st.columns(4)

avg_price = latest_df["price"].mean()
max_increase_row = (
    latest_df
    .dropna(subset=["percentage_change"])
    .sort_values("percentage_change", ascending=False)
    .head(1)
)
highest_price_row = latest_df.sort_values("price", ascending=False).head(1)

col1.metric("Rata-rata Harga Terbaru", f"Rp {avg_price:,.0f}")
col2.metric("Jumlah Komoditas", source_df["commodity_name"].nunique())
col3.metric("Jumlah Wilayah", source_df["province_name"].nunique())

if not max_increase_row.empty:
    col4.metric(
        "Kenaikan Tertinggi",
        max_increase_row.iloc[0]["commodity_name"],
        f"{max_increase_row.iloc[0]['percentage_change']:.2f}%"
    )
else:
    col4.metric(
        "Kenaikan Tertinggi",
        "-",
        "Belum ada data perubahan"
    )

st.divider()
st.subheader("Distribusi Data berdasarkan Kelompok Komoditas")

group_summary = (
    source_df.groupby("commodity_group")
    .agg(
        rows=("price", "count"),
        avg_price=("price", "mean"),
        commodities=("commodity_name", "nunique"),
    )
    .reset_index()
    .sort_values("rows", ascending=False)
)

fig_group = px.bar(
    group_summary,
    x="commodity_group",
    y="rows",
    color="commodity_group",
    title="Jumlah Data per Kelompok Komoditas"
)

st.plotly_chart(fig_group, width="stretch")

st.dataframe(group_summary, width="stretch")

st.divider()

st.subheader(f"Tren Harga: {selected_commodity} di {selected_region}")

if filtered.empty:
    st.warning("Tidak ada data untuk filter yang dipilih.")
else:
    fig_trend = px.line(
        filtered,
        x="price_date",
        y="price",
        color="source",
        markers=True,
        title=f"Tren Harga {selected_commodity} berdasarkan Sumber Data"
    )
    st.plotly_chart(fig_trend, width="stretch")

    st.subheader("Detail Data Terfilter")
    st.dataframe(
        filtered[
            [
                "price_date",
                "province_name",
                "city_name",
                "commodity_name",
                "price",
                "previous_price",
                "percentage_change",
                "alert_status"
            ]
        ],
        width="stretch"
    )

st.divider()
st.subheader("Perbandingan Harga Terbaru per Wilayah")

comparison = latest_df[latest_df["commodity_name"] == selected_commodity]

if comparison.empty:
    st.info("Tidak ada data terbaru untuk komoditas yang dipilih.")
else:
    fig_region = px.bar(
        comparison.sort_values("price", ascending=False),
        x="province_name",
        y="price",
        color="alert_status",
        title=f"Harga Terbaru {selected_commodity} per Wilayah"
    )
    st.plotly_chart(fig_region, width="stretch")


st.divider()
st.subheader("Top Kenaikan Harga Valid")

movement_df = source_df.dropna(subset=["percentage_change"]).copy()

movement_df = movement_df[
    movement_df["alert_status"].isin(["Normal", "Watch", "Warning", "Critical"])
]

increase_df = movement_df[movement_df["percentage_change"] > 0].sort_values(
    "percentage_change",
    ascending=False
).head(10)

if increase_df.empty:
    st.info("Belum ada kenaikan harga valid dari data yang tersedia.")
else:
    fig_increase = px.bar(
        increase_df,
        x="percentage_change",
        y="commodity_name",
        color="source",
        orientation="h",
        hover_data=[
            "price_date",
            "price",
            "previous_price",
            "gap_days",
            "alert_status",
        ],
        title="Top 10 Kenaikan Harga berdasarkan Data Real Scraping"
    )

    fig_increase.update_layout(
        yaxis={"categoryorder": "total ascending"}
    )

    st.plotly_chart(fig_increase, width="stretch")

    st.dataframe(
        increase_df[
            [
                "price_date",
                "commodity_name",
                "price",
                "previous_price",
                "percentage_change",
                "gap_days",
                "alert_status",
                "source",
            ]
        ],
        width="stretch"
    )

st.subheader("Top Penurunan Harga Valid")

decrease_df = movement_df[movement_df["percentage_change"] < 0].sort_values(
    "percentage_change",
    ascending=True
).head(10)

if decrease_df.empty:
    st.info("Belum ada penurunan harga valid dari data yang tersedia.")
else:
    fig_decrease = px.bar(
        decrease_df,
        x="percentage_change",
        y="commodity_name",
        color="source",
        orientation="h",
        hover_data=[
            "price_date",
            "price",
            "previous_price",
            "gap_days",
            "alert_status",
        ],
        title="Top 10 Penurunan Harga berdasarkan Data Real Scraping"
    )

    fig_decrease.update_layout(
        yaxis={"categoryorder": "total ascending"}
    )

    st.plotly_chart(fig_decrease, width="stretch")

    st.dataframe(
        decrease_df[
            [
                "price_date",
                "commodity_name",
                "price",
                "previous_price",
                "percentage_change",
                "gap_days",
                "alert_status",
                "source",
            ]
        ],
        width="stretch"
    )


st.divider()
st.subheader("Ranking Harga Komoditas Terbaru")

ranking_df = latest_df.sort_values("price", ascending=False).head(15)

fig_ranking = px.bar(
    ranking_df,
    x="price",
    y="commodity_name",
    color="source",
    orientation="h",
    hover_data=[
        "province_name",
        "city_name",
        "unit",
        "price_date",
    ],
    title="Top 15 Harga Komoditas Tertinggi dari Data Scraping Terbaru"
)

fig_ranking.update_layout(
    yaxis={"categoryorder": "total ascending"}
)

st.plotly_chart(fig_ranking, width="stretch")

st.dataframe(
    ranking_df[
        [
            "price_date",
            "province_name",
            "city_name",
            "commodity_name",
            "unit",
            "price",
            "source",
        ]
    ],
    width="stretch"
)


st.divider()
st.subheader("Peta Harga Komoditas")

province_coords = pd.DataFrame([
    {"province_name": "Aceh", "lat": 4.6951, "lon": 96.7494},
    {"province_name": "Sumatera Utara", "lat": 3.5952, "lon": 98.6722},
    {"province_name": "Sumatera Barat", "lat": -0.9471, "lon": 100.4172},
    {"province_name": "Riau", "lat": 0.5071, "lon": 101.4478},
    {"province_name": "Kepulauan Riau", "lat": 3.9457, "lon": 108.1429},
    {"province_name": "Jambi", "lat": -1.6101, "lon": 103.6131},
    {"province_name": "Sumatera Selatan", "lat": -3.3194, "lon": 103.9144},
    {"province_name": "Bengkulu", "lat": -3.7928, "lon": 102.2608},
    {"province_name": "Lampung", "lat": -5.4500, "lon": 105.2667},
    {"province_name": "Bangka Belitung", "lat": -2.7411, "lon": 106.4406},
    {"province_name": "DKI Jakarta", "lat": -6.2088, "lon": 106.8456},
    {"province_name": "Jawa Barat", "lat": -6.9175, "lon": 107.6191},
    {"province_name": "Jawa Tengah", "lat": -6.9667, "lon": 110.4167},
    {"province_name": "DI Yogyakarta", "lat": -7.7956, "lon": 110.3695},
    {"province_name": "Jawa Timur", "lat": -7.2575, "lon": 112.7521},
    {"province_name": "Banten", "lat": -6.1200, "lon": 106.1503},
    {"province_name": "Bali", "lat": -8.4095, "lon": 115.1889},
    {"province_name": "Nusa Tenggara Barat", "lat": -8.6529, "lon": 117.3616},
    {"province_name": "Nusa Tenggara Timur", "lat": -8.6574, "lon": 121.0794},
    {"province_name": "Kalimantan Barat", "lat": -0.2788, "lon": 111.4753},
    {"province_name": "Kalimantan Tengah", "lat": -1.6815, "lon": 113.3824},
    {"province_name": "Kalimantan Selatan", "lat": -3.0926, "lon": 115.2838},
    {"province_name": "Kalimantan Timur", "lat": -0.5022, "lon": 117.1536},
    {"province_name": "Kalimantan Utara", "lat": 3.0731, "lon": 116.0414},
    {"province_name": "Sulawesi Utara", "lat": 1.4931, "lon": 124.8413},
    {"province_name": "Sulawesi Tengah", "lat": -1.4300, "lon": 121.4456},
    {"province_name": "Sulawesi Selatan", "lat": -5.1477, "lon": 119.4327},
    {"province_name": "Sulawesi Tenggara", "lat": -4.1449, "lon": 122.1746},
    {"province_name": "Gorontalo", "lat": 0.6999, "lon": 122.4467},
    {"province_name": "Sulawesi Barat", "lat": -2.8441, "lon": 119.2321},
    {"province_name": "Maluku", "lat": -3.2385, "lon": 130.1453},
    {"province_name": "Maluku Utara", "lat": 1.5700, "lon": 127.8088},
    {"province_name": "Papua", "lat": -4.2699, "lon": 138.0804},
    {"province_name": "Papua Barat", "lat": -1.3361, "lon": 133.1747},
])

map_df = latest_df[
    latest_df["commodity_name"] == selected_commodity
].merge(
    province_coords,
    on="province_name",
    how="left"
)

map_df = map_df.dropna(subset=["lat", "lon"])

if map_df.empty:
    st.info("Koordinat provinsi belum tersedia untuk data yang dipilih.")
else:
    fig_map = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        size="price",
        color="price",
        hover_name="province_name",
        hover_data={
            "commodity_name": True,
            "price": ":,.0f",
            "source": True,
            "lat": False,
            "lon": False,
        },
        zoom=4,
        height=500,
        title=f"Peta Harga {selected_commodity}"
    )

    fig_map.update_layout(mapbox_style="open-street-map")
    fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})

    st.plotly_chart(fig_map, width="stretch")


st.divider()
st.subheader("Berita Terkait Harga Pangan")

try:
    news_df = load_news_data()

    if news_df.empty:
        st.info("Belum ada berita yang tersimpan.")
    else:
        selected_category = st.selectbox(
            "Filter Kategori Berita",
            ["Semua"] + sorted(news_df["category"].dropna().unique().tolist())
        )

        if selected_category != "Semua":
            news_df = news_df[news_df["category"] == selected_category]

        st.dataframe(
            news_df[
                [
                    "published_at",
                    "category",
                    "source_name",
                    "title",
                    "query_keyword",
                    "url",
                ]
            ],
            width="stretch"
        )

except Exception as e:
    st.info(f"Panel berita belum tersedia: {e}")

st.divider()
st.subheader("Daftar Alert Harga")

alert_df = df[
    df["alert_status"].isin(["Watch", "Warning", "Critical"])
].sort_values("percentage_change", ascending=False)

if alert_df.empty:
    st.info(
        "Belum ada alert Watch/Warning/Critical. "
        "Sebagian besar perubahan harga saat ini masih dalam kategori Normal."
    )
else:
    st.dataframe(
        alert_df[
            [
                "price_date",
                "province_name",
                "city_name",
                "commodity_name",
                "price",
                "previous_price",
                "percentage_change",
                "gap_days",
                "alert_status",
                "source",
            ]
        ],
        width="stretch"
    )