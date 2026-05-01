import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg2://pangan_user:user123@localhost:5434/panganwatch"
engine = create_engine(DATABASE_URL)


@st.cache_data
def load_data():
    query = """
        SELECT
            f.price_date,
            c.commodity_name,
            c.unit,
            r.province_name,
            r.city_name,
            f.price,
            f.source,
            a.previous_price,
            a.percentage_change,
            a.alert_status
        FROM fact_food_price f
        JOIN dim_commodity c ON f.commodity_id = c.commodity_id
        JOIN dim_region r ON f.region_id = r.region_id
        LEFT JOIN fact_price_alert a
            ON f.commodity_id = a.commodity_id
            AND f.region_id = a.region_id
            AND f.price_date = a.price_date
        ORDER BY f.price_date
    """
    df = pd.read_sql(query, engine)
    df["price_date"] = pd.to_datetime(df["price_date"])
    return df


st.set_page_config(
    page_title="PanganWatch Indonesia",
    page_icon="🌾",
    layout="wide"
)

st.title("🌾 PanganWatch Indonesia")
st.caption("Sistem Monitoring Harga Pangan Strategis Berbasis Data Pipeline")

df = load_data()

# Sidebar filters
st.sidebar.header("Filter Data")

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

commodity_options = sorted(df["commodity_name"].unique())
province_options = sorted(df["province_name"].unique())

selected_commodity = st.sidebar.selectbox("Pilih Komoditas", commodity_options)
selected_province = st.sidebar.selectbox("Pilih Provinsi", province_options)

min_date = df["price_date"].min()
max_date = df["price_date"].max()

selected_date_range = st.sidebar.date_input(
    "Pilih Rentang Tanggal",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

filtered = df[
    (df["commodity_name"] == selected_commodity) &
    (df["province_name"] == selected_province)
]

if len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
    filtered = filtered[
        (filtered["price_date"] >= pd.to_datetime(start_date)) &
        (filtered["price_date"] <= pd.to_datetime(end_date))
    ]

latest_date = df["price_date"].max()
latest_df = df[df["price_date"] == latest_date]

st.subheader("Ringkasan Nasional")

col1, col2, col3, col4 = st.columns(4)

avg_price = latest_df["price"].mean()
max_increase_row = latest_df.sort_values("percentage_change", ascending=False).head(1)
highest_price_row = latest_df.sort_values("price", ascending=False).head(1)

col1.metric("Rata-rata Harga Terbaru", f"Rp {avg_price:,.0f}")
col2.metric("Jumlah Komoditas", df["commodity_name"].nunique())
col3.metric("Jumlah Provinsi", df["province_name"].nunique())

if not max_increase_row.empty:
    col4.metric(
        "Kenaikan Tertinggi",
        max_increase_row.iloc[0]["commodity_name"],
        f"{max_increase_row.iloc[0]['percentage_change']:.2f}%"
    )

st.divider()

st.subheader(f"Tren Harga: {selected_commodity} di {selected_province}")

if filtered.empty:
    st.warning("Tidak ada data untuk filter yang dipilih.")
else:
    fig_trend = px.line(
        filtered,
        x="price_date",
        y="price",
        color="city_name",
        markers=True,
        title=f"Tren Harga {selected_commodity}"
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

fig_region = px.bar(
    comparison,
    x="province_name",
    y="price",
    color="alert_status",
    title=f"Perbandingan Harga Terbaru {selected_commodity} per Provinsi"
)
st.plotly_chart(fig_region, width="stretch")

st.subheader("Daftar Alert Harga")

alert_df = latest_df[
    latest_df["alert_status"].isin(["Watch", "Warning", "Critical"])
].sort_values("percentage_change", ascending=False)

st.dataframe(
    alert_df[
        [
            "price_date",
            "province_name",
            "city_name",
            "commodity_name",
            "price",
            "percentage_change",
            "alert_status"
        ]
    ],
    width="stretch"
)