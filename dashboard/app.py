from __future__ import annotations

import os

import altair as alt
import pandas as pd
import snowflake.connector
import streamlit as st


st.set_page_config(page_title="NYC Taxi Warehouse", layout="wide")


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "NYC_TAXI_DW"),
        schema="MARTS",
    )


@st.cache_data(ttl=600)
def query(sql: str) -> pd.DataFrame:
    with get_connection().cursor() as cur:
        cur.execute(sql)
        return cur.fetch_pandas_all()


st.title("NYC Taxi Warehouse")

summary = query(
    """
    select
        count(*) as trip_count,
        round(sum(total_amount), 2) as gross_revenue,
        round(avg(total_amount), 2) as avg_fare,
        round(avg(trip_distance), 2) as avg_distance,
        round(avg(case when is_airport_trip then 1 else 0 end) * 100, 2) as airport_trip_pct
    from NYC_TAXI_DW.MARTS.FCT_TAXI_TRIPS
    """
)

daily_revenue = query(
    """
    select
        pickup_date,
        sum(gross_revenue) as gross_revenue,
        sum(trip_count) as trip_count,
        avg(avg_revenue_per_mile) as avg_revenue_per_mile
    from NYC_TAXI_DW.MARTS.MART_DAILY_REVENUE
    group by 1
    order by 1
    """
)

hourly_demand = query(
    """
    select
        pickup_day_name,
        pickup_hour_of_day,
        sum(trip_count) as trip_count
    from NYC_TAXI_DW.MARTS.AGG_HOURLY_ZONE_DEMAND
    group by 1, 2
    """
)

top_zones = query(
    """
    select
        pickup_borough,
        pickup_zone,
        sum(trip_count) as trip_count,
        round(sum(gross_revenue), 2) as gross_revenue
    from NYC_TAXI_DW.MARTS.AGG_HOURLY_ZONE_DEMAND
    group by 1, 2
    order by trip_count desc
    limit 20
    """
)

airport_by_hour = query(
    """
    select
        pickup_hour_of_day,
        sum(trip_count) as trip_count,
        sum(airport_trip_count) as airport_trip_count,
        round(sum(airport_trip_count) / nullif(sum(trip_count), 0) * 100, 2) as airport_trip_pct
    from NYC_TAXI_DW.MARTS.AGG_HOURLY_ZONE_DEMAND
    group by 1
    order by 1
    """
)

metrics = summary.iloc[0]
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Trips", f"{int(metrics.TRIP_COUNT):,}")
col2.metric("Gross Revenue", f"${metrics.GROSS_REVENUE:,.0f}")
col3.metric("Avg Fare", f"${metrics.AVG_FARE:,.2f}")
col4.metric("Avg Distance", f"{metrics.AVG_DISTANCE:,.2f} mi")
col5.metric("Airport Share", f"{metrics.AIRPORT_TRIP_PCT:,.2f}%")

left, right = st.columns((2, 1))

with left:
    st.subheader("Daily Revenue")
    st.altair_chart(
        alt.Chart(daily_revenue)
        .mark_line(point=True)
        .encode(
            x=alt.X("PICKUP_DATE:T", title="Pickup Date"),
            y=alt.Y("GROSS_REVENUE:Q", title="Gross Revenue"),
            tooltip=["PICKUP_DATE:T", "GROSS_REVENUE:Q", "TRIP_COUNT:Q"],
        )
        .properties(height=320),
        use_container_width=True,
    )

with right:
    st.subheader("Airport Trip Share")
    st.altair_chart(
        alt.Chart(airport_by_hour)
        .mark_bar()
        .encode(
            x=alt.X("PICKUP_HOUR_OF_DAY:O", title="Hour"),
            y=alt.Y("AIRPORT_TRIP_PCT:Q", title="Airport Trip %"),
            tooltip=["PICKUP_HOUR_OF_DAY:O", "AIRPORT_TRIP_PCT:Q", "AIRPORT_TRIP_COUNT:Q"],
        )
        .properties(height=320),
        use_container_width=True,
    )

st.subheader("Hourly Demand Heatmap")
st.altair_chart(
    alt.Chart(hourly_demand)
    .mark_rect()
    .encode(
        x=alt.X("PICKUP_HOUR_OF_DAY:O", title="Hour"),
        y=alt.Y("PICKUP_DAY_NAME:N", title="Day"),
        color=alt.Color("TRIP_COUNT:Q", title="Trips"),
        tooltip=["PICKUP_DAY_NAME:N", "PICKUP_HOUR_OF_DAY:O", "TRIP_COUNT:Q"],
    )
    .properties(height=260),
    use_container_width=True,
)

st.subheader("Top Pickup Zones")
st.dataframe(top_zones, use_container_width=True, hide_index=True)

