import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_detection")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fraud_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud_pass")


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide",
)

st.title("🛡️ Real-Time Fraud Detection Dashboard")

try:
    conn = get_connection()

    col1, col2, col3, col4 = st.columns(4)

    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM transactions_silver")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM transactions_silver WHERE is_fraud = 1.0")
    fraud = cur.fetchone()[0]

    fraud_rate = (fraud / total * 100) if total > 0 else 0

    cur.execute("SELECT ROUND(AVG(amount), 2) FROM transactions_silver")
    avg_amount = cur.fetchone()[0] or 0

    col1.metric("Total Transactions", f"{total:,}")
    col2.metric("Fraud Detected", f"{fraud:,}")
    col3.metric("Fraud Rate", f"{fraud_rate:.4f}%")
    col4.metric("Avg Transaction", f"${avg_amount:,.2f}")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Transaction Types Distribution")
        df_types = pd.read_sql("""
            SELECT type, COUNT(*) as count, SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count
            FROM transactions_silver
            GROUP BY type
        """, conn)

        fig_types = go.Figure()
        fig_types.add_trace(go.Bar(
            name="Total",
            x=df_types["type"],
            y=df_types["count"],
            marker_color="lightblue",
        ))
        fig_types.add_trace(go.Bar(
            name="Fraud",
            x=df_types["type"],
            y=df_types["fraud_count"],
            marker_color="red",
        ))
        fig_types.update_layout(barmode="group", height=400)
        st.plotly_chart(fig_types, use_container_width=True)

    with col_right:
        st.subheader("Transaction Amounts by Fraud Status")
        df_amounts = pd.read_sql("""
            SELECT
                CASE WHEN is_fraud = 1 THEN 'Fraud' ELSE 'Legitimate' END as status,
                amount
            FROM transactions_silver
            WHERE amount <= 500000
            LIMIT 5000
        """, conn)

        fig_amounts = px.histogram(
            df_amounts,
            x="amount",
            color="status",
            nbins=50,
            color_discrete_map={"Fraud": "red", "Legitimate": "steelblue"},
            height=400,
        )
        st.plotly_chart(fig_amounts, use_container_width=True)

    st.divider()

    st.subheader("Recent Fraudulent Transactions")
    df_fraud = pd.read_sql("""
        SELECT step, type, amount, name_orig, name_dest,
               oldbalance_org, newbalance_orig, ingested_at
        FROM transactions_silver
        WHERE is_fraud = 1.0
        ORDER BY ingested_at DESC
        LIMIT 100
    """, conn)
    st.dataframe(df_fraud, use_container_width=True)

    conn.close()

except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.info("Make sure PostgreSQL is running and accessible.")
