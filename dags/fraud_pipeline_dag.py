from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "brayan",
    "depends_on_past": False,
    "retries": 2,
}

def validate_bronze_to_silver(**context):
    hook = PostgresHook(postgres_conn_id="fraud_detection")
    conn = hook.get_conn()
    cur = conn.cursor()

    checks = [
        ("No null amounts in silver", "SELECT COUNT(*) FROM transactions_silver WHERE amount IS NULL", 0),
        ("No negative amounts", "SELECT COUNT(*) FROM transactions_silver WHERE amount < 0", 0),
        ("Silver row count > 0", "SELECT COUNT(*) FROM transactions_silver", None),
    ]

    results = []
    for name, query, expected in checks:
        cur.execute(query)
        actual = cur.fetchone()[0]
        passed = (expected is None and actual > 0) or (actual == expected)
        results.append({"check": name, "passed": passed, "actual": actual})
        logger.info(f"CHECK {name}: {'PASS' if passed else 'FAIL'} (actual={actual})")

    failed = [r for r in results if not r["passed"]]
    if failed:
        raise ValueError(f"Data quality checks failed: {[r['check'] for r in failed]}")

    conn.close()


def compute_daily_aggregates(**context):
    hook = PostgresHook(postgres_conn_id="fraud_detection")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_fraud_rates (
            id SERIAL PRIMARY KEY,
            tx_date DATE,
            tx_type VARCHAR(10),
            total_transactions BIGINT,
            fraud_transactions BIGINT,
            fraud_rate FLOAT,
            avg_amount FLOAT,
            max_amount FLOAT,
            computed_at TIMESTAMP DEFAULT NOW()
        );

        INSERT INTO daily_fraud_rates (tx_date, tx_type, total_transactions, fraud_transactions, fraud_rate, avg_amount, max_amount)
        SELECT
            DATE_TRUNC('day', ingested_at)::DATE AS tx_date,
            type AS tx_type,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN is_fraud = 1.0 THEN 1 ELSE 0 END) AS fraud_transactions,
            ROUND(SUM(CASE WHEN is_fraud = 1.0 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 6) AS fraud_rate,
            ROUND(AVG(amount), 2) AS avg_amount,
            MAX(amount) AS max_amount
        FROM transactions_silver
        GROUP BY tx_date, tx_type
        ORDER BY tx_date, tx_type;
    """)

    conn.commit()
    conn.close()
    logger.info("Daily aggregates computed successfully")


def compute_user_risk_scores(**context):
    hook = PostgresHook(postgres_conn_id="fraud_detection")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_risk_scores (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(20),
            total_transactions BIGINT,
            fraud_transactions BIGINT,
            fraud_rate FLOAT,
            avg_amount FLOAT,
            risk_level VARCHAR(10),
            computed_at TIMESTAMP DEFAULT NOW()
        );

        TRUNCATE TABLE user_risk_scores;

        INSERT INTO user_risk_scores (user_id, total_transactions, fraud_transactions, fraud_rate, avg_amount, risk_level)
        SELECT
            name_orig AS user_id,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN is_fraud = 1.0 THEN 1 ELSE 0 END) AS fraud_transactions,
            ROUND(SUM(CASE WHEN is_fraud = 1.0 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 6) AS fraud_rate,
            ROUND(AVG(amount), 2) AS avg_amount,
            CASE
                WHEN SUM(CASE WHEN is_fraud = 1.0 THEN 1 ELSE 0 END) > 0 THEN 'HIGH'
                WHEN AVG(amount) > 100000 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS risk_level
        FROM transactions_silver
        GROUP BY name_orig;
    """)

    conn.commit()
    conn.close()
    logger.info("User risk scores computed successfully")


with DAG(
    dag_id="fraud_pipeline_daily",
    default_args=default_args,
    description="Daily fraud detection pipeline aggregation and validation",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["fraud", "pipeline"],
) as dag:

    validate_data = PythonOperator(
        task_id="validate_bronze_to_silver",
        python_callable=validate_bronze_to_silver,
    )

    daily_agg = PythonOperator(
        task_id="compute_daily_aggregates",
        python_callable=compute_daily_aggregates,
    )

    risk_scores = PythonOperator(
        task_id="compute_user_risk_scores",
        python_callable=compute_user_risk_scores,
    )

    validate_data >> daily_agg >> risk_scores
