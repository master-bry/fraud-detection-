from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import logging
import os
import pickle

logger = logging.getLogger(__name__)

default_args = {
    "owner": "brayan",
    "depends_on_past": False,
    "retries": 2,
}

MODEL_PATH = os.getenv("MODEL_PATH", "/opt/airflow/models/fraud_classifier.pkl")


def extract_training_data(**context):
    hook = PostgresHook(postgres_conn_id="fraud_detection")
    conn = hook.get_conn()

    import pandas as pd
    query = """
        SELECT step, type, amount, oldbalance_org, newbalance_orig,
               oldbalance_dest, newbalance_dest, velocity_1h, velocity_24h,
               amount_zscore, balance_ratio_orig, is_fraud
        FROM transactions_silver
        WHERE is_fraud IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Extracted {len(df)} training rows")
    return df


def train_model(**context):
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report, roc_auc_score
    from sklearn.preprocessing import LabelEncoder

    ti = context["ti"]
    df = ti.xcom_pull(task_ids="extract_training_data")

    if df is None or df.empty:
        logger.warning("No training data available, skipping model training")
        return

    le = LabelEncoder()
    df["type_encoded"] = le.fit_transform(df["type"])

    feature_cols = [
        "step", "type_encoded", "amount", "oldbalance_org", "newbalance_orig",
        "oldbalance_dest", "newbalance_dest", "velocity_1h", "velocity_24h",
        "amount_zscore", "balance_ratio_orig",
    ]

    X = df[feature_cols].fillna(0)
    y = df["is_fraud"].fillna(0).astype(int)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    model = GradientBoostingClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
        subsample=0.8,
    )

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    report = classification_report(y_test, y_pred, output_dict=True)
    auc = roc_auc_score(y_test, y_proba)

    logger.info(f"Model AUC: {auc:.4f}")
    logger.info(f"Classification Report:\n{classification_report(y_test, y_pred)}")

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "encoder": le, "features": feature_cols, "auc": auc}, f)

    logger.info(f"Model saved to {MODEL_PATH}")
    return {"auc": auc, "precision": report["1"]["precision"], "recall": report["1"]["recall"]}


with DAG(
    dag_id="fraud_model_retrain",
    default_args=default_args,
    description="Weekly model retraining pipeline",
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    tags=["fraud", "ml", "retrain"],
) as dag:

    extract = PythonOperator(
        task_id="extract_training_data",
        python_callable=extract_training_data,
    )

    train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    extract >> train
