import os
import pickle
import logging
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

MODEL_PATH = os.getenv("MODEL_PATH", "models/fraud_classifier.pkl")
POSTGRES_URL = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_detection")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fraud_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud_pass")

model_data = None


def load_model():
    global model_data
    if os.path.exists(MODEL_PATH):
        with open(MODEL_PATH, "rb") as f:
            model_data = pickle.load(f)
        logger.info(f"Model loaded from {MODEL_PATH}")
    else:
        logger.warning(f"Model not found at {MODEL_PATH}, scoring will use heuristics")


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_model()
    yield


app = FastAPI(
    title="Fraud Detection API",
    description="Real-time fraud scoring endpoint for mobile money transactions",
    version="1.0.0",
    lifespan=lifespan,
)


class Transaction(BaseModel):
    step: float
    type: str
    amount: float
    nameOrig: str
    oldbalanceOrg: float
    newbalanceOrig: float
    nameDest: str
    oldbalanceDest: float
    newbalanceDest: float
    velocity_1h: float = 0.0
    velocity_24h: float = 0.0
    amount_zscore: float = 0.0
    balance_ratio_orig: float = 0.0


class FraudScore(BaseModel):
    fraud_probability: float
    risk_level: str
    model_used: str
    scored_at: str


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    timestamp: str


def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_URL,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


@app.get("/health", response_model=HealthResponse)
def health_check():
    return HealthResponse(
        status="healthy",
        model_loaded=model_data is not None,
        scored_at=datetime.utcnow().isoformat(),
    )


@app.post("/score", response_model=FraudScore)
def score_transaction(tx: Transaction):
    if model_data is None:
        probability = heuristic_score(tx)
        return FraudScore(
            fraud_probability=probability,
            risk_level=classify_risk(probability),
            model_used="heuristic",
            scored_at=datetime.utcnow().isoformat(),
        )

    le = model_data["encoder"]
    feature_cols = model_data["features"]
    model = model_data["model"]

    type_encoded = le.transform([tx.type])[0] if tx.type in le.classes_ else 0

    features = pd.DataFrame([{
        "step": tx.step,
        "type_encoded": type_encoded,
        "amount": tx.amount,
        "oldbalance_org": tx.oldbalanceOrg,
        "newbalance_orig": tx.newbalanceOrig,
        "oldbalance_dest": tx.oldbalanceDest,
        "newbalance_dest": tx.newbalanceDest,
        "velocity_1h": tx.velocity_1h,
        "velocity_24h": tx.velocity_24h,
        "amount_zscore": tx.amount_zscore,
        "balance_ratio_orig": tx.balance_ratio_orig,
    }])

    probability = model.predict_proba(features[feature_cols])[0][1]

    return FraudScore(
        fraud_probability=round(float(probability), 6),
        risk_level=classify_risk(probability),
        model_used="gradient_boosting",
        scored_at=datetime.utcnow().isoformat(),
    )


def heuristic_score(tx: Transaction) -> float:
    score = 0.0

    if tx.type in ("TRANSFER", "CASH_OUT") and tx.amount > 100000:
        score += 0.3

    if tx.oldbalanceOrg > 0 and tx.amount > tx.oldbalanceOrg * 0.9:
        score += 0.3

    if tx.velocity_1h > 5:
        score += 0.2

    if tx.amount_zscore > 2.0:
        score += 0.2

    return min(score, 1.0)


def classify_risk(probability: float) -> str:
    if probability >= 0.7:
        return "HIGH"
    elif probability >= 0.3:
        return "MEDIUM"
    else:
        return "LOW"


@app.get("/recent-fraud")
def get_recent_fraud(limit: int = 50):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, step, type, amount, name_orig, is_fraud, ingested_at
            FROM transactions_bronze
            WHERE is_fraud = 1.0
            ORDER BY ingested_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        conn.close()

        return [
            {
                "id": str(r[0]),
                "step": r[1],
                "type": r[2],
                "amount": r[3],
                "name_orig": r[4],
                "is_fraud": r[5],
                "ingested_at": str(r[6]),
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching recent fraud: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
def get_stats():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM transactions_silver")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM transactions_silver WHERE is_fraud = 1.0")
        fraud_count = cur.fetchone()[0]

        cur.execute("SELECT ROUND(AVG(amount), 2) FROM transactions_silver")
        avg_amount = cur.fetchone()[0]

        conn.close()

        return {
            "total_transactions": total,
            "fraud_transactions": fraud_count,
            "fraud_rate": round(fraud_count / total, 6) if total > 0 else 0,
            "average_amount": float(avg_amount) if avg_amount else 0,
        }
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
