CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS transactions_bronze (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    step FLOAT,
    type VARCHAR(10),
    amount FLOAT,
    name_orig VARCHAR(20),
    oldbalance_org FLOAT,
    newbalance_orig FLOAT,
    name_dest VARCHAR(20),
    oldbalance_dest FLOAT,
    newbalance_dest FLOAT,
    is_fraud FLOAT,
    is_flagged_fraud FLOAT,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions_silver (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    step FLOAT,
    type VARCHAR(10),
    amount FLOAT,
    name_orig VARCHAR(20),
    oldbalance_org FLOAT,
    newbalance_orig FLOAT,
    name_dest VARCHAR(20),
    oldbalance_dest FLOAT,
    newbalance_dest FLOAT,
    is_fraud FLOAT,
    is_flagged_fraud FLOAT,
    velocity_1h FLOAT,
    velocity_24h FLOAT,
    amount_zscore FLOAT,
    balance_ratio_orig FLOAT,
    validated_at TIMESTAMP DEFAULT NOW(),
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_scores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID REFERENCES transactions_silver(id),
    fraud_probability FLOAT,
    risk_level VARCHAR(10),
    scored_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_step ON transactions_bronze(step);
CREATE INDEX IF NOT EXISTS idx_bronze_type ON transactions_bronze(type);
CREATE INDEX IF NOT EXISTS idx_silver_step ON transactions_silver(step);
CREATE INDEX IF NOT EXISTS idx_silver_type ON transactions_silver(type);
CREATE INDEX IF NOT EXISTS idx_scores_transaction ON fraud_scores(transaction_id);
