WITH staged AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

daily_summary AS (
    SELECT
        DATE_TRUNC('day', ingested_at)::DATE AS transaction_date,
        type AS transaction_type,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        ROUND(
            SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*),
            6
        ) AS fraud_rate,
        ROUND(AVG(amount), 2) AS avg_transaction_amount,
        MAX(amount) AS max_transaction_amount,
        SUM(amount) AS total_amount,
        AVG(velocity_1h) AS avg_velocity_1h,
        AVG(velocity_24h) AS avg_velocity_24h
    FROM staged
    GROUP BY 1, 2
)

SELECT * FROM daily_summary
ORDER BY transaction_date DESC, transaction_type
