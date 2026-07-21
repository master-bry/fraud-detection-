WITH source AS (
    SELECT * FROM {{ source('raw', 'transactions_silver') }}
),

renamed AS (
    SELECT
        id,
        step,
        type,
        amount,
        name_orig,
        oldbalance_org,
        newbalance_orig,
        name_dest,
        oldbalance_dest,
        newbalance_dest,
        is_fraud,
        is_flagged_fraud,
        velocity_1h,
        velocity_24h,
        amount_zscore,
        balance_ratio_orig,
        validated_at,
        ingested_at
    FROM source
)

SELECT * FROM renamed
