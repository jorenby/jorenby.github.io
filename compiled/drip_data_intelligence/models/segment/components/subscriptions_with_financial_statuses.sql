WITH transactions_ranked_by_date_desc AS (
    SELECT
        s.id,
        t.created_at,
        t.amount,
        DENSE_RANK() OVER (
            PARTITION BY s.id
            ORDER BY
                t.created_at DESC
        ) as transaction_date_rank_desc
    FROM
        "dumps"."dev"."subscriptions" s
        JOIN "dumps"."dev"."transactions" t ON s.id = t.subscription_id
),
last_transaction AS (
    SELECT
        id as drip_subscription_id,
        amount AS last_transaction_amount,
        created_at AS last_transaction_date
    FROM
        transactions_ranked_by_date_desc
    WHERE
        TRUE
        AND transaction_date_rank_desc = 1
)
SELECT
    drip_subscription_id,
    CASE
        WHEN last_transaction_date > (
    

    
        CURRENT_DATE
    
 - INTERVAL '31 DAYS') THEN TRUE
        ELSE FALSE
    END AS paid_in_the_last_30_days,
    CASE
        WHEN last_transaction_amount > 0 THEN TRUE
        ELSE FALSE
    END AS last_payment_amount_greater_than_0
FROM
    last_transaction