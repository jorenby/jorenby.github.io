WITH subscription_transactions_ordered_chronologically AS (
	SELECT
		subscription_id AS drip_subscription_id
		, ROUND(amount::NUMERIC / 100, 2) AS drip_first_payment_value_mrr
	    , ROUND(amount_refunded::NUMERIC / 100, 2) AS drip_amount_refunded
		, DENSE_RANK() OVER (PARTITION BY subscription_id ORDER BY id) AS chronological_transaction_rank
	FROM "dumps"."dev"."transactions"
	WHERE TRUE
	AND category = 'monthly'
	AND amount > 0
)

, substract_partial_and_full_refunds AS (
    SELECT
	    drip_subscription_id
        , drip_first_payment_value_mrr
	    , CASE
	       WHEN (drip_first_payment_value_mrr = drip_amount_refunded AND chronological_transaction_rank = 1
	            AND LEAD(drip_first_payment_value_mrr) OVER (PARTITION BY drip_subscription_id ORDER BY chronological_transaction_rank) IS NOT NULL)
	            OR
	            ((drip_first_payment_value_mrr - drip_amount_refunded) < 10 AND chronological_transaction_rank = 1
                AND LEAD(drip_first_payment_value_mrr) OVER (PARTITION BY drip_subscription_id ORDER BY chronological_transaction_rank) IS NOT NULL)
                THEN LEAD(drip_first_payment_value_mrr) OVER (PARTITION BY drip_subscription_id ORDER BY chronological_transaction_rank)
           WHEN drip_first_payment_value_mrr > drip_amount_refunded AND drip_amount_refunded > 0 AND chronological_transaction_rank = 1
                THEN drip_first_payment_value_mrr - drip_amount_refunded
           WHEN drip_amount_refunded = 0 AND chronological_transaction_rank = 1
	            THEN drip_first_payment_value_mrr
	        ELSE NULL
          END AS drip_first_paymant_value_mrr_2nd
        , drip_amount_refunded
        , chronological_transaction_rank

FROM subscription_transactions_ordered_chronologically
WHERE TRUE
)

SELECT
    drip_subscription_id
    , CASE
        WHEN drip_first_paymant_value_mrr_2nd IS NULL
            THEN 0
        WHEN drip_first_paymant_value_mrr_2nd = 1
            THEN 0
        ELSE drip_first_paymant_value_mrr_2nd
      END as drip_first_payment_value_mrr
FROM substract_partial_and_full_refunds
WHERE TRUE
AND chronological_transaction_rank = 1