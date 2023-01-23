WITH transactions AS (
    select id transaction_id
         , subscription_id
    from "dumps"."dev"."transactions"
    WHERE '2022-10-01' <= created_at -- TODO: convert this to an incremental model for speed
      AND amount > 0
    )
   , invoice_item_allocations AS (
    SELECT transaction_id
         , amount
         , credit
         , invoice_item_id
         , category
    FROM transactions
         LEFT JOIN "dumps"."dev"."invoice_item_allocations" iia USING (transaction_id)
         INNER JOIN "dumps"."dev"."invoice_items" ii ON ii.id = iia.invoice_item_id
    )
   , invoice_items AS (
    SELECT DISTINCT invoice_item_id
                  , category
    FROM invoice_item_allocations
    )
   , paid_amounts AS (
    SELECT transaction_id
         , SUM(CASE WHEN category = 'sms_monthly_service' THEN amount ELSE 0 END) sms_paid_amount
         , SUM(CASE WHEN category = 'monthly_service' THEN amount ELSE 0 END)     email_paid_amount
    FROM invoice_item_allocations
    WHERE credit = FALSE
    GROUP BY
        transaction_id
    )
   , tax_amounts AS (
    SELECT tax_iia.transaction_id
         , SUM(CASE WHEN ii.category = 'sms_monthly_service' THEN tax_iia.amount ELSE 0 END) sms_tax_amount
         , SUM(CASE WHEN ii.category = 'monthly_service' THEN tax_iia.amount ELSE 0 END)     email_tax_amount
    FROM "dumps"."dev"."invoice_item_allocations" tax_iia
         INNER JOIN "dumps"."dev"."invoice_items" tax_ii ON tax_iia.invoice_item_id = tax_ii.id
         INNER JOIN invoice_items ii ON tax_ii.taxable_item_id = ii.invoice_item_id
    WHERE tax_iia.credit = FALSE
    GROUP BY tax_iia.transaction_id
    )
--     select * from tax_amounts;
   , credits_used AS (
    SELECT transaction_id
         , SUM(CASE WHEN ii.category = 'sms_monthly_service' THEN credit_iia.amount ELSE 0 END) sms_credits_used
         , SUM(CASE WHEN ii.category = 'monthly_service' THEN credit_iia.amount ELSE 0 END)     email_credits_used
    FROM "dumps"."dev"."invoice_item_allocations" credit_iia
         INNER JOIN invoice_items ii USING (invoice_item_id)
    WHERE credit_iia.credit = TRUE
    GROUP BY transaction_id
    )
--     select * from credits_used;
   , sms_totals as (
    select transaction_id
         , subscription_id
         , 'sms_monthly_service'                                  category
         , ISNULL(sms_paid_amount, 0) + ISNULL(sms_tax_amount, 0) amount_cents
         , ISNULL(sms_tax_amount, 0)                              tax_cents
         , ISNULL(sms_credits_used, 0)                            credits_used
    from transactions
         LEFT JOIN paid_amounts USING (transaction_id)
         LEFT JOIN tax_amounts USING (transaction_id)
         LEFT JOIN credits_used USING (transaction_id)
    )
   , email_totals as (
    select transaction_id
         , subscription_id
         , 'monthly_service'                                          category
         , ISNULL(email_paid_amount, 0) + ISNULL(email_tax_amount, 0) amount_cents
         , ISNULL(email_tax_amount, 0)                                tax_cents
         , ISNULL(email_credits_used, 0)                              credits_used
    from transactions
         LEFT JOIN paid_amounts USING (transaction_id)
         LEFT JOIN tax_amounts USING (transaction_id)
         LEFT JOIN credits_used USING (transaction_id)
    )
--  select* from email_totals;
   , totals AS (
    select *
    from sms_totals
    UNION ALL
    select *
    from email_totals
    )
select *
from totals
order by
    transaction_id, category