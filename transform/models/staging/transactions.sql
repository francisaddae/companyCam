--Modeling Transaction table
WITH transactions_table AS(
    SELECT
        ROW_NUMBER() OVER(ORDER BY transaction_id) AS transactions_id,
        transaction_id
    FROM {{ref( 'commerce') }}
    GROUP BY transaction_id
)
SELECT *
FROM transactions_table