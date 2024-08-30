--Modeling Payment_Method table
WITH payments_table AS(
    SELECT
        ROW_NUMBER() OVER(ORDER BY payment_method) AS payment_method_id,
        payment_method
    FROM {{ref( 'commerce') }}
    GROUP BY payment_method
)
SELECT *
FROM payments_table