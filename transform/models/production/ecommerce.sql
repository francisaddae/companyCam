WITH reporting_data AS(
    SELECT
        trans.transaction_id,
        cust.customer_id,
        prod.product_id,
        trans.date,
        trans.quantity,
        trans.price,
        catgs.category,
        pays.payment_method,
        regs.region
    FROM {{ ref('transactions') }} AS trans
    INNER JOIN {{ ref('customers') }} AS cust
        ON trans.customers_id = cust.customers_id
    INNER JOIN {{ ref('products') }} AS prod
        ON trans.products_id = prod.products_id
    INNER JOIN {{ ref('payments') }} AS pays
        ON trans.payment_method_id = pays.payment_method_id
    INNER JOIN {{ ref('categories') }} AS catgs
        ON trans.category_id = catgs.category_id
    INNER JOIN {{ ref('regions') }} AS regs
        ON trans.region_id = regs.region_id
),
final AS (
    SELECT
        transaction_id,
        customer_id,
        product_id,
        date,
        quantity,
        price,
        category,
        payment_method,
        region
    FROM reporting_data
)
SELECT * FROM final