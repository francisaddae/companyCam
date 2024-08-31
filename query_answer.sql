--  What region has the most sales measures by N amount of sales?
SELECT
    region, COUNT(transaction_id) AS number_of_sales
FROM prod.ecommerce
GROUP BY region
ORDER BY number_of_sales DESC;

--  What region has the highest average sales by revenue
SELECT
    region, AVG(quantity * price) AS sales_by_n_amount
FROM prod.ecommerce
GROUP BY region
ORDER BY sales_by_n_amount DESC;

--  Which category for each region should we invest more marketing spend for?
WITH category_region_revenue AS(
    SELECT
        region,
        category,
        SUM(quantity * price) AS total_revenue,
        RANK() OVER(PARTITION BY region ORDER BY total_revenue DESC) AS cat_reg_decision
    FROM prod.ecommerce
    GROUP BY
        region,
        category
)
SELECT
    category,
    region,
    total_revenue
FROM category_region_revenue
WHERE cat_reg_decision = 1
ORDER BY region;