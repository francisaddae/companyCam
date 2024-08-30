--Modeling Categories table
WITH category_table AS(
    SELECT
        ROW_NUMBER() OVER(ORDER BY category) AS category_id,
        category
    FROM {{ref( 'commerce') }}
    GROUP BY category
)
SELECT *
FROM category_table