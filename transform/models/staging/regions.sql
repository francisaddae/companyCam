--Modeling Region table
WITH region_table AS(
    SELECT
        ROW_NUMBER() OVER(ORDER BY region) AS region_id,
        region
    FROM {{ref( 'commerce') }}
    GROUP BY region
)
SELECT *
FROM region_table