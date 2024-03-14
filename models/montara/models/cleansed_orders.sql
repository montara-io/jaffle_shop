--start_query
WITH orders AS (SELECT * FROM {{ ref('orders') }})

select * from orders
--original_sql
--select * from orders