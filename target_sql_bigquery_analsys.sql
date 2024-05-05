-- Data type of all columns in the "customers" table.
SELECT * FROM `bigquery-dinesh.target_business_case.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'customers'

-- Get the time range between which the orders were placed.
SELECT MIN(order_purchase_timestamp) AS start_date, MAX(order_purchase_timestamp) AS last_date
FROM bigquery-dinesh.target_business_case.orders
;
-- Count the Cities & States of customers who ordered during the given period.
SELECT COUNT(DISTINCT customer_state) as no_of_states, COUNT(DISTINCT customer_city) as no_of_cities
FROM bigquery-dinesh.target_business_case.customers
LIMIT 10

-- Count tht cities per state during th given period
SELECT customer_state, count(DISTINCT customer_city)
FROM bigquery-dinesh.target_business_case.customers
GROUP BY customer_state
ORDER BY customer_state
LIMIT 10

-- 2.In-depth Exploration
-- 2.1 Is there a growing trend in the no. of orders placed over the past years?
SELECT DATE_TRUNC(order_purchase_timestamp, YEAR) as date_year, COUNT(order_id) as no_of_order
FROM bigquery-dinesh.target_business_case.orders
where lower(order_status) not in  ('unavailable','canceled')
group by 1
order by 1
-- 2.1 Is there a growing tren in the no. of orders placed over the months

SELECT DATE_TRUNC(order_purchase_timestamp, MONTH) as date_month, COUNT(order_id) as no_of_order
FROM bigquery-dinesh.target_business_case.orders
where lower(order_status) not in  ('unavailable','canceled')
group by 1
order by 1

-- 2.2. Can we see some kind of monthly seasonality in terms of the no. of orders being placed?
WITH cte AS (
  SELECT order_id, customer_id, order_purchase_timestamp, 
    CASE
      WHEN EXTRACT(MONTH FROM order_purchase_timestamp) BETWEEN 1 AND 3 THEN 'Q1'
      WHEN EXTRACT(MONTH FROM order_purchase_timestamp) BETWEEN 4 AND 6 THEN 'Q2'
      WHEN EXTRACT(MONTH FROM order_purchase_timestamp) BETWEEN 7 AND 9 THEN 'Q3'
      ELSE 'Q4'
    END AS quarter,
    EXTRACT(YEAR FROM order_purchase_timestamp) AS year
  FROM bigquery-dinesh.target_business_case.orders
  where lower(order_status) not in  ('unavailable','canceled')
)

SELECT distinct year, quarter, COUNT(order_id) as num_of_order, concat(year, '-', quarter) as year_quarter
from cte
group by year, quarter
order by year, quarter

-- During what time of the day, do the Brazilian customers mostly place their orders? (Dawn, Morning, Afternoon or Night)
-- 0-6 hrs : Dawn
-- 7-12 hrs : Mornings
-- 13-18 hrs : Afternoon
-- 19-23 hrs : Night
WITH tod as (
  SELECT order_id, customer_id, order_purchase_timestamp,
    CASE
      WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 0 AND 6 THEN 'Dawn'
      WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 7 AND 12 THEN 'Mornings'
      WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 13 AND 18 THEN 'Afternoon'
      WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 19 AND 23 THEN 'Night'
    END AS time_of_day
  FROM bigquery-dinesh.target_business_case.orders
  where lower(order_status) not in  ('unavailable','canceled')
)
SELECT distinct time_of_day,COUNT(order_id) over(partition by time_of_day) AS no_of_orders
FROM tod
ORDER BY time_of_day DESC

select * from bigquery-dinesh.target_business_case.orders
order by order_purchase_timestamp desc
-- Evolution of E-commerce orders in the Brazil region:
-- 3.1 Get the month on month no. of orders placed in each state.

WITH cus_order_data as (
  SELECT c.*, o.order_id, o.order_purchase_timestamp
  FROM bigquery-dinesh.target_business_case.customers c
  join bigquery-dinesh.target_business_case.orders o
  on c.customer_id = o.customer_id
  where lower(o.order_status) not in  ('unavailable','canceled') 
)
-- ,
-- temp AS (
  SELECT distinct FORMAT_DATE('%B', order_purchase_timestamp) as month, DATE_TRUNC(order_purchase_timestamp, MONTH) as date_month, COUNT(order_id) over(partition by DATE_TRUNC(order_purchase_timestamp, MONTH), customer_state) as no_of_orders, customer_state
  FROM cus_order_data
  order by customer_state, date_month
-- )
-- select sum(no_of_orders)
-- from temp


-- 3.2 How are the customers distributed across all the states?

SELECT customer_state, COUNT(customer_id) AS no_of_cust_per_state
FROM bigquery-dinesh.target_business_case.customers
GROUP BY customer_state
ORDER BY customer_state

-- Get the % increase in the cost of orders from year 2017 to 2018 (include months between Jan to Aug only).
-- You can use the "payment_value" column in the payments table to get the cost of orders.
WITH order_date AS(
    SELECT o.order_id,
      o.order_purchase_timestamp, p.payment_value,
      EXTRACT(YEAR FROM o.order_purchase_timestamp) as year
    FROM bigquery-dinesh.target_business_case.orders o 
    JOIN bigquery-dinesh.target_business_case.payments p
    ON o.order_id = p.order_id
    WHERE (o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-08-31') OR (o.order_purchase_timestamp BETWEEN '2018-01-01' AND '2018-08-31') AND (lower(order_status) not in  ('unavailable','canceled'))

)

SELECT year, total_order_value, (total_order_value - previous_year_value) * 100 / previous_year_value as pct_change
FROM (
  SELECT year,
  SUM(payment_value) as total_order_value,
  lag(SUM(payment_value), 1) over(order by year) as previous_year_value

  FROM order_date
  group by year
)



-- 4.2 Calculate the Total & Average value of order price for each state.
SELECT c.customer_state, 
  COUNT(oi.order_id) AS orders_per_state,
  ROUND(sum(oi.price), 2) AS total_value,
  ROUND(AVG(oi.price), 2) AS avg_value
FROM bigquery-dinesh.target_business_case.customers c
JOIN bigquery-dinesh.target_business_case.orders o
ON c.customer_id = o.customer_id
JOIN bigquery-dinesh.target_business_case.order_items oi
ON o.order_id = oi.order_id
WHERE lower(order_status) not in  ('unavailable','canceled')
GROUP BY c.customer_state
ORDER BY c.customer_state

-- 103886

SELECT COUNT(DISTINCT order_id) FROM bigquery-dinesh.target_business_case.payments


-- 4.3 Calculate the Total & Average value of order freight for each state.
SELECT c.customer_state,
  COUNT(oi.order_id) as no_of_orders,
  ROUND(SUM(oi.freight_value), 2) as total_freight_value_per_state,
  ROUND(AVG(oi.freight_value), 2) as avg_freight_value_per_state

FROM bigquery-dinesh.target_business_case.customers c
JOIN bigquery-dinesh.target_business_case.orders o 
ON c.customer_id = o.customer_id
JOIN bigquery-dinesh.target_business_case.order_items oi
ON o.order_id = oi.order_id
WHERE lower(order_status) not in  ('unavailable','canceled')
GROUP BY c.customer_state
order by c.customer_state

-- 5.1 Find the no. of days taken to deliver each order from the order’s purchase date as delivery time.
-- Also, calculate the difference (in days) between the estimated & actual delivery date of an order.
-- Do this in a single query.

-- You can calculate the delivery time and the difference between the estimated & actual delivery date using the given formula:
-- time_to_deliver = order_delivered_customer_date - order_purchase_timestamp
-- diff_estimated_delivery = order_delivered_customer_date - order_estimated_delivery_date
with days_to_deliver AS(
  SELECT order_id, order_purchase_timestamp,
  DATETIME_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) as time_to_deliver,
  DATETIME_DIFF(order_delivered_customer_date, order_estimated_delivery_date, DAY) as diff_estimated_delivery,
  CASE 
    WHEN DATETIME_DIFF(order_delivered_customer_date, order_estimated_delivery_date, DAY) <= 0 THEN 'YES'
    ELSE 'NO'
  END AS delivery_on_time
  FROM bigquery-dinesh.target_business_case.orders
  WHERE order_status = 'delivered'
  order by order_purchase_timestamp
)

-- SELECT * FROM days_to_deliver

-- SELECT count(*) as no_of_orders_gt_estimated_date
-- FROM days_to_deliver
-- where delivery_on_time = 'YES'

-- SELECT count(*) as no_of_orders_le_estimated_date
-- FROM days_to_deliver
-- where delivery_on_time = 'NO'

SELECT delivery_on_time, count(*) as no_of_order
FROM days_to_deliver
GROUP BY delivery_on_time

-- Find out the top 5 states with the highest & lowest average freight value.
WITH afv AS(
  SELECT c.customer_state,
    ROUND(SUM(oi.freight_value), 2) as total_freight_value_per_state,
    ROUND(AVG(oi.freight_value), 2) as avg_freight_value_per_state,
    DENSE_RANK() OVER(order by AVG(oi.freight_value) DESC) as freight_value_usg_rnk,
    DENSE_RANK() OVER(order by AVG(oi.freight_value) ASC) as freight_value_usg_rnk_asc
  FROM bigquery-dinesh.target_business_case.customers c
  JOIN bigquery-dinesh.target_business_case.orders o 
  ON c.customer_id = o.customer_id
  JOIN bigquery-dinesh.target_business_case.order_items oi
  ON o.order_id = oi.order_id
  GROUP BY c.customer_state
  order by c.customer_state
)
select customer_state, freight_value_usg_rnk

from afv
where freight_value_usg_rnk <=5 or freight_value_usg_rnk_asc <=5
order by freight_value_usg_rnk


-- 5.3 Find out the top 5 states with the highest & lowest average delivery time.
WITH dr AS (
  select 
    c.customer_state
    ,AVG(DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)) AS time_to_deliver
    ,AVG(DATETIME_DIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date, DAY)) AS diff_estimated_delivery
    ,DENSE_RANK() OVER(ORDER BY AVG(DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY))) AS avg_delivery_rnk
    ,DENSE_RANK() OVER(ORDER BY AVG(DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)) DESC) AS avg_delivery_rnk_desc
  FROM bigquery-dinesh.target_business_case.customers c
  JOIN bigquery-dinesh.target_business_case.orders o
  ON c.customer_id = o.customer_id
  GROUP BY c.customer_state
  ORDER BY c.customer_state
)

SELECT customer_state,
  avg_delivery_rnk,
  CASE 
    WHEN avg_delivery_rnk <= 5 THEN 'lowest 5'
    WHEN avg_delivery_rnk_desc <=5 THEN 'highest 5' 
  END AS delivery_time_rank
FROM dr
WHERE  avg_delivery_rnk <= 5 OR avg_delivery_rnk_desc <=5
ORDER BY avg_delivery_rnk


-- 5.4 Find out the top 5 states where the order delivery is really fast as compared to the estimated date of delivery.
-- You can use the difference between the averages of actual & estimated delivery date to figure out how fast the delivery was for each state.
WITH fast_deliver_report AS (
  SELECT c.customer_state
    ,ROUND(AVG(DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)),2) AS delivered_time
    ,ROUND(AVG(DATETIME_DIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp, DAY)),2) AS expected_deliver_avg
    ,ROUND(AVG(DATETIME_DIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp, DAY))-
    AVG(DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)),2) AS avg_date_delivered_before_expected
    ,DENSE_RANK() over(ORDER BY ROUND(AVG(DATETIME_DIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp, DAY))-
    AVG(DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)),2) DESC) as fast_delivery_rank
  FROM bigquery-dinesh.target_business_case.customers c
  JOIN bigquery-dinesh.target_business_case.orders o
  ON c.customer_id = o.customer_id
  GROUP BY c.customer_state
  order BY delivered_time
)

SELECT customer_state
  ,delivered_time as avg_delivered_in
  ,expected_deliver_avg as avg_expected_deliver_in
  ,avg_date_delivered_before_expected as avg_delievered_before_expected
 FROM fast_deliver_report
 WHERE fast_delivery_rank <= 5
order by fast_delivery_rank

 -- 6.1 Find the month on month no. of orders placed using different payment types.
 SELECT 
  date_trunc(o.order_purchase_timestamp, month) as date_month
  ,p.payment_type
  ,count(o.order_id) as no_of_orders
  

 FROM bigquery-dinesh.target_business_case.orders o
 JOIN bigquery-dinesh.target_business_case.payments p
 ON p.order_id = o.order_id
 GROUP BY date_month, p.payment_type
 order by date_month, p.payment_type

-- 6.2 Find the no. of orders placed on the basis of the payment installments that have been paid.


with cte as (
  select o.*, p.payment_installments,
  max(o.order_purchase_timestamp) over() as last_recorded_date
  FROM bigquery-dinesh.target_business_case.orders o
  JOIN bigquery-dinesh.target_business_case.payments p
  ON p.order_id = o.order_id
  WHERE lower(o.order_status) = 'delivered'
),

paid_orders as (

  select order_id
    ,order_purchase_timestamp
    ,payment_installments
    ,last_recorded_date
    ,DATE_DIFF(last_recorded_date, order_purchase_timestamp, DAY) / 30 AS months_since_last_order

  from cte
  order by order_purchase_timestamp
)

select payment_installments,
  count(distinct order_id) as paid_order_count
from paid_orders
where payment_installments <= months_since_last_order
group by payment_installments
