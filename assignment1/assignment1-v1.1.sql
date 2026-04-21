-- Assignment 2 - Athena SQL Queries
-- S3 Bucket: imba-tobby-17a
-- Database: prd

-- ── Step 1: Create order_products_prior ───────────────────────────────────────
CREATE TABLE order_products_prior
WITH (
  external_location = 's3://imba-tobby-17a/features/order_products_prior/',
  format = 'parquet'
) AS (
  SELECT a.*,
         b.product_id,
         b.add_to_cart_order,
         b.reordered
  FROM orders a
  JOIN order_products b ON a.order_id = b.order_id
  WHERE a.eval_set = 'prior'
);

-- ── Step 2: user_features_1 ───────────────────────────────────────────────────
CREATE TABLE user_features_1
WITH (
  external_location = 's3://imba-tobby-17a/features/user_features_1/',
  format = 'parquet'
) AS (
  SELECT user_id,
         Max(order_number)           AS user_orders,
         Sum(days_since_prior_order) AS user_period,
         Avg(days_since_prior_order) AS user_mean_days_since_prior
  FROM orders
  GROUP BY user_id
);

-- ── Step 3: user_features_2 ───────────────────────────────────────────────────
CREATE TABLE user_features_2
WITH (
  external_location = 's3://imba-tobby-17a/features/user_features_2/',
  format = 'parquet'
) AS (
  SELECT user_id,
         Count(*)                   AS user_total_products,
         Count(DISTINCT product_id) AS user_distinct_products,
         Sum(CASE WHEN reordered = 1 THEN 1 ELSE 0 END)
           / Cast(Sum(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS DOUBLE)
                                    AS user_reorder_ratio
  FROM order_products_prior
  GROUP BY user_id
);

-- ── Step 4: up_features ───────────────────────────────────────────────────────
CREATE TABLE up_features
WITH (
  external_location = 's3://imba-tobby-17a/features/up_features/',
  format = 'parquet'
) AS (
  SELECT user_id,
         product_id,
         Count(*)               AS up_orders,
         Min(order_number)      AS up_first_order,
         Max(order_number)      AS up_last_order,
         Avg(add_to_cart_order) AS up_average_cart_position
  FROM order_products_prior
  GROUP BY user_id, product_id
);

-- ── Step 5: prd_features ──────────────────────────────────────────────────────
CREATE TABLE prd_features
WITH (
  external_location = 's3://imba-tobby-17a/features/prd_features/',
  format = 'parquet'
) AS (
  SELECT product_id,
         Count(*)                                               AS prod_orders,
         Sum(reordered)                                         AS prod_reorders,
         Sum(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders,
         Sum(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
  FROM (
    SELECT *,
           Rank() OVER (PARTITION BY user_id, product_id ORDER BY order_number) AS product_seq_time
    FROM order_products_prior
  )
  GROUP BY product_id
);

-- ── Preview results (run each separately) ─────────────────────────────────────
-- SELECT * FROM order_products_prior LIMIT 20;
-- SELECT * FROM user_features_1 LIMIT 20;
-- SELECT * FROM user_features_2 LIMIT 20;
-- SELECT * FROM up_features LIMIT 20;
-- SELECT * FROM prd_features LIMIT 20;
