/* Filename: athena-join-prior.sql
   Purpose: Join orders and order_products to create a flattened prior_orders dataset.
   Method: CTAS (Create Table As Select)
*/

CREATE TABLE imba_db.joined_prior_orders AS
SELECT 
    o.order_id, 
    o.user_id, 
    op.product_id
FROM "imba_db"."orders" o
JOIN "imba_db"."order_products" op ON o.order_id = op.order_id
WHERE o.eval_set = 'prior';


-- =================================================================
-- Instacart Market Basket Analysis - 特征工程 (Athena/SQL 版本)
-- 目的: 基于历史订单 (prior) 提取用户、产品及交叉特征
-- =================================================================

-- 1. 基础宽表: 将订单信息与产品明细合并
-- 使用 Parquet 格式存储以优化后续查询性能并降低扫描成本
CREATE TABLE order_products_prior 
WITH (
    external_location = 's3://你的存储桶名称/features/order_products_prior/', 
    format = 'parquet'
) AS 
SELECT 
    a.*,
    b.product_id,
    b.add_to_cart_order,
    b.reordered
FROM orders a
JOIN order_products b ON a.order_id = b.order_id
WHERE a.eval_set = 'prior';


-- 2. 用户维度特征 1 (User Features 1): 统计用户的下单规律
CREATE TABLE user_features_1 AS
SELECT 
    user_id,
    MAX(order_number) AS user_orders,               -- 用户总订单数
    SUM(days_since_prior_order) AS user_period,     -- 用户活跃总天数 (从第一单到最后一单)
    AVG(days_since_prior_order) AS user_mean_days_since_prior -- 平均下单周期 (频率)
FROM orders
GROUP BY user_id;


-- 3. 用户维度特征 2 (User Features 2): 统计用户的购买偏好
CREATE TABLE user_features_2 AS
SELECT 
    user_id,
    COUNT(*) AS user_total_products,                -- 用户累计购买的商品总件数
    COUNT(DISTINCT product_id) AS user_distinct_products, -- 用户购买过的去重商品数
    -- 计算复购率: 复购商品数 / (总商品数 - 首单商品数)
    SUM(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) / 
        CAST(SUM(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS DOUBLE) AS user_reorder_ratio
FROM order_products_prior
GROUP BY user_id;


-- 4. 用户-产品交叉特征 (User-Product Features): 刻画用户对特定产品的偏好
CREATE TABLE up_features AS
SELECT 
    user_id,
    product_id,
    COUNT(*) AS up_orders,                          -- 该用户买过几次该产品
    MIN(order_number) AS up_first_order,            -- 第一次买该产品是在第几个订单
    MAX(order_number) AS up_last_order,             -- 最后一次买该产品是在第几个订单
    AVG(add_to_cart_order) AS up_average_cart_position -- 该产品通常被放在购物车第几个位置
FROM order_products_prior
GROUP BY user_id, product_id;


-- 5. 产品维度特征 (Product Features): 刻画产品本身的受欢迎程度
-- 使用子查询和窗口函数 (RANK) 来计算用户购买该产品的次序
CREATE TABLE prd_features AS
SELECT 
    product_id,
    COUNT(*) AS prod_orders,                        -- 该产品总销量
    SUM(reordered) AS prod_reorders,                -- 该产品总复购次数
    -- 统计该产品作为“首购”和“二次回购”的次数，用于衡量产品的获客与留存能力
    SUM(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders,
    SUM(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
FROM (
    SELECT 
        product_id,
        reordered,
        -- 为每个用户购买的每个产品按订单顺序打上序号
        RANK() OVER (PARTITION BY user_id, product_id ORDER BY order_number) AS product_seq_time
    FROM order_products_prior
)
GROUP BY product_id;
