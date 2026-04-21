# ── Cell 1: Setup ─────────────────────────────────────────────────────────────
%idle_timeout 2880
%glue_version 3.0
%worker_type G.1X
%number_of_workers 5

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ── Cell 2: Load raw tables from Glue Catalog ──────────────────────────────────
dy_orders = glueContext.create_dynamic_frame.from_catalog(database='imba-tobby-17a', table_name='orders')
df_orders = dy_orders.toDF()
df_orders.createOrReplaceTempView('orders')
df_orders.show()

dy_order_products = glueContext.create_dynamic_frame.from_catalog(database='imba-tobby-17a', table_name='order_products')
df_order_products = dy_order_products.toDF()
df_order_products.createOrReplaceTempView('order_products')
df_order_products.show()

# ── Cell 3: Create order_products_prior ────────────────────────────────────────
df_opp = spark.sql("""
    SELECT a.*,
           b.product_id,
           b.add_to_cart_order,
           b.reordered
    FROM orders a
    JOIN order_products b ON a.order_id = b.order_id
    WHERE a.eval_set = 'prior'
""")
df_opp.createOrReplaceTempView('order_products_prior')
df_opp.show()

df_opp.write.mode('overwrite').format('parquet').save('s3://imba-tobby-17a/features/order_products_prior_sv/')

# ── Cell 4: user_features_1 ───────────────────────────────────────────────────
df_uf1 = spark.sql("""
    SELECT user_id,
           Max(order_number)           AS user_orders,
           Sum(days_since_prior_order) AS user_period,
           Avg(days_since_prior_order) AS user_mean_days_since_prior
    FROM orders
    GROUP BY user_id
""")
df_uf1.show()

df_uf1.write.mode('overwrite').format('parquet').save('s3://imba-tobby-17a/features/user_features_1_sv/')

# ── Cell 5: user_features_2 ───────────────────────────────────────────────────
df_uf2 = spark.sql("""
    SELECT user_id,
           Count(*)                   AS user_total_products,
           Count(DISTINCT product_id) AS user_distinct_products,
           Sum(CASE WHEN reordered = 1 THEN 1 ELSE 0 END)
             / Cast(Sum(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS DOUBLE)
                                      AS user_reorder_ratio
    FROM order_products_prior
    GROUP BY user_id
""")
df_uf2.show()

df_uf2.write.mode('overwrite').format('parquet').save('s3://imba-tobby-17a/features/user_features_2_sv/')

# ── Cell 6: up_features ───────────────────────────────────────────────────────
df_up = spark.sql("""
    SELECT user_id,
           product_id,
           Count(*)               AS up_orders,
           Min(order_number)      AS up_first_order,
           Max(order_number)      AS up_last_order,
           Avg(add_to_cart_order) AS up_average_cart_position
    FROM order_products_prior
    GROUP BY user_id, product_id
""")
df_up.show()

df_up.write.mode('overwrite').format('parquet').save('s3://imba-tobby-17a/features/up_features_sv/')

# ── Cell 7: prd_features ──────────────────────────────────────────────────────
df_prd = spark.sql("""
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
""")
df_prd.show()

df_prd.write.mode('overwrite').format('parquet').save('s3://imba-tobby-17a/features/prd_features_sv/')
