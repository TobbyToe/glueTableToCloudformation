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