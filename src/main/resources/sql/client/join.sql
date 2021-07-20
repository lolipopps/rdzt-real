SELECT t1.order_id
     , t1.item
     , t1.order_time
     , t1.amount
     , t2.state
FROM orders t1
LEFT JOIN orders_detail t2
ON t1.item = t2.item and t1.order_id = t2.order_id