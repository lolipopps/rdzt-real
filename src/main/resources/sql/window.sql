SELECT t1.order_id, t1.item, t1.order_time, t2.order_id, t2.create_time,t2.state
from (
         SELECT order_id
              , item
              , order_time
              , amount
              , ROW_NUMBER() OVER ( PARTITION BY item ORDER BY order_time desc) AS rownum
         FROM orders
     ) t1
         left join
     (
         SELECT order_id
              , item
              , create_time
              , state
              , ROW_NUMBER() OVER ( PARTITION BY item ORDER BY create_time desc) AS rownum
         FROM orders_detail
     ) t2
     on t1.item = t2.item
WHERE t1.rownum = 1 and t2.rownum = 1;
