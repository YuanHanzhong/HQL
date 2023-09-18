select
    t1.category_id
  , sum( if( t1.create_date = '2021-10-01', 1, 0 ) ) as `第1天`

from (
         select distinct
             si.category_id
           , od.create_date
           , si.name
         from order_detail od
              join
              sku_info     si
              on
                  od.sku_id = si.sku_id
         where
             od.create_date >= '2021-10-01' and od.create_date <= '2021-10-07'
     ) t1
group by t1.category_id
;
