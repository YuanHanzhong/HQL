-- 2.1 查询累积销量排名第二的商品
-- 累计销量
select sku_id, amount_sku, dense_rank_amount_sku
from (
         -- NOTE order by可以多项, 排名的话常常跟着 desc
         select sku_id, amount_sku, dense_rank( ) over (order by amount_sku desc) as dense_rank_amount_sku
         from (
                  select sku_id, sum( sku_num ) as amount_sku
                  from order_detail od
                  group by sku_id
              ) t1
     ) t2
where
    dense_rank_amount_sku = 2;

select sku_id
from (
         select *, dense_rank( ) over (order by sum_sku_num desc) as dense_rank_sku_num
         from (
                  --  要什么取什么, 不是越多越好, 而是越少越好
                  select sku_id, sum( sku_num ) as sum_sku_num
                  from order_detail od
                       --  直接要 sum 的时候就没有必要开窗, 直接 group by, 书写简单, 运行还高效.
                  group by sku_id
                  order by sum_sku_num
              ) sum_sku_num_table
     ) dense_rank_sku_table
where
    dense_rank_sku_num = 2
;
-- 2.2 [课堂讲解]查询至少连续三天下单的用户
-- NOTE 以后就用 row_number

-- 2023年09月06日21:48:46 如果使用 dense_rank 是不是就不用去重了?
-- 不去重的话, 用 rank 就一点也不对了. 用 dense_rank 还对一点.

select
    user_id
  , create_date
  , dense_rank( ) over (partition by user_id order by create_date) as rank_create_date
from order_info oi

-- lag 和rank 的本质区别

-- 2023年09月06日, 使用 rank
select user_id
from (
         select user_id, date_sub( create_date, rank_create_date ) as flag
         from (
                  select
                      user_id
                    , create_date
                    , rank( ) over (partition by user_id order by create_date) as rank_create_date
                  from (
                           select distinct user_id, create_date
                           from order_info oi
                       ) t1
              ) t2
     ) t3
group by user_id
having
    count( flag ) >= 3



-- 有 3 种方式
--      1. lag


-- 使用不去重+dense_rank. 这种方式不行, 会有重复.
select user_id, create_date, date_sub( create_date, dense_rank_create_date )
from (
         select
             user_id
           , create_date
           , dense_rank( ) over (partition by user_id order by create_date) as dense_rank_create_date
         from order_info oi
     ) t1

select user_id, flag, count( * ) as count_flag
from (
         select user_id, create_date, row_number, date_sub( create_date, row_number ) as flag
         from (
                  
                  -- NOTE 最保险的排序就是在开窗函数里, partition by order by都要用
                  select
                      user_id
                    , create_date
                    , row_number( ) over (partition by user_id order by create_date) as row_number
                  from (
                           -- NOTE 连续登录, 首先要考虑去重.
                           select user_id, create_date
                           from order_info oi
                           group by user_id, create_date
                       ) t1
              ) t12
         
         
         order by user_id, create_date
     ) t123
group by user_id, flag
       -- 聚合函数用 having 可以减少一层
having
    count_flag >= 2
;


-- 使用 lag
-- 不一定是连续的,

-- 使用 lag 的第二种方式 , 和另外 2 种方式有本质区别
-- 开窗并不会去重  NOTE
-- 开窗一般写上 partition by order by, 程序健壮 NOTE
select user_id, create_date, datediff( create_date, lag_create_date_3 ) as flag
from (
         select
             user_id
           , create_date
           , lag( create_date, 3 ) over (partition by user_id order by create_date) as lag_create_date_3
         from (
                  select user_id, create_date
                  from order_info oi
                  group by user_id, create_date
              ) t1
     ) t2
where
    -- lag 3, 一定得等于 3
    datediff( create_date, lag_create_date_3 ) = 3;

-- lag(字段) NOTE
-- 开窗函数不能直接放在 where 条件里, 聚合函数可以, 并且放在 having

select user_id, count( flag )
from (
         -- 涉及到日期的时候很特殊, 要用函数, 并且注意格式 NOTE
         select user_id, create_date, lag_create_date, datediff( create_date, lag_create_date ) as flag
         from (
                  select
                      user_id
                    , create_date
                      -- 使用 lag 就是为了要一个递增的东西  方式 1
                    , lag( create_date ) over (partition by user_id order by create_date) as lag_create_date
                  
                  from (
                           -- 去重, 一天只保留一次
                           --  忌讳使用 select *
                           select user_id, create_date
                           from order_info oi
                           group by user_id, create_date
                       ) t1
              ) t12
     ) t123
     -- 使用 having 必须要有 group by
group by user_id, flag
having
    count( flag ) >= 4
;


-- 思路 1: 使用lag, 再用 where确定唯一的一个
-- 连续问题, 先去重 NOTE

select user_id, datediff( create_date, lag_date )
from (
         select
             user_id
           , create_date
           , lag( create_date, 3 ) over (partition by user_id order by create_date) as lag_date
         from order_info oi
         group by user_id, create_date
         order by user_id, create_date
     ) original_data
where
    datediff( create_date, lag_date ) = 3
;

-- 思路2: 使用 rank 加递增序号, 再相减
-- 第二次写 2023年08月30日17:47:25
select user_id
from (
         select
             user_id
           , create_date
           , date_sub( create_date,
                       rank( ) over (partition by user_id order by create_date) ) as flag
         from (
                  select user_id, create_date
                  from order_info oi
                  group by user_id, create_date
              ) original_data
     ) flag_table
group by user_id
having
    count( flag ) > 3;

-- 可以多写一层, 不会更复杂, 反而更容易读
select user_id, count( flag )
from (
         select
             user_id
           , date_sub( create_date, rank( ) over (partition by user_id order by create_date) ) as flag
             -- 开窗中的 partition 和给的数据中是否 partition 过没有关系
         from (
                  -- 连续问题, 先去重 NOTE
                  select user_id, create_date
                  from order_info oi
                  group by user_id, create_date
                  order by user_id, create_date
              ) single_data
     ) flag_table
group by user_id
         -- 有 group by, 有 join 的时候用*很容易出错, 不要用* ,慎重用* , 用哪个写哪个.
having
    count( flag ) >= 3
;
;


select user_id
from (
         select
             user_id
           , create_date
             -- 直接开窗比 lag lead 好用很多 NOTE
           , date_sub( create_date, row_number( ) over (partition by user_id order by create_date) ) as flag
         from (
                  select
                      user_id
                    , create_date
                  from order_info
                  group by user_id, create_date
              ) one_date_per_user_table
     ) t2 -- 判断一串日期是否连续：若连续，用这个日期减去它的排名，会得到一个相同的结果 NOTE
group by user_id
         -- group by 的为必要出现在 select里面, 只是借助它进行了筛选 NOTE
         -- 在 SELECT 子句中，除了聚合函数，只能使用出现在 GROUP BY 子句中的字段。NOTE
having
    -- 这一步筛选用的很巧妙
    count( flag ) >= 3 -- 连续下单大于等于三天
;

-- 2.3 查询各品类销售商品的种类数及销量最高的商品

select sku_id, sum_sku_num, dense_rank_amount_sku
from (
         select sku_id, sum_sku_num, dense_rank( ) over (order by sum_sku_num desc) as dense_rank_amount_sku
         from (
                  select sku_id, sum( sku_num ) as sum_sku_num
                  from order_detail od
                  group by sku_id
              ) t1
     ) t2
where
    dense_rank_amount_sku = 1;


-- 2.4 查询用户的累计消费金额及VIP等级
select
    user_id
  , case
        when sum_total_amount < 100000                              then '高级'
        when sum_total_amount < 200000 and sum_total_amount > 10000 then '中级'
    end
from (
         select user_id, sum( total_amount ) as sum_total_amount
         from order_info oi
         group by user_id
     ) t1



select
    user_id
  , create_date
  , sum_money
  , case
        when sum_money > 100000                       then '黄金'
        when sum_money < 100000 and sum_money > 80000 then '白金'
        when sum_money < 80000                        then '普通'
    end as vip_level
from (
         select *, sum( total_amount ) over (partition by user_id order by create_date) as sum_money
         from order_info oi
     ) t1
;

-- 2.5 查询首次下单后第二天连续下单的用户比率 STAR

-- NOTE group by , partition by决定了计算范围
select
        sum( `if`( datediff( second_borrow_date, first_borrow_date ) = 1, 1, 0 ) ) * 100.0 as amount_firstday
  ,     sum( `if`( datediff( second_borrow_date, first_borrow_date ) != 1, 1, 0 ) )        as amount_secondday
from (
         select
             user_id
           , min( create_date ) over (partition by user_id order by create_date) as first_borrow_date
           , max( create_date ) over (partition by user_id order by create_date) as second_borrow_date
         from (
                  select
                      user_id
                    , create_date
                    , row_number( ) over (partition by user_id order by create_date) as row_number_flag
                  from (
                           select distinct user_id, create_date
                           from order_info oi
                       ) t1
              ) t2
         where
             row_number_flag <= 2
     ) t3
;


select user_id, first_day, second_day
from (
         select
             user_id
           , create_date                                                            as first_day
           , lag( create_date, 1 ) over (partition by user_id order by create_date) as second_day
         from (
                  select distinct
                      user_id
                    , create_date
                    , row_number( ) over (partition by user_id order by create_date) as flag
                  
                  from order_info oi
              ) t1
         where
             flag <= 2
     ) t2
where
    datediff( first_day, second_day ) = 1
;


-- 欠妥, 没有考虑到重复的下单
select
    sum( `if`( datediff( lag_date, create_date ) = -1, 1, 0 ) )
    -- NOTE lag的要更小, 因为往后移动了
    -- NOTE 要想不重不漏就是用等于和不等于某个数字, 而不是非得等于某些数字
  , sum( `if`( datediff( lag_date, create_date ) != -1, 1, 0 ) )
    -- ,
    --     datediff(create_date,lag_date)
from (
         select
             user_id
           , create_date
           , rank_create_date
           , lag( create_date, 1, create_date ) over
             (partition by user_id order by create_date) as lag_date
         from (
                  select
                      user_id
                    , create_date
                    , rank( ) over (partition by user_id order by create_date) as rank_create_date
                  from (
                           
                           select distinct user_id, create_date
                           from order_info oi
                       ) t1
              ) t2
         where
             -- 这里直接 rank 还不行, 不能解决不连续的情况
             rank_create_date <= 2
     ) t3
;



-- 参考
-- 这个方法不对, 不应该 count(*), 而是应该用等于 1 和不等于 1 来区分, 因为算的是用户的比例, 而非下单次数的比例
select
    -- 所有的 where 里面的判断都可以放到 if 里面去判断 NOTE
    sum( if( datediff( buy_date_second, buy_date_first ) = 1, 1, 0 ) ) * 100.0
  , sum( if( datediff( buy_date_second, buy_date_first ) != 1, 1, 0 ) )
    --                     / count( * ) * 100

from (
         
         select
             user_id
           , rk
           , min( create_date ) over (partition by user_id order by create_date) as buy_date_first
           , max( create_date ) over (partition by user_id order by create_date) as buy_date_second
           , datediff(
                 max( create_date ) over (partition by user_id order by create_date),
                 min( create_date ) over (partition by user_id order by create_date)
                 )                                                               as flag
         from (
                  -- 添加排序列, 目的就是筛选出首次和第二次
                  select
                      user_id
                    , create_date
                    , rank( ) over (partition by user_id order by create_date) as rk
                  from (
                           -- 去重
                           select
                               user_id
                             , create_date
                           from order_info
                           group by user_id, create_date
                       ) t1
              ) t2
         where
             -- 先筛选, 再聚合, 效率还可以
             rk <= 2
     ) t3;
-- 没有 count if 这个用法 NOTE
select sum( `if`( rank_create_date = 2, 1, 0 ) ) * 100.0 / count( * )
from (
         select user_id, create_date, rank_create_date
         from (
                  select
                      user_id
                    , create_date
                      
                      -- 仅仅用 rank 不能保证连续, 再借 flag, 再减, 再 count 才算连续
                    , rank( ) over (partition by user_id order by create_date) as rank_create_date
                  from (
                           -- 去重日期
                           select distinct user_id, create_date
                           from order_info oi
                       ) t1
              ) t2
         where
             rank_create_date <= 2
     ) t3
;
-- sum(if()) 这个组合真的好用 NOTE

-- 2023年08月30日19:22:54 使用 rank 的方式

-- 如何直接把select count的数拿出来计算?

-- 首次下单,第二天还下单的用户数
select count( user_id )
from (
         select user_id, flag, count( * )
         from (
                  
                  select *, date_sub( create_date, rk ) as flag
                  from (
                           select *, rank( ) over (partition by user_id order by create_date) as rk
                           
                           from (
                                    select user_id, create_date
                                    from order_info oi
                                    group by user_id, create_date
                                ) kill_duplicate_table
                       ) add_rank_talbe
                  where
                      rk <= 2
              ) flag_table
         group by user_id, flag
         having
             count( * ) = 2
         order by user_id, flag
     ) flag_is_2_table
;

-- 2023年08月30日19:01:52 可以看出模式来, 但是很难匹配成功
select *, count( ) over (partition by user_id order by create_date )
from (
         -- NOTE 使用 lag 就不好过滤了, 使用 rank 更好地筛选过滤
         select user_id, create_date, lag( create_date, 1 ) over (partition by user_id order by create_date)
         from order_info oi
         group by user_id, create_date
     ) t1
;

-- 用户总数
select count( user_id ) as count_user
from order_info oi;


--  拿到总的用户数
select count( distinct user_id )
from (
         -- 这里思路错了, 不是连续下单的用户比例, 而是首次, 一个用户只有一次
         select
             user_id
           , create_date
           , lag( create_date, 1 ) over (partition by user_id order by create_date) as lag_one_day
           , rank( ) over (partition by user_id order by create_date)               as rank_create_date
         from order_info oi
              -- group by 主要作用就是去重
         group by user_id, create_date
                -- 没有分层级, 搞乱了
         having
             rank_create_date <= 2
     ) tq
where
    datediff( create_date, lag_one_day ) = 1
;


-- 我的为什么那么复杂, 人家的为什么那么简单, 差在哪里了
--      基础数据, 人家一次搞定, 我弄了好几轮
-- 2.6 每个商品销售首年的年份、销售数量和销售金额

-- 1	2021	51	102000.00
-- 2	2021	302	3020.00
-- 2023年09月04日10:37:05

select sku_id, year_create_date, amount_sale, rank_year, sum_sku_num
from (
         select
             sku_id
           , year_create_date
           , sum( sku_num )                                               as sum_sku_num
           , sum( sale )                                                  as amount_sale
           , rank( ) over (partition by sku_id order by year_create_date) as rank_year
         from (
                  select sku_id, sku_num, sku_num * price as sale, year( create_date ) as year_create_date
                  from order_detail od
                  group by sku_id, sku_num, price, year( create_date )
              ) t1
         group by sku_id, year_create_date
     ) t12
where
    rank_year = 1
;

select *
from order_info oi


select sku_id as `商品 id`, year_create as `首年销售年份`, sum_sku_num as `首年销售量`, sum_year_amount as `首年销售金额`
from (
         select *, rank( ) over (partition by sku_id order by year_create) as rank_year
         from (
                  select
                      sku_id
                    , year_create
                    , sum( sale_amount ) as sum_year_amount
                    , sum( sku_num )     as sum_sku_num
                  from (
                           select
                               year( create_date ) as year_create
                             , sku_id
                             , create_date
                             , price
                             , sku_num
                             , price * sku_num     as sale_amount
                           from order_detail od
                       ) original_table
                  group by sku_id, year_create
              ) t1
     ) t2
where
    rank_year = 1
order by sku_id
;

-- 参考
select
    sku_id
  , year( create_date )
  , sum( sku_num )
  , sum( price * sku_num )
from (
         select
             order_id
           , sku_id
           , price
           , sku_num
           , create_date
           , rank( ) over (partition by sku_id order by year( create_date )) as rk
         from order_detail
     ) t1
where
    rk = 1
group by sku_id, year( create_date )
;

-- 2.7 从订单明细表(order_detail)中筛选出去年总销量小于100的商品及其销量，
-- 假设今天的日期是2022-01-10，不考虑上架时间小于一个月的商品


desc order_detail;
-- 永远都是首选聚合函数 NOTE
select sku_id, sum( sku_num )
from order_detail od
where
    year( create_date ) = '2021' and create_date < date_sub( `current_date`( ), 30 )
group by sku_id
having
    sum( sku_num ) < 100
order by sku_id
;

select
    t1.sku_id
  , name
  , order_num
from (
         select
             sku_id
           , sum( sku_num ) as order_num
         from order_detail
         where
               year( create_date ) = '2021'
           and sku_id in (
                             select sku_id
                             from sku_info
                             where
                                 datediff( '2022-01-10', from_date ) > 30
                         )
         group by sku_id
         having
             sum( sku_num ) < 100
     )        t1
     left join
     sku_info t2
     on t1.sku_id = t2.sku_id;


select sku_id, sku_num, sum( price * sku_num ) over (partition by sku_id)
from (
         -- 基础数据
         select *
         from order_detail od
         where
               year( create_date ) = 2021
           and datediff( '2022-01-10', create_date ) > 100
     ) t1
group by year( create_date )


-- 从用户登录明细表（user_login_detail）中查询每天的新增用户数，若一个用户在某天登录了，
-- 且在这一天之前没登录过，则任务该用户为这一天的新增用户

-- 第一次
select mydate, count( user_id )
from (
         select user_id, date_format( login_ts, 'yyyy-MM-dd' ) as mydate
         from (
                  select user_id, login_ts, rank( ) over (partition by user_id order by login_ts) as rk
                  from user_login_detail uld
              ) t1
         where
             rk = 1
     ) t2
group by mydate


-- 2023年09月02日15:11:03 使用 min 的效率, 最高, 比排序编号效率高很多


select get_day, count( user_id )
from (
         select user_id, date_format( m, 'yyyy-MM-dd' ) as get_day
         from (
                  select user_id, min( login_ts ) as m
                  from user_login_detail uld
                  group by user_id
              ) original_table
     
     ) t2
group by get_day

--   统计每个商品的销量最高的日期

select *
from (
         select
             sku_id
           , create_date
           , sum_sku_num
           , dense_rank( ) over (partition by sku_id order by create_date) as densn_rank_date
         from (
                  where
                      dense_rank_sum_sku_num = 1
                  ) t2
     
     ) t3
where
    densn_rank_date = 1
;


select *
from (
         select
             sku_id
           , create_date
           , sum_sku_num
             
             -- 排序可以一次排多个
             --
           , dense_rank( ) over (partition by sku_id order by sum_sku_num desc,create_date asc ) as dense_rank_sum_sku_num
         from (
                  select sku_id, create_date, sum( sku_num ) as sum_sku_num
                  from order_detail od
                  group by sku_id, create_date
              ) t1
     ) t12
where
    dense_rank_sum_sku_num = 1;



select *
from (
         select
             sku_id
           , create_date
             
             -- 排序可以一次排多个
             --
           , max( sum_sku_num )
             -- todo
         from (
                  select sku_id, create_date, sum( sku_num ) as sum_sku_num
                  from order_detail od
                  group by sku_id, create_date
              ) t1
         group by sku_id, create_date
     ) t12
where
    dense_rank_sum_sku_num = 1;
--  2.10 查询销售件数高于品类平均数的商品
select *
from order_info oi


select sku_id, name, sku_num, avg_sku_num, sum( sku_num )
from (
     
     ) t2
where
    avg_sku_num < sku_num
group by sku_id, name, sku_num, avg_sku_num
;


select sku_id, name, sum( sku_num ) as sum_sku_num, avg_sku_num
from (
         select
             category_id
           , sku_id
           , name
           , sku_num
           , avg( sku_num ) over (partition by category_id) as avg_sku_num
         from (
                  select category_id, od.sku_id, sku_num, name
                  from order_detail od join sku_info si on si.sku_id = od.sku_id
              
              ) original_table
     ) avg_table
group by sku_id, name, avg_sku_num
having
    sum_sku_num > avg_sku_num


-- 2.11 用户注册、登录、下单综合统计 STAR

select t1.user_id, count_login_times, count_order_date
from (
         select
             user_id
           , count( create_date ) as count_order_date
           , sum( total_amount )  as sum_total_amount
         from order_info oi
         group by user_id
     )      t1
     join (
              select
                  user_id
                , count( login_ts ) as count_login_times
              
              from user_login_detail uld
              group by user_id
          ) t2
     on t1.user_id = t2.user_id
;



select
    t1.user_id
  , register_date
  , total_login_count
  , login_count_2021
  , t2.order_amount_2021
  , t2.order_count_2021
from (
         select
             user_id
           , min( login_ts ) over (partition by user_id order by login_ts)            as register_date
           
           , count( * ) over (partition by user_id)                                   as total_login_count
           , sum( if( year( login_ts ) = '2021', 1, 0 ) ) over (partition by user_id) as login_count_2021
         
         from user_login_detail uld
     )      t1
     join (
              select
                  user_id
                , count( order_id )   as order_count_2021
                , sum( total_amount ) as order_amount_2021
              from order_info oi
              where
                  year( create_date ) = 2021
              group by user_id
          ) t2
     on t1.user_id = t2.user_id
;


--     join order_info oi on oi.user_id = uld.user_id
-- group by uld.u

;

select*
from order_info oi


select
    user_id
  , register_date
    -- 开窗不能课 group 一起用 NOTE
  , count( login_ts )      as login_count_2021
  , sum( sku_num )         as order_count_2021
  , sum( sku_num * price ) as order_amount_2021
from (
         select
             oi.user_id
           , login_ts
           , sku_num
           , price
           , min( login_ts ) over (partition by oi.user_id order by login_ts) as register_date
             
             -- NOTE 必须要先计算, 再关联
           , count( 1 ) over (partition by oi.user_id)                        as total_login_count
         from user_login_detail uld join order_info oi on oi.user_id = uld.user_id
                                    join order_detail od on od.create_date = oi.create_date
     ) original_table
where
    year( login_ts ) = '2021'
group by user_id, register_date, total_login_count
;

-- 参考
select
    login.user_id
  , register_date
  , total_login_count
  , login_count_2021
  , order_count_2021
  , order_amount_2021
from (
         select
             user_id
           , min( login_ts )                                   as register_date
           , count( 1 )                                        as total_login_count
           , count( if( year( login_ts ) = '2021', 1, null ) ) as login_count_2021
         from user_login_detail
         group by user_id
     ) login
     join
     (
         select
             user_id
           , count( order_id )   as order_count_2021
           , sum( total_amount ) as order_amount_2021
         from order_info
         where
             year( create_date ) = '2021'
         group by user_id
     ) oi
     on login.user_id = oi.user_id;

select *
from user_login_detail uld join order_info oi on oi.user_id = uld.user_id


-- 2023年09月02日17:01:28

select *
from (
         select
             user_id
           , min( login_ts )                                as register_date
           , count( login_ts )                              as total_login_count
             -- 好用组合 sum(if())
           , sum( `if`( year( login_ts ) = '2021', 1, 0 ) ) as login_count_2021
         
         from (
                  select user_id, login_ts
                  from user_login_detail uld
              ) t1
         group by user_id
         -- NOTE left join
     ) t2 left join
     (
         select
             user_id
             -- 一个 order id 下有多个 sku
           , count( distinct order_id ) as order_count_2021
           , sum( total_amount )        as order_amount_2021
         from order_info oi
         where
             year( create_date ) = 2021
         group by user_id
     
     ) t3
          on t2.user_id = t3.user_id
;

select *
from order_info oi
;

select *
from order_detail od
;

select *
from (
         user_login_detail uld
         )


-- 12. 2.12 查询指定日期的全部商品价格
-- 2.12.1 题目需求
-- 从商品价格修改明细表（sku_price_modify_detail）中查询2021-10-01的全部商品的价格，假设所有商品初始价格默认都是99。

-- 我的 2023年09月07日09:40:39
select sku_id, new_price
from (
         select
             sku_id
           , new_price
           , change_date
             --
           , `if`( datediff( '2021-10-01', change_date ) > 0, new_price, 89 )
           , dense_rank( ) over (partition by sku_id order by change_date desc) as dense_rank_date
         from sku_price_modify_detail spmd
         where
             datediff( '2021-10-01', change_date ) >= 0
     ) t1
where
    dense_rank_date = 1
    
    -- 即使用了排序, 但是出现的仍然为字母排序, 而非数字排序, 为什么?

order by sku_id;
;
-- 参考学习
select
    sku_info.sku_id
  , nvl( new_price, 99 ) as price
from sku_info
     left join
     (
         select
             sku_id
           , new_price
         from (
                  select
                      sku_id
                    , new_price
                    , change_date
                      -- row_number也可以排序, 并不是 row_number就不用排序
                      -- NOTE row_number肯定是单调递增的
                      -- ,
                    , row_number( ) over (partition by sku_id order by change_date desc) as rn
                  from sku_price_modify_detail
                  where
                      change_date <= '2021-10-01'
              ) t1
         where
             rn = 1
     ) t2
     on sku_info.sku_id = t2.sku_id
;
-- order by sku_id
desc sku_price_modify_detail;


-- 2.13 即时订单比例
-- 2.13.1 题目需求
-- 订单配送中，如果期望配送日期和下单日期相同，称为即时订单，如果期望配送日期和下单日期不同，称为计划订单。
-- 请从配送信息表（delivery_info）中求出每个用户的首单（用户的第一个订单）中即时订单的比例，保留两位小数，以小数形式显示。
-- 期望结果如下：
-- percentage
-- 0.5

desc delivery_info;

select
    -- 判断日期, 用 datediff 是个好习惯
    sum( `if`( order_date = custom_date, 1, 0 ) )
  , count( order_date )
    -- count的时候写具体一点比较好 NOTE
from (
         select
             user_id
           , order_date
           , custom_date
           , row_number( ) over (partition by user_id order by order_date) as row_number_user_id
         from delivery_info di
     ) t1
where
    row_number_user_id = 1
;



select datediff( '2020-09-10', '2021-09-08' );

-- 参考
select
    round( sum( if( order_date = custom_date, 1, 0 ) ) / count( * ), 2 ) as percentage
from (
         select
             delivery_id
           , user_id
           , order_date
           , custom_date
             -- 使用 row_number的程序更健壮, rank 和 dense_rank可能会筛选出多个来
           , row_number( ) over (partition by user_id order by order_date) as rn
         from delivery_info
     ) t1
where
    rn = 1;

--,2.14 向用户推荐朋友收藏的商品 想不清楚, 再思考 STAR
-- 2.14.1
-- 题目需求
-- 现需要请向所有用户推荐其朋友收藏但是用户自己未收藏的商品，请从好友关系表（friendship_info
-- ）和收藏表（favor_info
-- ）中查询出应向哪位用户推荐哪些商品。期望结果如下：
-- 1
-- ）部分结果展示
-- 101	2
-- 101	4
-- 101	7
-- 101	9
-- 101	8
-- 101	11
-- 101	1

-- NOTE 就这一次, 只为那瞬间精彩


-- 2023年09月09日23:36:50 最好的方式

SELECT distinct
    friendship_info_full.user1_id AS user_id
  , friend_favor.sku_id
FROM (
         -- 准备最基础的信息
         select user1_id, user2_id
         from friendship_info fi
         union
         select user2_id, user1_id
         from friendship_info f
     )          friendship_info_full
     left JOIN
     favor_info friend_favor ON friendship_info_full.user2_id = friend_favor.user_id
WHERE
    NOT EXISTS (
                   SELECT 1
                   FROM favor_info self_f
                   WHERE
                       friendship_info_full.user1_id = self_f.user_id AND friend_favor.sku_id = self_f.sku_id
               );



SELECT distinct
    f1.user1_id AS user_id
  , friend_f.sku_id
FROM (
         -- 准备最基础的信息
         select user1_id, user2_id
         from friendship_info fi
         union
         select user2_id, user1_id
         from friendship_info f
     )          f1
     JOIN
     favor_info friend_f ON f1.user2_id = friend_f.user_id
WHERE
    NOT EXISTS (
                   SELECT 1
                   FROM favor_info self_f
                   WHERE
                       f1.user1_id = self_f.user_id AND friend_f.sku_id = self_f.sku_id
               );



select user_id, sku_id
from friendship_info      friend_info
         -- 先关联上
         -- NOTE join not exists 是成组的
     left join favor_info friend_f
     on friend_info.user2_id = friend_f.user_id
where
        sku_id not in (
                          -- 把自己的排除
                          -- 表名, 甚至是列名见名知意很重要.
                          select self_favor.sku_id
                          from friendship_info self_info
                               join favor_info self_favor
                               on self_info.user1_id = self_favor.user_id and friend_f.sku_id = self_favor.sku_id
                      )
;
union

select user_id, sku_id
from friendship_info friend_info
         -- 先关联上
         -- NOTE join not exists 是成组的
     join favor_info friend_favor
     on friend_info.user1_id = friend_favor.user_id
where
    not exists (
                   -- 把自己的排除
                   -- 表名, 甚至是列名见名知意很重要.
                   select 1
                   from friendship_info self_info
                        join favor_info self_favor
                        on self_info.user2_id = self_favor.user_id and friend_favor.sku_id = self_favor.sku_id
               )
;


select distinct
    t1.user_id
  , friend_favor.sku_id
from (
         select
             user1_id as user_id
           , user2_id as friend_id
         from friendship_info
         union
         select
             user2_id
           , user1_id
         from friendship_info
     )                    t1
     left join favor_info friend_favor
     on t1.friend_id = friend_favor.user_id
     left join favor_info user_favor
     on t1.user_id = user_favor.user_id
         and friend_favor.sku_id = user_favor.sku_id
where
    user_favor.sku_id is null;

-- P1 再思考


SELECT DISTINCT
    f1.user1_id AS user_id
  , favor.sku_id
FROM friendship_info f1
     JOIN
     favor_info      favor ON f1.user2_id = favor.user_id
WHERE
        favor.sku_id NOT IN (
                                SELECT sku_id
                                FROM favor_info
                                WHERE
                                    user_id = f1.user1_id
                            )
UNION
-- 考虑互为朋友的情况
SELECT DISTINCT
    f1.user2_id AS user_id
  , favor.sku_id
FROM friendship_info f1
     JOIN
     favor_info      favor ON f1.user1_id = favor.user_id
WHERE
        favor.sku_id NOT IN (
                                SELECT sku_id
                                FROM favor_info
                                WHERE
                                    user_id = f1.user2_id
                            );


-- P1 再思考

SELECT DISTINCT
    f1.user1_id AS user_id
  , favor.sku_id
FROM friendship_info f1
     JOIN
     -- 连接起朋友所喜欢的来
         favor_info  favor ON f1.user2_id = favor.user_id
WHERE
    -- 去掉自己喜欢的
    NOT EXISTS (
                   SELECT 1
                   FROM favor_info user_favor
                   WHERE
                         user_favor.user_id = f1.user1_id
                     AND user_favor.sku_id = favor.sku_id
               )
UNION
-- 考虑互为朋友的情况
SELECT DISTINCT
    f1.user2_id AS user_id
  , favor.sku_id
FROM friendship_info f1
     JOIN
     favor_info      favor ON f1.user1_id = favor.user_id
WHERE
    NOT EXISTS (
                   SELECT 1
                   FROM favor_info user_favor
                   WHERE
                         user_favor.user_id = f1.user2_id
                     AND user_favor.sku_id = favor.sku_id
               );

SELECT DISTINCT
    f1.user1_id AS user_id
  , favor.sku_id
FROM friendship_info f1
     JOIN
          favor_info favor ON f1.user2_id = favor.user_id
     left semi
     join favor_info fi

select self_id, friend_id
from (
         select user1_id as self_id, user2_id as friend_id
         from (
                  select user1_id, user2_id
                  from friendship_info fi
                  union
                  select user2_id, user1_id
                  from friendship_info fi2
              ) t1
     )               t12
         -- 把所有的喜欢的商品关联起来
     join favor_info self_favor on t12.self_id = self_favor.user_id
              
              -- 剔除自己喜欢的
     join favor_info friend_favor on t12.friend_id = friend_favor.user_id
    and self_favor.sku_id = friend_favor.sku_id -- 自己和朋友都喜欢


;


select *
from (
         select
             user1_id as user_id
           , user2_id as friend_id
         from friendship_info
         union
         select
             user2_id
           , user1_id
         from friendship_info
     )                    t1
     left join favor_info friend_favor
     on t1.friend_id = friend_favor.user_id
     left join favor_info user_favor
     on t1.user_id = user_favor.user_id
--          and friend_favor.sku_id = user_favor.sku_id
-- 起别名太关键了, 会变得特别容易读


-- where
--     user_favor.sku_id is null;


-- NOTE 朋友的话, 是互为朋友, 要 union
select user1_id as user_id, user2_id as friend_id, sku_id
from (
         select user1_id, user2_id
         from friendship_info fi
         union
         select user2_id, user1_id
         from friendship_info f
     )                    t1
     left join favor_info i on user2_id = i.user_id



select
    fi.user1_id
  , fi.user2_id
  , *
from friendship_info       fi
     right join favor_info favor
     on fi.user2_id = favor.user_id



-- NOTE 各种 join 之间的练习和区别
-- join == inner join
-- left outer join == left join
-- right outer join == right join
-- 参考

-- 选择唯一的用户ID和其朋友收藏的商品SKU ID


select
    t1.user_id
  , favor_info.sku_id
from (
         -- 下面的子查询考虑了互为朋友的情况，意思是如果A是B的朋友，B也是A的朋友
         -- 首先从friendship_info表选择user1_id作为用户，user2_id作为该用户的朋友
         select
             -- 使用别名使结果更容易理解
             user1_id as user_id
           , user2_id as friend_id
         from friendship_info
         
         union
         -- 使用union删除重复记录，并考虑反方向的朋友关系
         -- 接着选择user2_id作为用户，user1_id作为该用户的朋友
         select
             user2_id
           , user1_id
         from friendship_info
     )                    t1 -- 上述子查询的结果命名为t1

-- NOTE 使用 left join 来扩展列


-- NOTE join的话, 一定要起别名
-- 把朋友的收藏放进来
     left join favor_info fi on t1.friend_id = fi.user_id
                   -- 把自己的收藏放进来
     left join favor_info on t1.user_id = favor_info.user_id
where
    fi.sku_id is null;
;


-- NOTE 去除某段字符,
-- substring
-- split
-- cast
-- date_format()


-- 2.15.1 题目需求
-- 从登录明细表（user_login_detail）中查询出，所有用户的连续登录两天及以上的日期区间，以登录时间（login_ts）为准。期望结果如下：
-- split 取出日期 NOTE
select user_id, count( flag ) as count_flag, min( get_date ) as start_date, max( get_date ) as end_date
from (
         select user_id, get_date, row_number, date_sub( get_date, row_number ) as flag
         from (
                  select
                      user_id
                    , get_date
                    , row_number( ) over (partition by user_id order by get_date) as row_number
                  from (
                           select distinct user_id, split( login_ts, ' ' )[0] as get_date
                           from user_login_detail uld
                       ) t1
              ) t2
     ) t3
group by user_id, flag
       -- NOTE having count也是经常使用
having
    count( flag ) >= 2
;


-- 参考, 写法非常巧妙

select
    user_id
  , min( login_date ) as start_date
  , max( login_date ) as end_date
from (
         select
             user_id
           , login_date
           , date_sub( login_date, rn ) as flag
         from (
                  select
                      user_id
                    , login_date
                    , row_number( ) over (partition by user_id order by login_date) as rn
                  from (
                           select
                               user_id
                             , date_format( login_ts, 'yyyy-MM-dd' ) as login_date
                           from user_login_detail
                           group by user_id, date_format( login_ts, 'yyyy-MM-dd' )
                       ) t1
              ) t2
     ) t3
group by user_id, flag
having
    count( * ) >= 2;


-- --2.15 查询所有用户的连续登录两天及以上的日期区间
-- 2.15.1
-- 题目需求
-- 从登录明细表（user_login_detail）中查询出，所有用户的连续登录两天及以上的日期区间，以登录时间（login_ts-- ）为准。
-- 101	2021-09-27	2021-09-30
-- 102	2021-10-01	2021-10-02
-- 106	2021-10-04	2021-10-05
-- 107	2021-10-05	2021-10-06


-- 筛选出连续登录的用户 NOTE
-- NOTE lag直接相等的方式只能筛选一个固定的值, 不能筛选连续多天

-- 一次只能筛选出一个
--      使用 lag--> 相减--> 筛选等于固定的数字的,
-- 一次可以筛选多个, 确定范围
--      使用 lag--> 相减--> sum(if)-->where >=
--      使用 row_number-->相减-->count()over()-->where >=
select user_id, flag, count( * ), min( login_date ) as start_date, max( login_date ) as end_date
from (
         select user_id, login_date, rn, date_sub( login_date, rn ) as flag
         from (
                  select user_id, login_date, row_number( ) over (partition by user_id order by login_date) as rn
                  from (
                           select user_id, login_date
                           from (
                                    select distinct user_id, split( login_ts, ' ' )[0] as login_date
                                    from user_login_detail uld
                                ) t1
                       ) t2
              ) t3
     ) t4
group by user_id, flag
having
    count( * ) >= 2
;



select user_id, login_date, lag_1_login_date
from (
         select
             user_id
           , login_date
           , lag( login_date, 1, login_date ) over (partition by user_id order by login_date) as lag_1_login_date
         from (
                  select user_id, login_date
                  from (
                           select distinct user_id, split( login_ts, ' ' )[0] as login_date
                           from user_login_detail uld
                       ) t1
              ) t2
     ) t3
where
    datediff( login_date, lag_1_login_date ) = 1;

-- 2.16 男性和女性每日的购物总金额统计
-- 2.16.1 题目需求
-- 从订单信息表（order_info）和用户信息表（user_info）中，分别统计每天男性和女性用户的订单总金额，如果当天男性或者女性没有购物，则统计结果为0。
-- 期望结果如下：
-- create_date
-- （日期）	total_amount_male
-- （男性用户总金额）	total_amount_female
-- （女性用户总金额）
-- 2021-09-27	29000.00	0.00
-- 2021-09-28	70500.00	0.00
-- 2021-09-29	43300.00	0.00

desc order_info;

desc user_info;
select *
from user_info ui
limit 10;

select
    create_date
  , sum( if( gender = '男', total_amount, 0 ) ) as total_amount_male
  , sum( if( gender = '女', total_amount, 0 ) ) as total_amount_female

from (
         
         select *
         from order_info oi join user_info ui on ui.user_id = oi.user_id
     ) t1
group by create_date;

-- NOTE 有了 over,就是原来基础上增加 1 列, 就不会去重了.
select distinct
    create_date
  , sum( if( gender = '男', total_amount, 0 ) ) over (partition by create_date) as total_amount_male
  , sum( if( gender = '女', total_amount, 0 ) ) over (partition by create_date) as total_amount_female

from (
         
         select *
         from order_info oi join user_info ui on ui.user_id = oi.user_id
     ) t1
;


-- 2.17 订单金额趋势分析
-- 2.17.1 题目需求
-- 查询截止每天的最近3天内的订单金额总和以及订单金额日平均值，保留两位小数，四舍五入。期望结果如下：
-- create_date --
-- （日期）
-- total_3d
-- （最近3日订单金额总和）
-- avg_ad
-- （最近3日订单金额日平均值）
-- 2021-09-27	29000.00	29000.00
-- 2021-09-28	99500.00	49750.00
-- 2021-09-29	142800.00	47600.00
-- 2021-09-30	114660.00	38220.00


select a.create_date, sum( a.sum_total_amount ) as total_3d, avg( a.sum_total_amount ) as avg_ad
from (
         select create_date, sum( total_amount ) as sum_total_amount
         from order_info oi
         group by create_date
     )           a
     left join (
                   select create_date, sum( total_amount ) as sum_total_amount
                   from order_info oi
                   group by create_date
               ) b
     on datediff( a.create_date, b.create_date ) <= 2
         and datediff( a.create_date, b.create_date ) >= 0
group by a.create_date
;

select a.create_date, b.create_date, a.sum_total_amount, b.sum_total_amount
from (
         select create_date, sum( total_amount ) as sum_total_amount
         from order_info oi
         group by create_date
     )           a
     left join (
                   select create_date, sum( total_amount ) as sum_total_amount
                   from order_info oi
                   group by create_date
               ) b
     on datediff( a.create_date, b.create_date ) <= 2
         and datediff( a.create_date, b.create_date ) >= 0;

-- 参考, 这个不能解决某一天没有用户下单的问题, 有日期不连续问题
select
    create_date
  , round( sum( total_amount_by_day ) over (order by create_date rows between 2 preceding and current row ),
           2 ) as total_3d
  , round( avg( total_amount_by_day ) over (order by create_date rows between 2 preceding and current row ),
           2 ) as avg_3d
from (
         select
             create_date
           , sum( total_amount ) as total_amount_by_day
         from order_info
         group by create_date
     ) t1;
-- 2.18 购买过商品1和商品2但是没有购买商品3的顾客 -- STAR
-- 2.18.1 题目需求
-- 从订单明细表(order_detail)中查询出所有购买过商品1和商品2，但是没有购买过商品3的用户，期望结果如下：
-- user_id
-- 103
-- 105

-- NOTE 有 1 有 2, 没有 3, 借助集合实现
--

select user_id, cs
from (
         select
             user_id
           , collect_set( sku_id ) as cs
         
         from order_detail od join order_info oi on oi.order_id = od.order_id
         group by user_id
     ) t1
where
      array_contains( cs, '1' )
  and array_contains( cs, '2' )
  and !array_contains( cs, '3' )
;

-- 2.19 统计每日商品1和商品2销量的差值
-- 2.19.1 题目需求
-- 从订单明细表（order_detail）中统计每天商品1和商品2销量（件数）的差值（商品1销量-商品2销量），期望结果如下：
-- create_date	diff
-- 2021-09-27	2
-- 2021-10-01	-10
-- 2021-10-02	-49
-- 2021-10-03	4
-- 2021-10-04	-55

select
    create_date
  
  , sum( `if`( sku_id = 1, sku_num, 0 ) ) - sum( `if`( sku_id = 2, sku_num, 0 ) ) as `1-2`
  , sum( `if`( sku_id = 1, sku_num, 0 ) )                                         as `sku_1`
  , sum( `if`( sku_id = 2, sku_num, 0 ) )                                         as `sku_2`
from order_detail od
group by create_date


-- - 2.20 查询出每个用户的最近三笔订单 2.20.1 题目需求
-- 从订单信息表（order_info）中查询出每个用户的最近三笔订单，期望结果如下：
-- user_id	order_id	create_date
-- 101	2	2021-09-28
-- 101	3	2021-09-29
-- 101	4	2021-09-30
-- 102	5	2021-10-01

-- NOTE 注意,  order by 后常常跟着 desc

select *
from (
         select
             user_id
           , order_id
           , create_date
           , row_number( ) over (partition by user_id order by create_date desc) as rn
         from order_info oi
     ) t1
where
    rn <= 3;

-- 2.21 查询每个用户登录日期的最大空档期
-- 2.21.1 题目需求
-- 从登录明细表（user_login_detail）中查询每个用户两个登录日期（以login_ts为准）之间的最大的空档期。
-- 统计最大空档期时，用户最后一次登录至今的空档也要考虑在内，假设今天为2021-10-10。期望结果如下：
select user_id, max( flag )
from (
         select
             user_id
           , login_ts
           , lag( login_ts, 1, '2021-10-10' ) over (partition by user_id order by login_ts desc)
           , datediff( lag( login_ts, 1, '2021-10-10' ) over (partition by user_id order by login_ts desc),
                       login_ts ) as flag
         from user_login_detail uld
     ) t1
group by user_id

--NOTE  null只能用is 来判断, 不可以用 大于 小于来判断

--2.22 查询相同时刻多地登陆的用户 STAR
--     2.22.1 题目需求
-- 从登录明细表（user_login_detail）中查询在相同时刻，多地登陆（ip_address不同）的用户，期望结果如下：
-- user_id(用户id)
-- 101
-- 102
-- 104
-- 107


-- NOTE 尽量不适用 join,因为他的效率很低.

select distinct uld.user_id
from user_login_detail      uld
     join user_login_detail uld2
              -- 通过主键关联是最基本的一步
     on uld.user_id = uld2.user_id
         -- 确保不是一行
         and uld.login_ts != uld2.login_ts
         -- 关联条件
         and uld.login_ts < uld2.logout_ts
         and uld.logout_ts > uld2.login_ts
         and uld.ip_address != uld2.ip_address;


select distinct user_id
from (
         
         select *, `if`( max_logout is null, 2, `if`( max_logout > login_ts, 1, 0 ) ) as flag
         from (
                  select
                      user_id
                    , ip_address
                    , login_ts
                    , logout_ts
                    , max( logout_ts ) over (
                      partition by user_id
                      order by logout_ts rows between unbounded preceding and 1 preceding ) as max_logout
                  from user_login_detail uld
              ) t1
     ) t2
where
    flag = 1

-- flag 可以判断多个 NOTE

;

-- 好用的
select distinct user_id
from (
         select
             user_id
           , login_ts
           , max( logout_ts )
                  over (partition by user_id order by login_ts rows between unbounded preceding and 1 preceding) as max_logout
         from user_login_detail
     ) t1
where
    max_logout > login_ts
;

--  NOTE null不能用来比较打下, 比较的话, 结果仍然为 null


select null < 1;

-- 参考答案, 比较通用, P1

select
    *
    --     distinct     t2.user_id
from (
         select
             t1.user_id
           , login_ts
           , logout_ts
           , max_logout
             -- NOTE 用 if 打标签, 很经典
             -- if嵌套是有顺序的, 和直接拿出某个数来还不一样
           , if( t1.max_logout is null, 2, if( t1.max_logout < t1.login_ts, 1, 0 ) ) as flag
         from (
                  select
                      user_id
                    , login_ts
                    , logout_ts
                    , max( logout_ts )
                           over (partition by user_id order by login_ts rows between unbounded preceding and 1 preceding) as max_logout
                  from user_login_detail
              ) t1
     ) t2
where
    t2.flag = 0
;

-- 参考 GPT
SELECT DISTINCT a.user_id
FROM user_login_detail      a
     JOIN user_login_detail b
     ON a.user_id = b.user_id
         
         -- NOTE 检测重叠方法, 非常好用
         AND a.login_ts < b.logout_ts
         AND a.logout_ts > b.login_ts
         AND a.ip_address != b.ip_address;



select
    uld.user_id
  , uld.ip_address
from user_login_detail           uld
     left join user_login_detail uld2
     on uld2.user_id = uld.user_id
         
         -- P2 如何表示重合
         -- 利用最大登出时间
         and uld2.ip_address != uld.ip_address


-- note null 跟任何值的结果比较仍然为 null, if 做判断的时候, null 为不成立
select null > 1;
select null < 1;
select null = null;

select `if`( null > 1, 1, 0 );
select `if`( null < 1, 1, 0 );
select `if`( null = null, 1, 0 );


-- 参考, 这个缺少对不同 ip 的判断
select distinct
    t2.user_id
from (
         select
             t1.user_id
           , login_ts
           , ip_address
           , max_logout
             -- 这里对 null 的处理 NOTE
           , if( t1.max_logout is null, 2, if( t1.max_logout < t1.login_ts, 1, 0 ) ) as flag
           
           , `if`( t1.max_logout < t1.login_ts, 1, 0 )
         from (
                  select
                      user_id
                    , login_ts
                    , logout_ts
                    , ip_address
                      -- NOTE 重合问题, 使用截止上一行的最大右和当前的左比较
                    
                    , max( logout_ts )
                           over (partition by user_id order by login_ts rows between unbounded preceding and 1 preceding) as max_logout
                  from user_login_detail
              ) t1
     ) t2
where
    t2.flag = 0


--2.23 销售额完成任务指标的商品 STAR
--     2.23.1 题目需求
-- 商家要求每个商品每个月需要售卖出一定的销售总额
-- 假设1号商品销售总额大于21000，2号商品销售总额大于10000，其余商品没有要求
-- 请写出SQL从订单详情表中（order_detail）查询连续两个月销售总额大于等于任务总额的商品
select date_format( create_date, 'yyyy-MM' )
from order_detail od

-- 注意, month()只能去除月份, 不能取出年


-- NOTE partition可以是多个字段

-- NOTE 日期格式的不正确, 会返回 null, 可以通过 concat 先转化为标准日期
select sku_id
from (
         
         select sku_id, date_year_month, rn, add_months( concat( date_year_month, '-01' ), -rn ) as flag
         from (
                  
                  select
                      sku_id
                    , date_year_month
                    , row_number( ) over (partition by sku_id order by date_year_month) as rn
                  from (
                           select
                               sku_id
                             , date_format( create_date, 'yyyy-MM' ) as date_year_month
                             , sum( price * sku_num )                as sum_amount
                           
                           from order_detail od
                           where
                               sku_id = 1 or sku_id = 2
                           group by sku_id, date_format( create_date, 'yyyy-MM' )
                           having
                               ( sku_id = 1 and sum_amount > 20000 ) or ( sku_id = 2 and sum_amount > 30000 )
                       ) t2
              ) t3
     ) t4
group by sku_id, flag
having
    count( * ) >= 2
;
-- p1 连续 2 个月大于


-- 对特定的 key 做判断, 其他的不做判断. 只需要把特定的筛选出来即可
-- 连续 2 个月,


-- p2 CASE when then 放在什么位置?
--     case date_year_month
-- when sum_amount>20000 then

;


-- 参考

-- 求出1号商品  和  2号商品 每个月的购买总额 并过滤掉没有满足指标的商品
select
    sku_id
  , concat(
        substring(
            create_date
            , 0
            , 7 )
        , '-01' )         as ymd
  , sum(
        price * sku_num ) as sku_sum
from order_detail
where
    sku_id = 1 or sku_id = 2
group by sku_id, substring( create_date, 0, 7 )
having
     (
         sku_id = 1 and sku_sum >= 21000 )
  or (
         sku_id = 2 and sku_sum >= 10000 )

-- 判断是否为连续两个月
select distinct
    t3.sku_id
from (
         select
             t2.sku_id
           , count( * ) over (partition by t2.sku_id, t2.rymd) as cn
         from (
                  select
                      t1.sku_id
                    , add_months( t1.ymd, -row_number( ) over (partition by t1.sku_id order by t1.ymd) ) as rymd
                  from (
                           select
                               sku_id
                             , concat( substring( create_date, 0, 7 ), '-01' ) as ymd
                             , sum( price * sku_num )                          as sku_sum
                           from order_detail
                           where
                               sku_id = 1 or sku_id = 2
                           group by sku_id, substring( create_date, 0, 7 )
                           having
                               ( sku_id = 1 and sku_sum >= 21000 ) or ( sku_id = 2 and sku_sum >= 10000 )
                       ) t1
              ) t2
     ) t3
where
    t3.cn >= 2
;


-- 2.24 根据商品销售情况进行商品分类
-- 2.24.1 题目需求
-- 从订单详情表中（order_detail）对销售件数对商品进行分类，0-5000为冷门商品，5001-19999位一般商品，20000往上为热门商品，
-- 并求出不同类别商品的数量
-- 结果如下：
-- Category（类型）	Cn（数量）
-- 一般商品	1
-- 冷门商品	10
-- 热门商品	1

select categray, sum( sum_sku_num ), count( sum_sku_num )
from (
         
         
         select
             sku_id
           , sum_sku_num
             -- case 的用法 note
             --     when then
             --     when then
             -- end
           , case
                 when sum_sku_num < 5000 and sum_sku_num >= 0      then '冷门'
                 when sum_sku_num >= 5000 and sum_sku_num <= 19999 then '一般'
                 when sum_sku_num >= 20000                         then '热门'
             
             end as categray
         from (
                  select sku_id, sum( sku_num ) as sum_sku_num
                  from order_detail od
                  group by sku_id
              ) t1
     ) t2
group by categray

-- 2.25 各品类销量前三的所有商品 STAR
-- 2.25.1 题目需求
-- 从订单详情表中（order_detail）和商品（sku_info）中查询各个品类销售数量前三的商品。如果该品类小于三个商品，则输出所有的商品销量。
-- 结果如下：
-- Sku_id（商品id）	Category_id（品类id）
-- 2	1
-- 4	1
-- 1	1
-- 8	2
-- 7	2
-- 5	2

select sku_id, category_id
from (
         select
             category_id
           , sku_id
           , sum_sku_num
           , dense_rank( ) over (partition by category_id order by sum_sku_num desc) as drk
         from (
                  select category_id, sku_id, sum( sku_num ) as sum_sku_num
                  from (
                           select category_id, od.sku_id, sku_num
                           from sku_info si join order_detail od on od.sku_id = si.sku_id
                       ) t1
                  group by category_id, sku_id
              ) t2
     ) t3
where
    drk <= 3
;

select *
from order_detail od;


-- 没有弄清楚基础数据

-- 排名使用dense_rank

select *
from (
         select
             category_id
           , od.sku_id
           , sku_num
           , dense_rank( ) over (partition by category_id,od.sku_id order by sku_num desc) as drk
         from order_detail od join sku_info si on si.sku_id = od.sku_id
     ) t1
where
    drk <= 3;

-- 参考

select
    t2.sku_id
  , t2.category_id
from (
         select
             t1.sku_id
           , si.category_id
           , sku_sum
           , rank( ) over (partition by category_id order by t1.sku_sum desc) as rk
         from (
                  select
                      sku_id
                    , sum( sku_num ) as sku_sum
                  from order_detail
                  group by sku_id
              )        t1
              join
              sku_info si
              on
                  t1.sku_id = si.sku_id
     ) t2
where
    t2.rk <= 3;

--2.26 各品类中商品价格的中位数 STAR
-- 题目需求
-- 从商品（sku_info ）中价格的中位数, 如果中位数如果是偶数则输出中间两个值的平均值，如果是奇数，则输出中间数即可。
-- 结果如下：
-- （品类id）	     中位数）
-- 1	        3500.0
-- 2	        1250.0
-- 3	        510.0

select
    category_id
  , avg( price ) as middle_price
    -- 取出中间的值遇到了问题 P1
from (
         select
             sku_id
           , category_id
           , price
           , count( * ) over (partition by category_id )                        as cn
           , count( * ) over (partition by category_id ) % 2                    as flag
             -- NOTE 用 row_number就一定要用 order by, 否则 row_number就没有意义了
           , row_number( ) over (partition by category_id order by price desc ) as rn
         from sku_info si
     ) t1
     -- 取出品类中商品数为偶数
     -- NOTE 看奇数还是偶数就是用  %
where
      cn % 2 = 0
      -- 取出中间 2 行
  and ( rn = cn / 2 or rn = cn / 2 + 1 )
group by category_id

union


select
    category_id
  , avg( price ) as middle_price
from (
         select
             sku_id
           , category_id
           , price
           , count( * ) over (partition by category_id )                        as cn
             -- NOTE 用 row_number就一定要用 order by, 否则 row_number就没有意义了
           , row_number( ) over (partition by category_id order by price desc ) as rn
         from sku_info si
     ) t1
     -- 取出品类中商品数为技术
     -- NOTE 看奇数还是偶数就是用  %
where
      cn % 2 = 1
      -- 取出中间 2 行
  and ( rn = cn + 1 / 2 )
group by category_id
;


--求出偶数品类的中位数
select *
from (
         --求个每个品类 价格排序 商品数量 以及打上奇偶数的标签
         
         select
             sku_id
           , category_id
           , price
           , row_number( ) over (partition by category_id order by price desc) as rk
           , count( * ) over (partition by category_id)                        as cn
           , count( * ) over (partition by category_id) % 2                    as falg
         from sku_info
     ) t1
where
    t1.falg = 0 and ( t1.rk = cn / 2 or t1.rk = cn / 2 + 1 )
;


--求出偶数品类的中位数
select distinct
    t1.sku_id
  , category_id
  , price
  , rk
  , cn
  , falg
    --   , avg( t1.price ) over (partition by t1.category_id) as medprice
from (
         --求个每个品类 价格排序 商品数量 以及打上奇偶数的标签
         
         select
             sku_id
           , category_id
           , price
           , row_number( ) over (partition by category_id order by price desc) as rk
           , count( * ) over (partition by category_id)                        as cn
           , count( * ) over (partition by category_id) % 2                    as falg
         from sku_info
     ) t1
where
    t1.falg = 0 and ( t1.rk = cn / 2 or t1.rk = cn / 2 + 1 )

--求出奇数品类的中位数
select
    t1.category_id
  , t1.price
from (
         select
             sku_id
           , category_id
           , price
           , row_number( ) over (partition by category_id order by price desc) as rk
           , count( * ) over (partition by category_id)                        as cn
           , count( * ) over (partition by category_id) % 2                    as falg
         from sku_info
     ) t1
where
    t1.falg = 1 and t1.rk = round( cn / 2 )

-- 竖向拼接
select distinct
    t1.category_id
  , avg( t1.price ) over (partition by t1.category_id) as medprice
from (
         select
             sku_id
           , category_id
           , price
           , row_number( ) over (partition by category_id order by price desc) as rk
           , count( * ) over (partition by category_id)                        as cn
           , count( * ) over (partition by category_id) % 2                    as falg
         from sku_info
     ) t1
where
    t1.falg = 0 and ( t1.rk = cn / 2 or t1.rk = cn / 2 + 1 )

union

select
    t1.category_id
  , t1.price / 1
from (
         select
             sku_id
           , category_id
           , price
           , row_number( ) over (partition by category_id order by price desc) as rk
           , count( * ) over (partition by category_id)                        as cn
           , count( * ) over (partition by category_id) % 2                    as falg
         from sku_info
     ) t1
where
    t1.falg = 1 and t1.rk = round( cn / 2 )


-- 2.27 找出销售额连续3天超过100的商品
-- 2.27.1 题目需求
-- 从订单详情表（order_detail）中找出销售额连续3天超过100的商品
-- 结果如下：
-- Sku_id（商品id）
-- 1
-- 10
-- 11
-- 12


select sku_id, flag, count( * )
from (
         
         -- NOTE 全部为日期的时候才可以用 datediff
         -- NOTE 一个为日期格式, 另一个为整数的时候, 用 date_sbu
         select sku_id, date_sub( create_date, rn ) as flag
         from (
                  
                  select sku_id, create_date, row_number( ) over (partition by sku_id order by create_date) as rn
                  from order_detail od
                  group by sku_id, create_date
                  having
                      sum( price * sku_num ) > 100
              ) t1
     ) t2
group by sku_id, flag
       
       -- 我这里算的是正好连续 3 天, 生产环境中要问清楚, 连续 3 天包含 4 天吗?
having
    count( * ) >= 3
;
-- 核对, 吸收其长处

-- 每个商品每天的销售总额
select
    sku_id
  , create_date
  , sum( price * sku_num ) as sku_sum
from order_detail
group by sku_id, create_date
having
    sku_sum >= 100

--  判断连续三天以上
select distinct
    t3.sku_id
from (
         select
             t2.sku_id
           , count( * ) over (partition by t2.sku_id,t2.date_drk) as cdrk
         from (
                  select
                      t1.sku_id
                    , t1.create_date
                    , date_sub( t1.create_date,
                                rank( ) over (partition by t1.sku_id order by t1.create_date) ) as date_drk
                  from (
                           select
                               sku_id
                             , create_date
                             , sum( price * sku_num ) as sku_sum
                           from order_detail
                           group by sku_id, create_date
                           having
                               sku_sum >= 100
                       ) t1
              ) t2
     ) t3
where
    t3.cdrk >= 3


-- 2.28 查询有新注册用户的当天的新用户数量、新用户的第一天留存率 STAR
-- 从用户登录明细表（user_login_detail）中首次登录算作当天新增，第二天也登录了算作一日留存
-- 结果如下：
-- first_login（注册时间）	Register（新增用户数）	Retention（留存率）
-- 2021-09-21	1	0.0
-- 2021-09-22	1	0.0
-- 2021-10-04	2	0.5
-- 2021-10-06	1	0.0

-- NOTE 连续登录问题, 先去重. 否则统计时必然出现误差
-- 2023年09月13日08:39:59
select
    t2.user_id
  , register_date
  , u.user_id
  , u.login_ts
    --   , count( t2.user_id ) as register_count
from (
         select user_id, min( date_formatted ) as register_date
         from (
                  select distinct user_id, date_format( login_ts, 'yyyy-MM-dd' ) as date_formatted
                  from user_login_detail uld
              ) t1
         group by user_id
     )                           t2
     left join user_login_detail u
     on u.user_id = t2.user_id
         and datediff( login_ts, register_date ) = 1
;



select datediff( login_ts, '2021-09-09' )
from user_login_detail uld
;

-- 参考

-- 每个用户首次登录时间 和 第二天是否登录 并看每天新增和留存数量
select
    t1.first_login
  , count( t1.user_id ) as register
  , count( t2.user_id ) as remain_1
from (
         select
             user_id
           , date_format( min( login_ts ), 'yyyy-MM-dd' ) as first_login
         from user_login_detail
         group by user_id
     )                 t1
     left join
     user_login_detail t2
     on
                 t1.user_id = t2.user_id
             and datediff( date_format( t2.login_ts, 'yyyy-MM-dd' ), t1.first_login ) = 1
group by t1.first_login
;

-- 新增数量和留存率
select
    t3.first_login
  , t3.register
  , t3.remain_1 / t3.register as retention
from (
         select
             t1.first_login
           , count( t1.user_id ) as register
           , count( t2.user_id ) as remain_1
         from (
                  select
                      user_id
                    , date_format( min( login_ts ), 'yyyy-MM-dd' ) as first_login
                  from user_login_detail
                  group by user_id
              )                 t1
              left join
              user_login_detail t2
              on
                          t1.user_id = t2.user_id
                      and
                          datediff( date_format( t2.login_ts, 'yyyy-MM-dd' ), t1.first_login ) = 1
         group by t1.first_login
     ) t3
;


-- 我的 2023年09月13日07:55:45
select
    user_id
  , login_date
  , drk
  , sum( `if`( drk = 1, 1, 0 ) ) over (partition by user_id order by login_date) as first_day_login
from (
         select user_id, login_date, dense_rank( ) over (partition by user_id order by login_date) as drk
         from (
                  select distinct user_id, date_format( login_ts, 'yyyy-MM-dd' ) as login_date
                  from user_login_detail uld
              ) t1
     ) t2


select login_date, count( * ) as first_day_login
from (
         select
             user_id
           , login_date
           , drk
           , sum( `if`( drk = 1, 1, 0 ) ) as first_day_login
         from (
                  select user_id, login_date, dense_rank( ) over (partition by user_id order by login_date) as drk
                  from (
                           select distinct user_id, date_format( login_ts, 'yyyy-MM-dd' ) as login_date
                           from user_login_detail uld
                       ) t1
              ) t2
     ) t3
group by login_date


-- 2.29 求出商品连续售卖的时间区间
-- 2.29.1 题目需求
-- 从订单详情表（order_detail）中，求出商品连续售卖的时间区间
-- 结果如下（截取部分）：
-- Sku_id（商品id）	Start_date（起始时间）	End_date（结束时间）
-- 1	2021-09-27	2021-09-27
-- 1	2021-09-30	2021-10-01
-- 1	2021-10-03	2021-10-08
-- 10	2021-10-02	2021-10-03


-- NOTE 日期连续先去重
select sku_id, min( create_date ) as start_date, max( create_date ) as end_date
from (
         select sku_id, create_date, rn, date_sub( create_date, rn ) as flag
         from (
                  select distinct
                      sku_id
                    , create_date
                    , row_number( ) over (partition by sku_id order by create_date) as rn
                  from order_detail od
              ) t1
     ) t12
group by sku_id, flag

-- 2.30 登录次数及交易次数统计 STAR
-- 2.30.1 题目需求
-- 分别从登陆明细表（user_login_detail）和配送信息表中用户登录时间和下单时间统计登陆次数和交易次数

select t1.user_id, order_date, nvl( order_times, 0 ) as order_times, login_date, login_times
from (
         -- 用户的登录次数
         select
             user_id
           , date_format( login_ts, 'yyyy-MM-dd' ) as login_date
             -- 不能使用 count(*) P1
           , count( * )                            as login_times
         from user_login_detail uld
         group by user_id, date_format( login_ts, 'yyyy-MM-dd' )
         -- NOTE 使用 join 的事后,一般使用 left join
     ) t1 left join (
                        -- 用户的下单次数
                        select
                            user_id
                          , order_date
                          
                          , count( user_id ) as order_times
                        from delivery_info di
                        group by user_id, order_date
                    ) t2
          on t1.user_id = t2.user_id
              and t1.login_date = t2.order_date
;


select user_id, order_date, count( * )
from delivery_info di
group by user_id, order_date

-- 参考


-- 拿到每个用户每天的交易次数
select
    t1.user_id
  , t1.login_date
    -- 这里使用 max 仅仅是因为后面有 group by, 又需要这个数据
  , max( login_count )  as login_count
  , count( di.user_id ) as order_count
from (
         select
             user_id
           , date_format( login_ts, 'yyyy-MM-dd' ) as login_date
           , count( * )                            as login_count
         from user_login_detail
         group by user_id, date_format( login_ts, 'yyyy-MM-dd' )
     )             t1
     left join
     delivery_info di
     on
         t1.user_id = di.user_id and t1.login_date = di.order_date
group by t1.user_id, t1.login_date


-- 2.31 按年度列出每个商品销售总额
-- 2.31.1 题目需求
-- 从订单明细表（order_detail）中列出每个商品每个年度的购买总额

select sku_id, year( create_date ), sum( price * sku_num )
from order_detail od
group by sku_id, year( create_date )
order by sku_id, year( create_date )

-- 2.32. 某周内每件商品每天销售情况
-- 2.32.1 题目需求
-- 从订单详情表（order_detail）中查询2021年9月27号-2021年10月3号这一周所有商品每天销售情况。
select
    sku_id
  , sum( `if`( `dayofweek`( create_date ) = 1, sku_num, 0 ) ) as sunday
from order_detail od
where
    create_date >= '2021-09-27' and create_date <= '2021-10-03'
    --  NOTE 日期可以直接用来比较大小, 计算具体天数时需要用 datediff, date_sub.
    --  NOTE null 只能用 is 来判断
    -- NOTE and if or 连接的一定是逻辑

group by sku_id

select `dayofweek`( '2023-09-13' )


-- 2.33 查看每件商品的售价涨幅情况
-- 2.33.1 题目需求
-- 从商品价格变更明细表（sku_price_modify_detail），得到最近一次价格的涨幅情况，并按照涨幅升序排序。
-- 结果如下：
-- Sku_id（商品id）	Price_change（涨幅）
-- 8	-200.00
-- 9	-100.00
-- 2	-70.00
-- 11	-16.00
-- 12	-15.00


-- NOTE 涨幅肯定是新的减旧的
select sku_id, t1.new_price - lead_price as change_price
from (
         select
             sku_id
           , new_price
           , change_date
           , row_number( ) over (partition by sku_id order by change_date desc)        as rn
           , lead( new_price, 1 ) over (partition by sku_id order by change_date desc) as lead_price
         from sku_price_modify_detail
     ) t1
     -- 借助 flag 筛选出最近一次, 很巧妙
where
    rn = 1
;

-- 2.34 销售订单首购和次购分析 STAR
-- 2.34.1 题目需求
-- 通过商品信息表（sku_info）订单信息表（order_info）订单明细表（order_detail）
-- 分析如果有一个用户成功下单两个及两个以上的购买成功的手机订单（购买商品为xiaomi 10，apple 12，小米13）
-- 那么输出这个用户的id及第一次成功购买手机的日期和第二次成功购买手机的日期，以及购买手机成功的次数。

-- 101	2021-09-27	2021-09-28	3
-- 1010	2021-10-08	2021-10-08	2
-- 102	2021-10-01	2021-10-01	3

-- NOTE case when then end相当于 if, case 使用范围更广


select
    user_id
    -- coalesce取出第一个非空的
    -- NOTE 这里使用 MAX, 非常巧妙
    -- NOTE 另外一个 MAX 的用法就是取出一个用到了聚合函数的东西
  , max( CASE WHEN rn = 1 THEN create_date END ) as first_buy_date
  , max( `if`( rn = 2, create_date, null ) )     as second_buy_date
  , count( * )
from (
         select
             user_id
           , name
           , create_date
           , row_number( ) over (partition by user_id order by create_date) as rn
         from (
                  select user_id, od.create_date, name
                  from order_detail    od
                       join order_info oi on oi.order_id = od.order_id
                       join sku_info   si on si.sku_id = od.sku_id
                  where
                      si.name = 'xiaomi 10' or si.name = 'apple 12' or si.name = 'xiaomi 13'
              ) t1
     ) t2
group by user_id
;


-- NOTE first value的妙用: 可以取出第一行, 第二行
-- NOTE last value 结合 bounded unbounded可以非常灵活取数
-- GPT

SELECT
    user_id
  , MAX( CASE WHEN row_num = 1 THEN create_date END ) AS first_date
  , MAX( CASE WHEN row_num = 2 THEN create_date END ) AS second_date
  , total_orders                                      AS cn
FROM (
         SELECT
             oi.user_id
           , od.create_date
           , ROW_NUMBER( ) OVER (PARTITION BY oi.user_id ORDER BY od.create_date) AS row_num
           , COUNT( * ) OVER (PARTITION BY oi.user_id)                            AS total_orders
         FROM order_info        oi
              JOIN order_detail od ON oi.order_id = od.order_id
              JOIN sku_info     si ON od.sku_id = si.sku_id
         WHERE
             si.name IN ( 'xiaomi 10', 'apple 12', 'xiaomi 13' )
     ) RankedOrders
WHERE
    total_orders >= 2
GROUP BY user_id, total_orders
;
-- 参考

select distinct
    oi.user_id
  , name
  , first_value( od.create_date )
                 over (partition by oi.user_id order by od.create_date rows between unbounded preceding and unbounded following ) as first_date
  , last_value( od.create_date )
                over (partition by oi.user_id order by od.create_date rows between unbounded preceding and unbounded following )  as last_date
  , count( * )
           over (partition by oi.user_id order by od.create_date rows between unbounded preceding and unbounded following)        as cn
from order_info   oi
     join
     order_detail od
     on
         oi.order_id = od.order_id
     join
     sku_info     si
     on
         od.sku_id = si.sku_id
where
    -- NOTE 使用 in 要比使用很多 or 强很多
    si.name in ( 'xiaomi 10', 'apple 12', 'xiaomi 13' )


-- 2.35 同期商品售卖分析表
-- 从订单明细表（order_detail）中。
-- 求出同一个商品在2021年和2022年中同一个月的售卖情况对比。
-- 结果如下（截取部分）：
-- Sku_id（商品id）
-- Month-- （月份）
-- 2020_skusum -- （2020销售量）
-- 2021_skusum -- （2021销售量）
-- 1	9	0	11
-- 1	10	2	38
-- 10	10	94	205

-- NOTE 写了半天是错的, 主要是因为没有审清楚题


select
    sku_id
  , month_create_date
    -- NOTE sum(if(,,0)) 这个组合非常好用
  , sum( `if`( year_create_date = 2020, sum_sku_num, 0 ) ) as sum_sku_num_2020
  , sum( `if`( year_create_date = 2021, sum_sku_num, 0 ) ) as sum_sku_num_2021
from (
         select
             sku_id
           , year( create_date )  as year_create_date
           , month( create_date ) as month_create_date
           , sum( sku_num )       as sum_sku_num
         from order_detail od
         group by sku_id, year( create_date ), month( create_date )
     ) t1
group by sku_id, month_create_date
;


-- GPT P1 再对比
SELECT
    sku_id
  , month( create_date )                                as month
  , SUM( if( year( create_date ) = 2020, sku_num, 0 ) ) as 2020_skusum
  , SUM( if( year( create_date ) = 2021, sku_num, 0 ) ) as 2021_skusum
FROM order_detail
WHERE
    year( create_date ) IN ( 2020, 2021 )
GROUP BY sku_id, month( create_date )
ORDER BY sku_id, month( create_date );



select
    sku_id
  , split( year_month, '-' )[1]                                  as month
  , `if`( split( year_month, '-' )[0] = '2020', sum_sku_num, 0 ) as sum_sku_num_2020
  , `if`( split( year_month, '-' )[0] = '2021', sum_sku_num, 0 ) as sum_sku_num_2021
from (
         select
             sku_id
           , sum( sku_num )                        as sum_sku_num
           , date_format( create_date, 'yyyy-MM' ) as year_month
         from order_detail od
         group by sku_id, date_format( create_date, 'yyyy-MM' )
     ) t1
;


-- GPT

SELECT
    sku_id
  , month
  , sum( 2020_skusum ) AS 2020_skusum
  , sum( 2021_skusum ) AS 2021_skusum
FROM (
         SELECT
             sku_id
           , month( create_date )                                AS month
           , if( year( create_date ) = 2020, sum( sku_num ), 0 ) AS 2020_skusum
           , if( year( create_date ) = 2021, sum( sku_num ), 0 ) AS 2021_skusum
         FROM order_detail
         WHERE
             year( create_date ) IN ( 2020, 2021 )
         GROUP BY sku_id, month( create_date ), year( create_date )
     ) CTE
GROUP BY sku_id, month
ORDER BY sku_id, month;


-- 参考核对

select
    if( t1.sku_id is null, t2.sku_id, t1.sku_id )
  , month( if( t1.ym is null, t2.ym, t1.ym ) )
  , if( t1.sku_sum is null, 0, t1.sku_sum ) as 2020_skusum
  , if( t2.sku_sum is null, 0, t2.sku_sum ) as 2020_skusum
from (
         select
             sku_id
           , concat( date_format( create_date, 'yyyy-MM' ), '-01' ) as ym
           , sum( sku_num )                                         as sku_sum
         from order_detail
         where
             year( create_date ) = 2020
         group by sku_id, date_format( create_date, 'yyyy-MM' )
     ) t1
     full join
     (
         select
             sku_id
           , concat( date_format( create_date, 'yyyy-MM' ), '-01' ) as ym
           , sum( sku_num )                                         as sku_sum
         from order_detail
         where
             year( create_date ) = 2021
         group by sku_id, date_format( create_date, 'yyyy-MM' )
     ) t2
     on
         t1.sku_id = t2.sku_id and month( t1.ym ) = month( t2.ym )
;


select t1.sku_id, t1.year_month
from (
         select
             sku_id
           , sum( sku_num )                        as sum_sku_num
           , date_format( create_date, 'yyyy-MM' ) as year_month
         from order_detail od
         group by sku_id, date_format( create_date, 'yyyy-MM' )
     )      t1
     join (
              select
                  sku_id
                , sum( sku_num )                        as sum_sku_num
                , date_format( create_date, 'yyyy-MM' ) as year_month
              from order_detail od
              group by sku_id, date_format( create_date, 'yyyy-MM' )
          ) t2
     on t1.sku_id = t2.sku_id
         and split( t1.year_month, '-' )[1] = split( t2.year_month, '-' )[1]
         and split( t1.year_month, '-' )[0] - 1 = split( t2.year_month, '-' )[0]


-- 2.36 国庆期间每个品类的商品的收藏量和购买量
-- 2.36.1 题目需求
-- 从订单明细表（order_detail）和收藏信息表（favor_info）统计2021国庆期间，每个商品总收藏量和购买量
-- 结果如下：
-- Sku_id	Sku_sum（购买量）	Favor_cn（收藏量）
-- 1	38	1
-- 10	205	2
-- 11	225	2

select t1.sku_id, order_num, nvl( favor_num, 0 ) as favor_num
from (
         select sku_id, sum( sku_num ) as order_num
         from order_detail od
         where
             create_date <= '2021-10-07' and create_date >= '2021-10-01'
         group by sku_id
     ) t1
     left join
     
     (
         select sku_id, count( * ) as favor_num
         from favor_info fi
         where
             create_date <= '2021-10-07' and create_date >= '2021-10-01'
         group by sku_id
     ) t2
     on t1.sku_id = t2.sku_id

-- 2.37 统计活跃间隔对用户分级结果 STAR
-- NOTE 打标签的话, 用 case when then 最合适不过. sum if 适合用来分列, 取某个值
-- 2.37.1 题目需求
-- 用户等级：
-- 忠实用户：近7天活跃且非新用户
-- 新晋用户：近7天新增
-- 沉睡用户：近7天未活跃但是在7天前活跃
-- 流失用户：近30天未活跃但是在30天前活跃
-- 假设今天是数据中所有日期的最大值，从用户登录明细表中的用户登录时间给各用户分级，求出各等级用户的人数
-- 结果如下：
-- Level（用户等级）	Cn（用户数量)
-- 忠实用户	6
-- 新增用户	3
-- 沉睡用户	1


-- 确定今天日期
select `if`( max( login_ts ) > max( logout_ts ), max( login_ts ), max( logout_ts ) )
from user_login_detail uld
;

-- 基础数据去重

(
    select distinct user_id, date( login_ts ) as active_date
    from user_login_detail uld
)
union
(
    select distinct user_id, date( logout_ts ) as active_date
    from user_login_detail uld
)

-- 忠实用户：近7天活跃且非新用户

select
    count( * ) as loyal_user
from (
         select
             user_id
           , active_date
           , min( active_date ) over (partition by user_id order by active_date) as first_login
           , max( active_date ) over (partition by user_id order by active_date) as last_login
           , '2021-10-09'                                                        as today
         from (
                  (
                      select distinct user_id, date( login_ts ) as active_date
                      from user_login_detail uld
                  )
                  union
                  (
                      select distinct user_id, date( logout_ts ) as active_date
                      from user_login_detail uld
                  )
              ) t1
     ) t12
where
      
      datediff( today, active_date ) <= 7
  and ( datediff( today, first_login ) > 7 or datediff( today, last_login ) > 7 )
;


-- 参考

select
    t2.level
  , count( * )
from (
         select
             uld.user_id
           , case
                 when ( date_format( max( uld.login_ts ), 'yyyy-MM-dd' ) <= date_sub( today, 30 ) )
                     then '流失用户'-- 最近登录时间三十天前
                 when ( date_format( min( uld.login_ts ), 'yyyy-MM-dd' ) <= date_sub( today, 7 ) and
                        date_format( max( uld.login_ts ), 'yyyy-MM-dd' ) >= date_sub( today, 7 ) )
                     then '忠实用户' -- 最早登陆时间是七天前,并且最近七天登录过
                 when ( date_format( min( uld.login_ts ), 'yyyy-MM-dd' ) >= date_sub( today, 7 ) )
                     then '新增用户' -- 最早登录时间是七天内
                 when ( date_format( min( uld.login_ts ), 'yyyy-MM-dd' ) <= date_sub( today, 7 ) and
                        date_format( max( uld.login_ts ), 'yyyy-MM-dd' ) <= date_sub( today, 7 ) )
                     then '沉睡用户'-- 最早登陆时间是七天前,最大登录时间也是七天前
             end as level
         from user_login_detail uld
              join
              (
                  select
                      date_format( max( login_ts ), 'yyyy-MM-dd' ) as today
                  from user_login_detail
              )                 t1
              on
                  1 = 1
         group by uld.user_id, t1.today
     ) t2
group by t2.level
;


with init_data as (
                      select
                          user_id
                        , active_date
                        , min( active_date ) over (partition by user_id order by active_date) as first_login
                        , max( active_date ) over (partition by user_id order by active_date) as last_login
                        , '2021-10-09'                                                        as today
                      from (
                               (
                                   select distinct user_id, date( login_ts ) as active_date
                                   from user_login_detail uld
                               )
                               union
                               (
                                   select distinct user_id, date( logout_ts ) as active_date
                                   from user_login_detail uld
                               )
                           ) t1
                  )
;
where
    -- P2 怎么让最大日期动态变化. 真实环境中, 只需要用函数取当前日期即可.
    active_date > date_sub( '2021-10-09', 7 )


-- 新用户
select user_id, min( active_date ) as min_active_date
from (
         (
             select distinct user_id, date( login_ts ) as active_date
             from user_login_detail uld
         )
         union
         (
             select distinct user_id, date( logout_ts ) as active_date
             from user_login_detail uld
         )
     ) init_data
group by user_id


-- 2.38 连续签到领金币数
-- 2.38.1 题目需求
-- 用户每天签到可以领1金币，并可以累计签到天数，连续签到的第3、7天分别可以额外领2和6金币。
-- 每连续签到7天重新累积签到天数。
-- 从用户登录明细表中求出每个用户金币总数，并按照金币总数倒序排序
-- 结果如下：
-- User_id（用户id）	Sum_coin_cn（金币总数)
-- 101	7
-- 109	3
-- 105	1

select cast( 8 as int ) / 7
select cast( 8 / 3 as int )
select floor( 8 / 3 )
select ceil( 1floor / 3 )

select 8 % 3


select user_id, sum( coins )
from (
         
         select
             user_id
           , flag
           , cn
           , case
                 -- NOTE 取整, 除法的时候要 floor 一下
                 when cn % 7 = 1 then floor( cn / 7 ) * ( 7 + 2 + 6 ) + 1
                 when cn % 7 = 2 then floor( cn / 7 ) * ( 7 + 2 + 6 ) + 2
                 when cn % 7 = 3 then floor( cn / 7 ) * ( 7 + 2 + 6 ) + 3 + 2
                 when cn % 7 = 4 then floor( cn / 7 ) * ( 7 + 2 + 6 ) + 4 + 2
                 when cn % 7 = 5 then floor( cn / 7 ) * ( 7 + 2 + 6 ) + 5 + 2
                 when cn % 7 = 6 then floor( cn / 7 ) * ( 7 + 2 + 6 ) + 6 + 2
                 when cn % 7 = 7 then floor( cn / 7 ) * ( 7 + 2 + 6 ) + 7 + 2 + 6
             
             end as coins
         
         from (
                  select user_id, flag, count( * ) as cn
                  from (
                           select user_id, date_login_ts, rn, date_sub( date_login_ts, rn ) as flag
                           from (
                                    select distinct
                                        user_id
                                      , date( login_ts )                                            as date_login_ts
                                      , row_number( ) over (partition by user_id order by login_ts) as rn
                                    from user_login_detail uld
                                ) t1
                       ) t12
                  group by user_id, flag
              ) t3
     
     ) t4
group by user_id
;

-- 2.39 国庆期间的7日动销率和滞销率 STAR
-- 2.39.1 题目需求
-- 动销率定义为品类商品中一段时间内有销量的商品占当前已上架总商品数的比例（有销量的商品/已上架总商品数）。
-- 滞销率定义为品类商品中一段时间内没有销量的商品占当前已上架总商品数的比例。（没有销量的商品 / 已上架总商品数）。
-- 只要当天任一店铺有任何商品的销量就输出该天的结果
-- 从订单明细表（order_detail）和商品信息表（sku_info）表中求出国庆7天每天每个品类的商品的动销率和滞销率
-- 结果如下（截取部分）：
-- Category_id
-- （品类id）	1号
-- （动销)	1号
-- （滞销)	2号
-- （动销)	2号
-- （滞销)	3号
-- （动销)	3号
-- （滞销)
-- 2	0.75	0.25	0.75	0.25	0.75	0.25
-- 3	0.25	0.75	0.75	0.25	0.75	0.25

select t2.category_id, first_day, cn_category, first_day / cn_category
from (
         select
             category_id
           , sum( `if`( create_date = '2021-10-01', 1, 0 ) ) as first_day
         
         from (
                  -- 准备基础数据
                  select category_id, create_date
                  from sku_info          si
                       join order_detail od on od.sku_id = si.sku_id
                       -- NOTE 跟日期相关的时候, 一定要用字符串
                  where
                      create_date <= '2021-10-07' and create_date >= '2021-10-01'
              
              ) t1
         group by category_id
     )      t2
     join (
              -- NOTE 想要动态获得某些数据, 就是使用 join 的方式
              select category_id, count( * ) as cn_category
              from sku_info s
              group by category_id
          ) t2 on t2.category_id = t2.category_id
;



select distinct
    category_id
  , total
    -- 如何取出某个数, 作为除数
from order_detail od
     join (
              select category_id, count( * ) as total
              from sku_info si
              group by category_id
          )       t1
     on t1.category_id = category_id
where
    create_date >= '2021-10-01' and create_date <= '2021-10-07'
;


-- 每一天的动销率 和 滞销率
select
    t2.category_id
  , t2.`第1天` / t3.cn
  , 1 - t2.`第1天` / t3.cn
  , t2.`第2天` / t3.cn
  , 1 - t2.`第2天` / t3.cn
  , t2.`第3天` / t3.cn
  , 1 - t2.`第3天` / t3.cn
  , t2.`第4天` / t3.cn
  , 1 - t2.`第4天` / t3.cn
  , t2.`第5天` / t3.cn
  , 1 - t2.`第5天` / t3.cn
  , t2.`第6天` / t3.cn
  , 1 - t2.`第6天` / t3.cn
  , t2.`第7天` / t3.cn
  , 1 - t2.`第7天` / t3.cn
from (
         select
             t1.category_id
           , sum( if( t1.create_date = '2021-10-01', 1, 0 ) ) as `第1天`
           , sum( if( t1.create_date = '2021-10-02', 1, 0 ) ) as `第2天`
           , sum( if( t1.create_date = '2021-10-03', 1, 0 ) ) as `第3天`
           , sum( if( t1.create_date = '2021-10-04', 1, 0 ) ) as `第4天`
           , sum( if( t1.create_date = '2021-10-05', 1, 0 ) ) as `第5天`
           , sum( if( t1.create_date = '2021-10-06', 1, 0 ) ) as `第6天`
           , sum( if( t1.create_date = '2021-10-07', 1, 0 ) ) as `第7天`
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
     ) t2
     join
     (
         select
             category_id
           , count( * ) as cn
         from sku_info
         group by category_id
     ) t3
     on
         t2.category_id = t3.category_id
;


-- 2.40 同时在线最多的人数 STAR
-- 2.40.1 题目需求
-- 根据用户登录明细表（user_login_detail），求出平台同时在线最多的人数。
-- 结果如下：
-- Cn（人数）
-- 7
-- login logout , 先登录的后登出怎么算


select *
from user_login_detail uld1 join user_login_detail uld2
                            on (
                                        uld1.user_id = uld2.user_id
                                    -- NOTE 很有必要, 不自连接.
                                    and uld1.login_ts != uld2.login_ts
                                    and uld1.login_ts < uld2.logout_ts
                                    and uld1.logout_ts > uld2.login_ts
                                )


-- 找出所有的有重合的, 并合并
select uld1.user_id, min( uld1.login_ts ) as unique_login_ts, max( uld1.logout_ts ) as unique_logout_ts
from user_login_detail uld1 join user_login_detail uld2
                            on (
                                        uld1.user_id = uld2.user_id
                                    -- NOTE 很有必要, 不自连接.
                                    and uld1.login_ts != uld2.login_ts
                                    and uld1.login_ts < uld2.logout_ts
                                    and uld1.logout_ts > uld2.login_ts
                                )
group by uld1.user_id
;

-- 找出所有的没有重合的
-- NOTE 使用 left join where is null 实现没有重叠

select *
from user_login_detail uld1 left join user_login_detail uld2
                            on (
                                        uld1.user_id = uld2.user_id
                                    -- NOTE 很有必要, 不自连接.
                                    and uld1.login_ts != uld2.login_ts
                                    and uld1.login_ts < uld2.logout_ts
                                    and uld1.logout_ts > uld2.login_ts
                                )
where
    uld2.user_id is null
;


-- NOTE 使用 EXIST 实现没有重叠
select *
from user_login_detail uld1
where
    not exists (
                   select 1
                   from user_login_detail uld2
                   where
                         uld1.user_id = uld2.user_id
                     and uld1.login_ts != uld2.login_ts
                     and uld1.login_ts < uld2.logout_ts
                     and uld1.logout_ts > uld2.login_ts
               )



select max( sum_flag )
from (
         
         select user_id, ts, flag, sum( flag ) over (order by ts) as sum_flag
         from (
                  select
                      user_id
                    , login_ts as ts
                    , 1        as flag
                  from user_login_detail uld
                  
                  union all
                  select
                      user_id
                    , logout_ts as ts
                    , -1        as flag
                  from user_login_detail uld
              ) t1
     ) t12
;

select
    *
    --     uld.user_id
    --   , uld.login_ts
    --   , uld.user_id
    --   , uld.ip_address
    --   , uld.logout_ts

from user_login_detail      uld
     join user_login_detail uld2
     on uld.login_ts < uld2.logout_ts
         and uld.logout_ts > uld2.login_ts
         and uld.login_ts != uld2.login_ts
         --          and uld.login_ts <> uld2.login_ts
         --          and uld.logout_ts != uld2.logout_ts
         and uld.user_id = uld2.user_id

desc user_login_detail;

select *
from user_login_detail uld;

-- GPT
SELECT
    a.user_id
FROM user_login_detail a
     JOIN
     user_login_detail b
     ON
         a.user_id = b.user_id
WHERE
      a.login_ts < b.logout_ts -- 登录时间a在另一个会话b的登出时间之前
  AND a.logout_ts > b.login_ts -- 但登出时间a在另一个会话b的登录时间之后
  AND a.login_ts != b.login_ts -- 避免自连接时的重复匹配
GROUP BY a.user_id
HAVING
    COUNT( DISTINCT b.login_ts ) > 1;
-- 用于确定用户的登录时间是否有重叠


-- 同一个用户, 多个地方登录, 算做1 个用户

-- 参考
-- 登录标记1 下线标记-1
select
    login_ts as l_time
  , 1        as flag
from user_login_detail
union
select
    logout_ts as l_time
  , -1        as flag
from user_login_detail
;

-- 按照时间求和
select
    sum( flag ) over (order by t1.l_time) as sum_l_time
from (
         select
             login_ts as l_time
           , 1        as flag
         from user_login_detail
         union all
         select
             logout_ts as l_time
           , -1        as flag
         from user_login_detail
     ) t1
;


-- 拿到最大值 就是同时在线最多人数
select
    max( sum_l_time )
from (
         select
             sum( flag ) over (order by t1.l_time) as sum_l_time
         from (
                  select
                      login_ts as l_time
                    , 1        as flag
                  from user_login_detail
                  union
                  select
                      logout_ts as l_time
                    , -1        as flag
                  from user_login_detail
              ) t1
     ) t2



-- 第1题 同时在线人数问题
-- 1.1 题目需求
-- 现有各直播间的用户访问记录表（live_events）如下，表中每行数据表达的信息为，一个用户何时进入了一个直播间，又在何时离开了该直播间。
-- user_id
-- (用户id)	live_id
-- (直播间id)	in_datetime
-- (进入直播间的时间)	out_datetime
-- (离开直播间的时间)
-- 100	1	2021-12-1 19:30:00	2021-12-1 19:53:00
-- 100	2	2021-12-1 21:01:00	2021-12-1 22:00:00
-- 101	1	2021-12-1 19:05:00	2021-12-1 20:55:00
-- 现要求统计各直播间最大同时在线人数，期望结果如下：
-- live_id	max_user_count
-- 1	4
-- 2	3

select live_id, max( sum_flag )
from (
         select *, sum( flag ) over (partition by live_id order by ts) as sum_flag
         from (
                  select user_id, live_id, ts, flag
                  from (
                           select user_id, live_id, in_datetime as ts, 1 as flag
                           from live_events le
                           union all
                           select user_id, live_id, out_datetime as ts, -1 as flag
                           from live_events le
                       ) t1
              ) t2
     ) t3
group by live_id
;

-- 第2题 会话划分问题
-- 2.1 题目需求
-- 现有页面浏览记录表（page_view_events）如下，表中有每个用户的每次页面访问记录。
select
    user_id
  , page_id
  , view_timestamp
  , concat( user_id, '-', flag ) as session
from (
         
         select
             user_id
           , page_id
           , view_timestamp
           , sum( `if`( diff_timestamp > 60, 1, 0 ) ) over (partition by user_id order by view_timestamp) as flag
         from (
                  select
                      user_id
                    , page_id
                    , view_timestamp
                    , view_timestamp - lag( view_timestamp, 1, view_timestamp )
                                            over (partition by user_id order by view_timestamp) as diff_timestamp
                  
                  from page_view_events pve
              ) t1
     ) t12
;


-- 第3题 间断连续登录用户问题
-- 3.1 题目需求
-- 现有各用户的登录记录表（login_events）如下，表中每行数据表达的信息是一个用户何时登录了平台。
-- user_id	login_datetime
-- 100	2021-12-01 19:00:00
-- 100	2021-12-01 19:30:00
-- 100	2021-12-02 21:01:00
-- 现要求统计各用户最长的连续登录天数，间断一天也算作连续，例如：一个用户在1,3,5,6登录，则视为连续6天登录。期望结果如下：
-- user_id	max_day_count
-- 100	3
-- 101	6
-- 102	3

select user_id, max( sum_flag )
from (
         select
             *
           , sum( `if`( flag = 1 or flag = 2, 1, 0 ) )
                  over (partition by user_id order by date_login_time) as sum_flag
         from (
                  select
                      user_id
                    , date_login_time
                      --   , row_number( ) over (partition by user_id order by date_login_time)
                    , lag( date_login_time, 1, date_login_time ) over (partition by user_id order by date_login_time)
                    , datediff( date_login_time,
                                lag( date_login_time, 1, date_login_time )
                                     over (partition by user_id order by date_login_time)
                          ) as flag
                  from (
                           select distinct user_id, date( login_datetime ) as date_login_time
                           from login_events le
                       ) t1
              ) t12
     ) t123
group by user_id
-- where flag=1 or flag=2
;

-- 核对
select
    user_id
  , max( recent_days ) as max_recent_days --求出每个用户最大的连续天数
from (
         select
             user_id
           , user_flag
           , datediff( max( login_date ), min( login_date ) ) + 1 as recent_days --按照分组求每个用户每次连续的天数(记得加1)
         from (
                  select
                      user_id
                    , login_date
                    , lag1_date
                    , concat( user_id, '_', flag ) as user_flag --拼接用户和标签分组
                  from (
                           select
                               user_id
                             , login_date
                             , lag1_date
                             , sum( if( datediff( login_date, lag1_date ) > 2, 1, 0 ) )
                                    over (partition by user_id order by login_date) as flag --获取大于2的标签
                           from (
                                    select
                                        user_id
                                      , login_date
                                      , lag( login_date, 1, '1970-01-01' )
                                             over (partition by user_id order by login_date) as lag1_date --获取上一次登录日期
                                    from (
                                             select
                                                 user_id
                                               , date_format( login_datetime, 'yyyy-MM-dd' ) as login_date
                                             from login_events
                                             group by user_id, date_format( login_datetime, 'yyyy-MM-dd' ) --按照用户和日期去重
                                         ) t1
                                ) t2
                       ) t3
              ) t4
         group by user_id, user_flag
     ) t5
group by user_id;


-- 第4题 日期交叉问题
-- 4.1 题目需求
-- 现有各品牌优惠周期表（promotion_info）如下，其记录了每个品牌的每个优惠活动的周期，其中同一品牌的不同优惠活动的周期可能会有交叉。
-- promotion_id	brand	start_date	end_date
-- 1	oppo	2021-06-05	2021-06-09
-- 2	oppo	2021-06-11	2021-06-21
-- 3	vivo	2021-06-05	2021-06-15
-- 现要求统计每个品牌的优惠总天数，若某个品牌在同一天有多个优惠活动，则只按一天计算。期望结果如下：
-- brand	promotion_day_count
-- vivo	17
-- oppo	16
-- redmi	22
-- huawei	22

select pi1.brand, min( pi1.start_date ), max( pi1.end_date )
from promotion_info pi1 join promotion_info pi2
                        on pi1.promotion_id != pi2.promotion_id
                            and pi1.start_date < pi2.end_date
                            and pi1.end_date > pi2.start_date
                            and pi1.brand = pi2.brand
group by pi1.brand


-- 去掉重合的. 使用 join 或者 exist 的方式, 只能确认是不是有过重合, 具体的数量很难计算.
select *
from promotion_info pi1 join promotion_info pi2
                        on pi1.promotion_id != pi2.promotion_id
                            and pi1.start_date < pi2.end_date
                            and pi1.end_date > pi2.start_date
                            and pi1.brand = pi2.brand
;

-- 参考
select
    brand
  , sum( datediff( end_date, start_date ) + 1 ) as promotion_day_count
from (
         select
             brand
           , max_end_date
              -- NOTE 使用 if, 很经典的判断
              -- 一发入魂, 不用迭代地去解决了
           , if( max_end_date is null or start_date > max_end_date, start_date,
                 date_add( max_end_date, 1 ) ) as start_date -- 这里采用了重合时, 就往后调整起始日期.
              -- NOTE 老老实实把原始数据弄出来, 最省力.
           , end_date
         from (
                  select
                      brand
                    , start_date
                    , end_date
                    , max( end_date )
                           over (partition by brand order by start_date rows between unbounded preceding and 1 preceding) as max_end_date
                  from promotion_info
              ) t1
     ) t2
where
    end_date > start_date
group by brand;
