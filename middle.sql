-- 连续登录, 间隔 1 天也算
-- 第3题 间断连续登录用户问题
-- 3.1 题目需求
-- 现有各用户的登录记录表（login_events）如下，表中每行数据表达的信息是一个用户何时登录了平台。
-- user_id	login_datetime
-- 100	2021-12-01 19:00:00
-- 100	2021-12-01 19:30:00
-- 100	2021-12-02 21:01:00
-- 现要求统计各用户最长的连续登录天数，间断一天也算作连续，例如：一个用户在1,3,5,6登录，则视为连续6天登录。
-- 期望结果如下：
-- user_id	max_day_count
-- 100	3
-- 101	6
-- 102	3


select user_id, date_login, row_number( ) over (partition by user_id order by date_login)
from (
         select distinct user_id, date( login_datetime ) as date_login
         from login_events
     ) t1


-- 2.1 查询累积销量排名第二的商品
select *
from (
         select sku_id, sum_sku_num, dense_rank( ) over (order by sum_sku_num desc) as drk
         from (
                  select sku_id, sum( sku_num ) as sum_sku_num
                  from order_detail od
                  group by sku_id
              ) t1
     ) t2
where
    drk = 2;


-- 累计销量
select sku_id, amount_sku, dense_rank_amount_sku
from (
         -- order by, partition by 都可以多项, 排名的话常常跟着 desc
         select sku_id, amount_sku, dense_rank( ) over (order by amount_sku desc) as dense_rank_amount_sku
         from (
                  select sku_id, sum( sku_num ) as amount_sku
                  from order_detail od
                  group by sku_id
              ) t1
     ) t2
where
    dense_rank_amount_sku = 2
;



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
--  以后就用 row_number

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
                  
                  -- 最保险的排序就是在开窗函数里, partition by order by都要用
                  select
                      user_id
                    , create_date
                    , row_number( ) over (partition by user_id order by create_date) as row_number
                  from (
                           --  连续登录, 首先要考虑去重.
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
-- 开窗并不会去重
-- 开窗一般写上 partition by order by, 程序健壮
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

-- lag(字段)
-- 开窗函数不能直接放在 where 条件里, 聚合函数可以, 并且放在 having

select user_id, count( flag )
from (
         -- 涉及到日期的时候很特殊, 要用函数, 并且注意格式
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
-- 连续问题, 先去重

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
                  -- 连续问题, 先去重
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
             -- 直接开窗比 lag lead 好用很多
           , date_sub( create_date, row_number( ) over (partition by user_id order by create_date) ) as flag
         from (
                  select
                      user_id
                    , create_date
                  from order_info
                  group by user_id, create_date
              ) one_date_per_user_table
     ) t2 -- 判断一串日期是否连续：若连续，用这个日期减去它的排名，会得到一个相同的结果
group by user_id
         -- group by 的为必要出现在 select里面, 只是借助它进行了筛选
         -- 在 SELECT 子句中，除了聚合函数，只能使用出现在 GROUP BY 子句中的字段。
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

select sum( `if`( cn = 2, 1, 0 ) ), count( distinct user_id )
from (
         select user_id, flag, count( * ) as cn
         from (
                  -- 保证连续
                  select user_id, create_date, rn, date_sub( create_date, rn ) as flag
                  from (
                           select
                               user_id
                             , create_date
                             , row_number( ) over (partition by user_id order by create_date) as rn
                           from (
                                    select distinct user_id, create_date
                                    from order_info oi
                                ) t1
                       ) rn_tab
                  where
                      rn <= 2
              ) flag_tab
         
         group by user_id, flag
     ) continue_tab



select
    sum( `if`( flag = 1 and rn = 2, 1, 0 ) )
  , sum( `if`( rn = 1, 1, 0 ) )
from (
         select
             user_id
           , create_date
           , datediff( create_date, lag_create_date ) as flag
           , rn
         from (
                  select
                      user_id
                    , create_date
                    , lag( create_date, 1 ) over (partition by user_id order by create_date) as lag_create_date
                    , row_number
                          ( ) over (partition by user_id order by create_date)               as rn
                  
                  from (
                           select distinct user_id, create_date
                           from order_info oi
                       ) init_tab
              ) lag_tab
     ) rn



select sum( `if`( cn = 2, 1, 0 ) ) * 100.0, sum( if( cn = 1 or cn = 2, 1, 0 ) )
from (
         -- NOTE 连续, 一定是有 count 这个步骤的, 不管是使用 lag, 还是使用row_number()
         
         select user_id, flag, count( flag ) as cn
         from (
                  select user_id, create_date, rn, date_sub( create_date, rn ) as flag
                  from (
                           select
                               user_id
                             , create_date
                             , row_number( ) over (partition by user_id order by create_date) as rn
                           from (
                                    select distinct user_id, create_date
                                    from order_info oi
                                
                                ) init_table
                       ) tag_table
                  where
                      rn in ( 1, 2 )
              ) flag_table
         group by user_id, flag
     ) count_table
;


-- 打标记
select
from (
         select
             user_id
           , create_date
           , row_number( ) over (partition by user_id order by create_date) as rn
         
         from (
                  -- 基础数据
                  select distinct user_id, create_date
                  from order_info oi
              ) init_table
     ) flag_table
where
    rn in ( 1, 2 )


-- 做统计
select
    sum( if( rn = 2 and datediff( create_date, lag_1_create_date ) = 1, 1, 0 ) )
  , sum( `if`( rn = 1, 1, 0 ) )
from (
         -- 打标记
         select
             user_id
           , create_date
           , row_number( ) over (partition by user_id order by create_date)         as rn
           , lag( create_date, 1 ) over (partition by user_id order by create_date) as lag_1_create_date
         
         from (
                  -- 基础数据
                  select distinct user_id, create_date
                  from order_info oi
              ) t1
     ) t3
where
    -- 注意把后面无关的去掉, 时时刻刻精简着数据
    rn in ( 1, 2 )
;


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
-- 永远都是首选聚合函数
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
-- 2.11.1 题目需求
-- 从用户登录明细表（user_login_detail）和订单信息表（order_info）中查询每个用户的注册日期（首次登录日期）
-- 总登录次数以及其在2021年的登录次数、订单数和订单总额。期望结果如下：
-- user_id

-- 101	2021-09-21	5	5	4	143660.00
-- 102	2021-09-22	4	4	4	177850.00
-- 103	2021-09-23	2	2	4	75890.00

select
    sum_tab.user_id
  , register_date
  , amount_login_times
  , amount_login_times_2021
  , sum_order_times_2021
  , sum_total_amount_2021
from (
         select
             user_id
           , min( init_tab.login_date )                            as register_date
           , count( init_tab.login_date )                          as amount_login_times
           , sum( if( year( init_tab.login_date ) = 2021, 1, 0 ) ) as amount_login_times_2021
         from (
                  select user_id, date( login_ts ) as login_date, date( logout_ts ) as logout_date
                  from user_login_detail uld
              ) init_tab
         group by user_id
     )          amount_tab
     left join(
                  select
                      sum( `if`( year( create_date ) = 2021, total_amount, 0 ) ) as sum_total_amount_2021
                    , sum( `if`( year( create_date ) = 2021, 1, 0 ) )            as sum_order_times_2021
                    , user_id
                  from order_info oi
                  group by user_id
              
              ) sum_tab
     on sum_tab.user_id = amount_tab.user_id



select
    t1.user_id
  , first_login_date
  , amount_login_times
  , amount_login_times_2021
  , amount_order_times_2021
  , total_amount_2021
from (
         select
             user_id
           , min( date( login_ts ) )                      as first_login_date
           , count( login_ts )                            as amount_login_times
           , sum( `if`( year( login_ts ) = 2021, 1, 0 ) ) as amount_login_times_2021
         from user_login_detail uld
         group by user_id
     ) t1 left join (
                        select
                            user_id
                          , count( create_date ) as amount_order_times_2021
                          , sum( total_amount )  as total_amount_2021
                        from order_info oi
                        where
                            year( create_date ) = 2021
                        group by user_id
                    ) t2
          on t1.user_id = t2.user_id


-- 2023年09月21日15:12:57
select
    user_id
  , min( date( login_ts ) )                                  as first_login_date
  , count( * )                                               as amount_login_times
  , sum( if( year( login_ts ) = 2021, 1, 0 ) )               as amount_login_times_2021
  , sum( if( year( create_date ) = 2021, 1, 0 ) )            as amount_order_times_2021
    -- NOTE NOTE join之后,表的行数会变化, count 就不准了. 把数据先收齐并不一定是个好方式, 尤其和 count, sum不兼容.
  , sum( if( year( create_date ) = 2021, total_amount, 0 ) ) as total_amount_2021
from user_login_detail uld join order_info oi on oi.user_id = uld.user_id
group by uld.user_id
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
;



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
    -- 开窗不能课 group 一起用
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
             
             -- 必须要先计算, 再关联
           , count( 1 ) over (partition by oi.user_id)                        as total_login_count
         from user_login_detail uld join order_info oi on oi.user_id = uld.user_id
                                    join order_detail od on od.create_date = oi.create_date
     ) original_table
where
    year( login_ts ) = '2021'
group by user_id, register_date, total_login_count
;


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
         --  left join
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
                      --  row_number肯定是单调递增的
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
    -- count的时候写具体一点比较好
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

--,2.14 向用户推荐朋友收藏的商品 想不清楚, 再思考
-- 2.14.1
-- 题目需求
-- 现需要请向所有用户推荐其朋友收藏但是用户自己未收藏的商品，请从好友关系表（friendship_info
-- ）和收藏表（favor_info
-- ）中查询出应向哪位用户推荐哪些商品。期望结果如下：
-- 101	2
-- 101	4
-- 101	7

select self, friend, favor_info_friend.sku_id
from (
         select user1_id as self, user2_id as friend
         from friendship_info fi
         union
         select user2_id as self, user1_id as friend
         from friendship_info f
     )                    init_table
     join      favor_info favor_info_friend
     on friend = favor_info_friend.user_id
     left join favor_info favor_info_self
     on init_table.self = favor_info_self.user_id and favor_info_friend.sku_id = favor_info_self.sku_id
where
    favor_info_self.user_id is null



-- 2023年09月20日19:30:23 方法 1: 使用 left join is null 实现
--  首选 left join, left join更好理解, 注意起好别名
select friend_all.self_id, favor_info_friend.sku_id, *
from (
         -- 实际环境中, 不用 union. 要不然百万大 V 的推荐就太多了
         select user1_id as self_id, user2_id as friend_id
         from friendship_info fi
         union
         select user2_id as self_id, user1_id as friend_id
         from friendship_info f
     )                    friend_all
         --  join 要取别名,用的时候好用. 取别名区分高手和菜鸟
     left join favor_info favor_info_friend
     on friend_all.friend_id = favor_info_friend.user_id
                   
                   -- 再次 join 是为了去掉那些自己已经收藏的
     left join favor_info favor_info_self
     on friend_all.self_id = favor_info_self.user_id
         -- 此处必须是相同, 不喜欢的可就太多了,跟两者没有关系了
         and favor_info_friend.sku_id = favor_info_self.sku_id
where
    -- is null 本身就代表否定
    favor_info_self.user_id is null;


-- 2023年09月20日19:30:55
-- 方法 2: 事实 exist 实现, 效率更高
select distinct user_id, sku_id
from (
         -- 实际环境中, 不用 union. 要不然百万大 V 的推荐就太多了
         select user1_id as self_id, user2_id as friend_id
         from friendship_info fi
         union
         select user2_id as self_id, user1_id as friend_id
         from friendship_info f
     )               friendship_info_all
     join favor_info favor_info_friend on friendship_info_all.friend_id = favor_info_friend.user_id
     --  where 后面是个条件, 选择是不是要的条件
where
    not exists (
                   select 1
                   from favor_info favor_info_self
                   where
                         favor_info_self.user_id = friendship_info_all.self_id
                     and favor_info_self.sku_id = favor_info_friend.sku_id
               )
;



select *
from favor_info fi


-- GPT
SELECT
    f.user_id
  , fav.product_id
FROM friendship_info f
     JOIN
     favor_info      fav ON f.friend_id = fav.user_id
     LEFT JOIN
     favor_info      u_fav ON f.user_id = u_fav.user_id AND fav.product_id = u_fav.product_id
WHERE
    u_fav.product_id IS NULL
ORDER BY f.user_id, fav.product_id;


select distinct
    t1.user1_id AS user_id
  , i.sku_id
from (
         select user1_id, user2_id
         from friendship_info fi
         union
         select user2_id, user1_id
         from friendship_info f
     )                    t1
     left join favor_info i
     on t1.user2_id = i.user_id
where
    not exists (
                   select 1
                   from favor_info self_favor
                   where
                         self_favor.user_id = t1.user1_id
                     and self_favor.sku_id = i.sku_id
               
               )
;


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
                         friendship_info_full.user1_id = self_f.user_id
                     AND friend_favor.sku_id = self_f.sku_id
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
         --  join not exists 是成组的
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
         --  join not exists 是成组的
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


--  朋友的话, 是互为朋友, 要 union
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



-- 各种 join 之间的练习和区别
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

--  使用 left join 来扩展列


--  join的话, 一定要起别名
-- 把朋友的收藏放进来
     left join favor_info fi on t1.friend_id = fi.user_id
                   -- 把自己的收藏放进来
     left join favor_info on t1.user_id = favor_info.user_id
where
    fi.sku_id is null;
;


-- 去除某段字符,
-- substring
-- split
-- cast
-- date_format()


select 7.0;

select cast( 7.0 as datetime );

-- 2.15.1 题目需求
-- 从登录明细表（user_login_detail）中查询出，所有用户的连续登录两天及以上的日期区间，以登录时间（login_ts）为准。期望结果如下：
-- split 取出日期
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
       --  having count也是经常使用
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


-- 筛选出连续登录的用户
--  lag直接相等的方式只能筛选一个固定的值, 不能筛选连续多天

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

--  有了 over,就是原来基础上增加 1 列, 就不会去重了.
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
-- 2.18 购买过商品1和商品2但是没有购买商品3的顾客
-- 2.18.1 题目需求
-- 从订单明细表(order_detail)中查询出所有购买过商品1和商品2，但是没有购买过商品3的用户，期望结果如下：
-- user_id
-- 103
-- 105


--  array_contains()的样例用法
select *
from (
         select user_id, collect_set( sku_id ) as cs
         from order_detail od join order_info oi on oi.order_id = od.order_id
         group by user_id
     ) t1
where
    ( array_contains( cs, '1' ) or array_contains( cs, '2' ) ) and !array_contains( cs, '3' )



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

--  注意,  order by 后常常跟着 desc

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

--  null只能用is 来判断, 不可以用 大于 小于来判断

--2.22 查询相同时刻多地登陆的用户
--     2.22.1 题目需求
-- 从登录明细表（user_login_detail）中查询在相同时刻，多地登陆（ip_address不同）的用户，期望结果如下：
-- user_id(用户id)
-- 101
-- 102
-- 2023年09月21日17:10:12


select *
from (
         select user_id, ip_address, login_ts, logout_ts, last_max_logout_ts
         from (
                  select
                      user_id
                    , ip_address
                    , login_ts
                    , logout_ts
                    , max( logout_ts )
                           over (partition by user_id order by login_ts rows between unbounded preceding and 1 preceding) as last_max_logout_ts
                  from (
                           select user_id, ip_address, login_ts, logout_ts
                           from user_login_detail
                       ) init_table
              ) max_table
         where
             last_max_logout_ts > login_ts
     )                           fold_ts
     left join user_login_detail uld
     on fold_ts.user_id = uld.user_id and fold_ts.last_max_logout_ts = uld.logout_ts and
        fold_ts.ip_address != uld.ip_address
where
    uld.user_id is not null;


-- 2023年09月20日20:12:40

-- 找出重合, 用 max 准没错

-- 在重合的同时, ip 不同

select *
from (
         -- 把重合的找出来
         select *
         from (
                  select
                      user_id
                    , ip_address
                    , login_ts
                    , logout_ts
                    , max( logout_ts ) over (partition by user_id order by login_ts
                      rows between unbounded preceding and 1 preceding)
                          as last_logout_ts
                      -- 修饰不一定放在末尾, 怎么顺手怎么写
                  from user_login_detail
              ) flag_table
         where
             last_logout_ts > login_ts
     )                      t1
         -- 为了找出不同的 ip
     join user_login_detail uld
     on t1.user_id = uld.user_id
         and t1.last_logout_ts = uld.logout_ts
         and t1.ip_address != uld.ip_address
;



--2.23 销售额完成任务指标的商品
-- 商家要求每个商品每个月需要售卖出一定的销售总额
-- 假设1号商品销售总额大于1000，2号商品销售总额大于10，其余商品没有要求
-- 请写出SQL从订单详情表中（order_detail）查询连续两个月销售总额大于等于任务总额的商品




select *
from order_detail od

desc order_detail;


select sku_id, format_create_month, month_amount, lag_create_month, flag
from (
         
         select
             sku_id
           , format_create_month
           , month_amount
           , lag_create_month
           , datediff( format_create_month, lag_create_month ) as flag
         from (
                  select
                      sku_id
                    , format_create_month
                    , month_amount
                    , lag( format_create_month, 1, format_create_month )
                           over (partition by sku_id order by format_create_month) as lag_create_month
                  from (
                           select sku_id, concat( create_month, '-01' ) as format_create_month, month_amount
                           from (
                                    select sku_id, create_month, sum( amount ) as month_amount
                                    from (
                                             select
                                                 sku_id
                                               , date_format( create_date, 'yyyy-MM' ) as create_month
                                               , amount
                                             from (
                                                      select sku_id, create_date, price * sku_num as amount
                                                      from order_detail od
                                                  ) init_tab
                                         ) format_tab
                                    group by sku_id, create_month
                                ) month_amount_tab
                           
                           where
                               ( sku_id = 1 and month_amount > 1000 ) or ( sku_id = 2 and month_amount > 10 )
                       ) format_create_month_tab
              
              ) lag_tab
     ) datediff_tab
where
    flag > 27 and flag < 32
;



select sku_id, flag, count( * )
from (
         -- NOTE 4. 打标签
         select sku_id, ym, sum_amount, ymd, rn, add_months( ymd, -rn ) as flag
         from (
                  -- NOTE 3. 格式化
                  select
                      sku_id
                    , ym
                    , sum_amount
                    , concat( ym, '-01' )                                  as ymd
                      -- NOTE 月连续只能用 add_month
                    , row_number( ) over (partition by sku_id order by ym) as rn
                  
                  from (
                           -- NOTE 2. 格式化, 方便后续使用
                           select
                               sku_id
                             , date_format( create_date, 'yyyy-MM' ) as ym
                             , sum( sku_num * price )                as sum_amount
                           from (
                                    -- NOTE 1. 准备尽量少的原始数据, 高效而准确
                                    select *
                                    from order_detail od
                                    where
                                        -- 这里先写写, 减少计算量
                                        sku_id in ( 1, 2 )
                                ) init_tab
                           group by sku_id, date_format( create_date, 'yyyy-MM' )
                       ) ym_tab
                  where
                      ( sku_id = 1 and sum_amount > 1000 ) or ( sku_id = 2 and sum_amount > 10 )
              
              ) ymd
     ) flag_tab
group by sku_id, flag
having
    count( * ) >= 2
;


-- months_between算的并不是自然月, 其实很精确. , 先集中处理一下
select add_months( '2020-09-09', 1 )
select months_between( '2020-09-09', '2020-09-12' )

-- 2023年09月20日21:39:22 最优解, 高效而健壮
select
    sku_id
  , ym
  , ymd
  , months_between( ymd, lag_ymd )
from (
         
         select sku_id, ym, ymd, lag( ymd, 1, ymd ) over (partition by sku_id order by ymd) as lag_ymd
         from (
                  
                  -- 此处必须得分步骤
                  select sku_id, ym, concat( ym, '-01' ) as ymd
                  from (
                           select distinct sku_id, date_format( create_date, 'yyyy-MM' ) as ym
                           from (
                                    select sku_id, create_date
                                    from (
                                             select
                                                 sku_id
                                               , create_date
                                               , sum( price )
                                                      over (partition by sku_id,date_format( create_date, 'yyyy-MM' )) as sum_price_month
                                             from order_detail
                                         ) t1
                                    where
                                         ( sum_price_month > 1000 and sku_id = 1 )
                                      or ( sum_price_month > 10 and sku_id = 2 )
                                ) t2
                       ) t3
              ) t4
     
     ) t5
where
    months_between( ymd, lag_ymd ) = 1
;



select order_id, create_date, lag( create_date, 1, create_date ) over (partition by sku_id order by create_date)
from order_detail od



select sku_id, year_month, sum_price, if_1, if_2, create_date
from (
         select
             sku_id
           , create_date
           , year_month
           , sum_price
           , `if`( sku_id = 1 and sum_price > 2000, 1, 0 ) as if_1
           , `if`( sku_id = 2 and sum_price > 10, 2, 0 )   as if_2
         from (
                  select
                      sku_id
                    , year_month
                    , sum( price ) over (partition by sku_id,year_month) as sum_price
                    , create_date
                  from (
                           select sku_id, price, date_format( create_date, 'yyyy-MM' ) as year_month, create_date
                           from order_detail od
                       ) t1
              ) t2
     ) t3



select date_format( create_date, 'yyyy-MM' )
from order_detail od

-- 注意, month()只能去除月份, 不能取出年


--  partition可以是多个字段

--  日期格式的不正确, 会返回 null, 可以通过 concat 先转化为标准日期

-- 2023年09月20日21:32:00 最好的答案
select sku_id
from (
         
         -- concat先拼接一下, 凑成日期格式, 很美妙
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

-- 2.25 各品类销量前三的所有商品
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

select sku_id, category_id, drk
from (
         select sku_id, category_id, dense_rank( ) over (partition by category_id order by sum_sku_num) as drk
         from (
                  select category_id, od.sku_id, sum( sku_num ) as sum_sku_num
                  from order_detail od join sku_info si on si.sku_id = od.sku_id
                  group by category_id, od.sku_id
              ) init_table
     
     ) drk_table
where
    drk <= 3
;


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


select category_id, avg( price )
from (
         select
             category_id
           , sku_id
           , price
           , row_number( ) over (partition by category_id order by price) as rn
           , count( * ) over (partition by category_id)                   as cn
         from (
                  select category_id, sku_id, price
                  from sku_info si
              ) init_tab
     ) tag_tab
where
     ( cn % 2 = 0 and rn = cn / 2 or rn = cn / 2 + 1 )
  or ( cn % 2 = 1 and rn = cn / 2 + 1 )
group by category_id
;

--
select category_id, avg( price )
from (
         select
             category_id
             -- NOTE 开窗一定要考虑下 partition by 和 order by 之后是什么样子
           , count( * ) over (partition by category_id)                   as cn
           , price
           , row_number( ) over (partition by category_id order by price) as rn
         from sku_info si
     ) init_tab
where
  -- 偶数情况
  (
                  cn % 2 = 0
          -- NOTE  只取中间 2 行, 很巧妙
          and ( rn = cn / 2 or rn = cn / 2 + 1 )
      )
  or
  -- 奇数情况
  (
                  cn % 2 = 1
          -- 选出中间行
          and rn = cn / 2 + 1
      )

group by category_id
;


-- 奇数情况

select
    sku_id
  , price
  , rn
  , category_id
  ,
    -- NOTE case 用来打标签, 但是这个标签必须是自制的
    -- NOTE 多少行, 直接 count, 用 rn就麻烦了
from (
         select sku_id, price, row_number( ) over (partition by category_id order by price) as rn, category_id
         from sku_info si
     ) rn_tab
where
--     max( rn ) % 2 = 0


-- 偶数情况


select
    category_id
  , avg( price ) as middle_price
    -- 取出中间的值遇到了问题
from (
         select
             sku_id
           , category_id
           , price
           , count( * ) over (partition by category_id )                        as cn
           , count( * ) over (partition by category_id ) % 2                    as flag
             -- 用 row_number就一定要用 order by, 否则 row_number就没有意义了
           , row_number( ) over (partition by category_id order by price desc ) as rn
         from sku_info si
     ) t1
     -- 取出品类中商品数为偶数
     --  看奇数还是偶数就是用  %
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
             --  用 row_number就一定要用 order by, 否则 row_number就没有意义了
           , row_number( ) over (partition by category_id order by price desc ) as rn
         from sku_info si
     ) t1
     -- 取出品类中商品数为技术
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
         
         -- 全部为日期的时候才可以用 date_diff
         --  一个为日期格式, 另一个为整数的时候, 用 date_sub
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
-- 2021-09-21	            1               	0.0

select
    login_date
  , sum( `if`( rn = 1, 1, 0 ) ) as register
  , sum( if( rn = 1 and flag = 1, 1, 0 ) )
from (
         select
             login_date
           , lead_login_date
           
           , datediff( lead_login_date, login_date ) as flag
           , rn
         from (
                  select
                      -- 留存就得以 user_id为单位
                      login_date
                    , lead( login_date ) over (partition by user_id order by login_date) as lead_login_date
                      -- NOTE 用 rn 来看第一, 第二非常好用
                    , row_number( ) over (partition by user_id order by login_date)      as rn
                  
                  from (
                           select distinct user_id, date( login_ts ) as login_date
                           from user_login_detail
                       ) init_tab
              ) tag_tab
     ) rn_tab
group by login_date
;


-- 2023年09月23日15:17:00
select
    login_date
  , sum( if( rn = 1, 1, 0 ) )                                                   as register_amount
  , sum( `if`( rn = 1 and datediff( lead_login_date, login_date ) = 1, 1, 0 ) ) as retention_amount
from (
         select
             user_id
           , login_date
             -- 使用 rn, 为的是筛选出首次登录
           , row_number( ) over (partition by user_id order by login_date)         as rn
             -- 用 lag 为连续做准备
             -- NOTE, 应该使用 lead, 这里有微妙的区别
           , lead( login_date, 1 ) over (partition by user_id order by login_date) as lead_login_date
             --            , lag( login_date, 1 ) over (partition by user_id order by login_date) as lag_login_date_1
         from (
                  -- 去重
                  select distinct user_id, date( login_ts ) as login_date
                  from user_login_detail
              ) distinct_tab
     ) tag_tab
where
    rn <= 2
group by login_date
;


-- 2023年09月23日14:53:25
select
    *
  , `if`( rn = 1, 1, 0 )                                                as new_user
  , if( rn = 2 and datediff( login_date, lag_login_date_1 ) = 1, 1, 0 ) as retention
from (
         select
             user_id
           , login_date
             -- 使用 rn, 为的是筛选出首次登录
           , row_number( ) over (partition by user_id order by login_date)        as rn
             -- 用 lag 为连续做准备
           , lag( login_date, 1 ) over (partition by user_id order by login_date) as lag_login_date_1
         from (
                  -- 去重
                  select distinct user_id, date( login_ts ) as login_date
                  from user_login_detail
              ) distinct_tab
     ) tag_tab
;

-- NOTE sum(if()) 做统计, 一步到位


-- 参考

-- 新增数量和留存率
select
    t3.first_login
  , t3.register
  , t3.remain_1 / t3.register as retention
from (
         -- 每个用户首次登录时间 和 第二天是否登录 并看每天新增和留存数量
         
         select
             t1.first_login
           , count( t1.user_id ) as register
           , count( t2.user_id ) as remain_1
         from (
                  select
                      user_id
                    , date( min( login_ts ) ) as first_login
                  from user_login_detail
                  group by user_id
              )                 t1
              left join
              user_login_detail t2
              on
                          t1.user_id = t2.user_id
                      and
                          datediff( date( t2.login_ts ), t1.first_login ) = 1
         group by t1.first_login
     ) t3
;



-- 2.29 求出商品连续售卖的时间区间
-- 2.29.1 题目需求
-- 从订单详情表（order_detail）中，求出商品连续售卖的时间区间
-- 结果如下（截取部分）：
-- Sku_id（商品id）	Start_date（起始时间）	End_date（结束时间）
-- 1	2021-09-27	2021-09-27
-- 1	2021-09-30	2021-10-01
-- 1	2021-10-03	2021-10-08
-- 10	2021-10-02	2021-10-03


--  日期连续先去重
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

-- 2.30 登录次数及交易次数统计
-- 2.30.1 题目需求
-- 分别从登陆明细表（user_login_detail）和配送信息表中用户登录时间和下单时间统计登陆次数和交易次数
-- 结果如下（截取部分）：
-- User_id
-- （用户id）	Login_date
-- （登录时间）	login_count
-- （登陆次数）	Order_count
-- （交易次数）
-- 101	2021-09-21	1	0
-- 101	2021-09-27	1	1
-- 101	2021-09-28	1	1
-- 101	2021-09-29	1	1
-- 101	2021-09-30	1	1


-- 2023年09月21日17:01:20
select login_table.user_id, date_login_ts, date_order_date, login_times, order_times
from (
         select user_id, date( login_ts ) as date_login_ts, count( login_ts ) as login_times
         from user_login_detail uld
         group by user_id, date( login_ts )
     ) login_table left join (
                                 select
                                     user_id
                                   , date( order_date )   as date_order_date
                                   , count( delivery_id ) as order_times
                                 from delivery_info di
                                 group by user_id, date( order_date )
                             
                             ) delivery_table
                   on login_table.user_id = delivery_table.user_id


select *
from delivery_info


select t1.user_id, order_date, nvl( order_times, 0 ) as order_times, login_date, login_times
from (
         -- 用户的登录次数
         select
             user_id
           , date_format( login_ts, 'yyyy-MM-dd' ) as login_date
             -- 不能使用 count(*)
           , count( * )                            as login_times
         from user_login_detail uld
         group by user_id, date_format( login_ts, 'yyyy-MM-dd' )
         -- 使用 join 的事后,一般使用 left join
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

;

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
    --   日期可以直接用来比较大小, 计算具体天数时需要用 datediff, date_sub.
    --   null 只能用 is 来判断
    --  and if or 连接的一定是逻辑

group by sku_id

select date( '2023-09-13' )
select CAST( '2023-09-13' AS DATE );


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

--  想要扩展列, 就是使用 left join, join 会导致数据丢失
--  涨幅肯定是新的减旧的
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


select
    user_id
  , max( if( rn = 1, create_date, 0 ) ) as first_order_date
    
    -- NOTE 日期只可以放到 max 里面,不可以放到 min 里面, min 之后就成了 0 了
  , max( if( rn = 2, create_date, 0 ) ) as second_order_date
  , count( * )

from (
         select user_id, name, od.create_date, row_number( ) over (partition by user_id order by od.create_date) as rn
         from order_info oi join order_detail od on od.order_id = oi.order_id
                            join sku_info si on si.sku_id = od.sku_id
         where
             name in ( 'xiaomi 10', 'xiaomi 13', 'apple 12' )
     ) init_table
group by user_id
;



select user_id, count( * ), min( od.create_date ) as first_order_date
from order_info oi join order_detail od on od.order_id = oi.order_id
                   join sku_info si on si.sku_id = od.sku_id
where
    name in ( 'xiaomi 10', 'xiaomi 13', 'apple 12' )
group by user_id
having
    count( * ) >= 2



select *
from sku_info si

select *
from order_info oi

select *
from order_detail od


--  case when then end相当于 if, case 使用范围更广


select
    user_id
    -- coalesce取出第一个非空的
    --  这里使用 MAX, 非常巧妙
    --  另外一个 MAX 的用法就是取出一个用到了聚合函数的东西
  , max( CASE WHEN rn = 1 THEN create_date END ) as first_buy_date
  , max( `if`( rn = 2, create_date, null ) )     as second_buy_date
  , count( * )                                   as cn
from (
         select
             user_id
           , name
           , create_date
           , row_number( ) over (partition by user_id order by create_date ) as rn
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
having
    count( * ) >= 2
;


--  first value的妙用: 可以取出第一行, 第二行
--  last value 结合 bounded unbounded可以非常灵活取数
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
    --  使用 in 要比使用很多 or 强很多
    si.name in ( 'xiaomi 10', 'apple 12', 'xiaomi 13' )


-- 2.35 同期商品售卖分析表 STAR
-- 从订单明细表（order_detail）中。
-- 求出同一个商品在2021年和2020年中同一个月的售卖情况对比。
-- 结果如下（截取部分）：
-- Sku_id（商品id）
-- Month-- （月份）
-- 2020_skusum -- （2020销售量）
-- 2021_skusum -- （2021销售量）
-- 1	9	0	11
-- 1	10	2	38
-- 10	10	94	205

-- 2023年09月23日20:34:03 强壮, 牛逼

select
    sku_id
  , month( create_date )
  , sum( `if`( year( create_date ) = 2020, sku_num, 0 ) )
  , sum( `if`( year( create_date ) = 2021, sku_num, 0 ) )
from order_detail od
group by sku_id, month( create_date )



select
    sku_id
  , create_month
  , sum( `if`( create_year = 2020, sum_sku_num, 0 ) )
  , sum( `if`( create_year = 2021, sum_sku_num, 0 ) )
from (
         select
             sku_id
           , year( create_date )  as create_year
           , month( create_date ) as create_month
           , sum( sku_num )       as sum_sku_num
         from (
                  select sku_id, create_date, sku_num
                  from order_detail od
              ) init_tab
         group by sku_id, year( create_date ), month( create_date ), sku_num
     ) sum_tab
group by sku_id, create_month
-- NOTE group by 谁,这一步很关键


-- 写了半天是错的, 主要是因为没有审清楚题


select
    sku_id
  , month_create_date
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


-- GPT 再对比
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
;



-- 2.37 统计活跃间隔对用户分级结果 STAR
-- 忠实用户：近7天活跃且非新用户
-- 新晋用户：近7天新增
-- 沉睡用户：近7天未活跃但是在7天前活跃
-- 流失用户：近30天未活跃但是在30天前活跃
-- 假设今天是数据中所有日期的最大值，从用户登录明细表 user_login_detail 中的用户登录时间给各用户分级，求出各等级用户的人数

-- 结果如下：
-- Level（用户等级）	Cn（用户数量)
-- 忠实用户	6
-- 新增用户	3
-- 沉睡用户	1


select user_id, max( login_date ) as latest_login_date, min( login_date ) as register_date
from (
         select distinct user_id, date( login_ts ) as login_date,
         from user_login_detail uld
     ) t1
group by user_id;
;

select level, count( user_id ) as cn
from (
         
         select
             user_id
           , case
                 when
                             datediff( max( last_active_date ) over (), last_active_date ) <= 7 and
                             datediff( max( last_active_date ) over (), register_date ) > 7 then 'loyal'
                 when
                     datediff( max( last_active_date ) over (), register_date ) <= 7        then 'new'
                 when datediff( max( last_active_date ) over (), last_active_date ) > 7     then 'sleep'
                 when datediff( max( last_active_date ) over (), last_active_date ) > 30    then 'go'
             
             end as level
         
         from (
                  
                  select
                      user_id
                    , max( active_date ) as last_active_date
                    , min( active_date ) as register_date
                  from (
                           select distinct user_id, active_date
                           from (
                                    -- 按天连续的,先去重
                                    select distinct user_id, date( login_ts ) as active_date
                                    from user_login_detail
                                    union all
                                    select distinct user_id, date( logout_ts ) as active_date
                                    from user_login_detail uld
                                ) union_tab
                       ) init_tab
                  group by user_id
              ) max_tab
     ) tag_tab
group by level
;



select flag, count( * )
from (
         
         select
             user_id
           , case
                 when datediff( max( last_active ) over (), first_active ) > 7
                     and datediff( max( last_active ) over (), last_active ) <= 7
                                                                               then 'loyal'
                 when datediff( max( last_active ) over (), first_active ) < 7 then 'new'
                 when datediff( max( last_active ) over (), last_active ) > 7  then 'sleep'
                 -- NOTE 只要一使用聚合函数, 就要 group by, 除非 over 一下.
                 -- NOTE 很多次用一个聚合函数的时候就分层, 减少重复计算
                 when datediff( max( last_active ) over (), last_active ) > 30 then 'go'
             end as flag
         from (
                  
                  select
                      user_id
                      -- NOTE 用户等级就使用 min 和 max
                    , min( ts ) as first_active
                    , max( ts ) as last_active
                  
                  from (
                           select distinct user_id, date( login_ts ) as ts
                           from user_login_detail uld
                           union
                           -- 有时候是 union, 有时候是 union all, 具体情况具体分析
                           select distinct user_id, date( logout_ts ) as ts
                           from user_login_detail u
                       
                       ) init_tab
                  group by user_id
              ) tag_tab
     ) case_tab

group by flag
;
-- 2023年09月23日11:55:42
select level, count( user_id )
from (
         
         select
             user_id
           , last_active_date
           , first_active_date
           , today
             -- NOTE 打标签, case 最强,没有之一
           , case
                 when datediff( today, last_active_date ) <= 7 and datediff( today, first_active_date ) > 7 then 'loyal'
                 when datediff( today, first_active_date ) <= 7                                             then 'new'
                 when datediff( today, last_active_date ) > 7                                               then 'sleep'
                 when datediff( today, last_active_date ) > 30                                              then 'go'
             end as level
         from (
                  select user_id, last_active_date, first_active_date, max( last_active_date ) over () as today
                  from (
                           select
                               user_id
                             , max( ts ) as last_active_date
                             , min( ts ) as first_active_date
                           from (
                                    select user_id, date( login_ts ) as ts
                                    from user_login_detail
                                    union all
                                    select user_id, date( logout_ts ) as ts
                                    from user_login_detail
                                ) init_tab
                           group by user_id
                       ) flag_table
              ) all_data_table
     
     ) tag_tab
group by level
;


-- 参考

select
    t2.level
  , count( * )
from (
         select
             uld.user_id
             -- NOTE case when then 竖着罗列
             -- NOTE sum(if()) 横着罗列
           
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
                  -- 给表格加 1 列
              join
              (
                  select
                      max( date( logout_ts ) ) as today
                    , max( date( login_ts ) )  as toda
                  
                  from user_login_detail
              )                 t1
         
         group by uld.user_id, t1.today
     ) t2
group by t2.level
;

--  真实环境中, 只需要用函数取当前日期即可.


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
                 --  取整, 除法的时候要 floor 一下
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

-- 2.39 国庆期间的7日动销率和滞销率 STAR STAR STAR
-- 2.39.1 题目需求
-- 动销率定义为品类商品中一段时间内有销量的商品占当前已上架总商品数的比例（有销量的商品/已上架总商品数）。
-- 滞销率定义为品类商品中一段时间内没有销量的商品占当前已上架总商品数的比例。（没有销量的商品 / 已上架总商品数）。
-- 只要当天任一店铺有任何商品的销量就输出该天的结果
-- 从订单明细表（order_detail）和商品信息表（sku_info）表中求出国庆7天每天每个品类的商品的动销率和滞销率


select *
from order_detail od


select *
from sku_info si


-- 2023年09月23日17:28:52

-- NOTE 日期格式字符串, 有非常严格的格式要求, 0 不可以省略


select sum_sku_sale_01, cn
from (
         select
             category_id
           , sum( `if`( create_date = '2021-10-01', 1, 0 ) ) as sum_sku_sale_01
           , sum( `if`( create_date = '2021-10-02', 1, 0 ) ) as sum_sku_sale_02
           , sum( `if`( create_date = '2021-10-03', 1, 0 ) ) as sum_sku_sale_03
         from (
                  select od.sku_id, category_id, create_date
                  from order_detail od join sku_info si on si.sku_id = od.sku_id
              
              ) init_tab
         group by category_id
     )      sum_tab
         -- NOTE 想要去个数, 旧的 join, 没有别的选择
     join (
              select
                  category_id
                , count( * )                                   as cn
                , sum( if( from_date <= '2020-04-01', 1, 0 ) ) as all_sku_01
                , sum( if( from_date <= '2020-02-01', 1, 0 ) ) as all_sku_02
                , sum( if( from_date <= '2020-03-01', 1, 0 ) ) as all_sku_03
              from sku_info s
              group by category_id
          ) cn_tab
     on cn_tab.category_id = sum_tab.category_id


-- 2023年09月23日17:00:15
select
    od.sku_id
  , od.sku_num
  , category_id
  , create_date
  , from_date
  , sum( if( create_date = '2021-10-01', 1, 0 ) ) over (partition by category_id order by create_date) as sum_order_sku
    
    -- NOTE 全部商品数量, 这一步一定得在 join 之前计算, 否则会有很多重复的
  , sum( if( from_date <= '2021-10-01', 1, 0 ) ) over (partition by category_id)                       as sum_all_sku

from order_detail od left join sku_info si on si.sku_id = od.sku_id


-- 99行
select *
from order_detail od


-- 12行
select *
from sku_info si


select *
from order_detail od join sku_info si on si.sku_id = od.sku_id


select *
from order_detail od left join sku_info si on si.sku_id = od.sku_id

select *
from order_detail


select
    sum( if( from_date < '2021-10-01', 1, 0 ) ) over (partition by category_id)  as on_shelf_2021_10_01
  , sum( if( from_date < '2021-10-02', 1, 0 ) ) over ( partition by category_id) as on_shelf_2021_10_02
from sku_info si



select *
from order_detail od


-- NOTE 很多时候在 count 的时候, 在 join 之前, 否则函数就变了


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


-- GPT 2023年09月23日11:04:29
-- 查询每个品类在指定日期范围内的动销率和滞销率

-- 主查询：计算每天每个品类的动销率和滞销率
select
    t2.category_id
    -- 第1天的动销率和滞销率
  , t2.day_1 / t3.cn_day1     as day_1_active_rate
  , 1 - t2.day_1 / t3.cn_day2 as day_1_inactive_rate
    -- 同理，重复上述逻辑为其他天数，例如：
  , 1 - t2.day_7 / t3.cn_day7 as day_7_inactive_rate
from (
         -- 子查询t2: 按品类汇总每天的动销商品数量
         select
             si.category_id
           , sum( if( od.create_date = '2021-10-01', 1, 0 ) ) as day_1
             -- 重复上述逻辑为其他天数，例如：
           , sum( if( od.create_date = '2021-10-07', 1, 0 ) ) as day_7
         from order_detail  od
              join sku_info si on od.sku_id = si.sku_id
              -- 只选择指定日期范围内的数据，并确保商品已上架
         where
             od.create_date between '2021-10-01' and '2021-10-07' and si.from_date <= od.create_date
         group by si.category_id
     )      t2
     join (
              select
                  category_id
                , sum( if( sku_info.from_date <= '2021-10-01', 1, 0 ) ) as cn_day1
                , sum( if( sku_info.from_date <= '2021-10-02', 1, 0 ) ) as cn_day2
                  -- ... 为每天重复相同的逻辑 ...
                , sum( if( sku_info.from_date <= '2021-10-07', 1, 0 ) ) as cn_day7
              from sku_info
          ) t3 on t2.category_id = t3.category_id
group by t2.category_id
;


select
    t2.category_id
  , coalesce( t2.day_1, 0 ) / t3.total_count     as day_1_active_rate
  , 1 - coalesce( t2.day_1, 0 ) / t3.total_count as day_1_inactive_rate
  ,
    -- 重复上述逻辑为其他天数
    -- ...
    coalesce( t2.day_7, 0 ) / t3.total_count     as day_7_active_rate
  , 1 - coalesce( t2.day_7, 0 ) / t3.total_count as day_7_inactive_rate

from (
         -- 子查询t2: 计算每天每个品类的动销商品数量
         select
             si.category_id
           , sum( if( od.create_date = '2021-10-01', 1, 0 ) ) as day_1
           ,
             -- 重复上述逻辑为其他天数
             -- ...
             sum( if( od.create_date = '2021-10-07', 1, 0 ) ) as day_7
         from order_detail od
              join
              sku_info     si
              on od.sku_id = si.sku_id
         where
               od.create_date between '2021-10-01' and '2021-10-07'
           and si.from_date <= od.create_date -- 考虑商品的上架日期
         group by si.category_id
     ) t2
     join
     (
         -- 子查询t3: 计算每个品类的总上架商品数量
         select
             category_id
           , count( * ) as total_count
         from sku_info
         where
             sku_info.from_date <= '2021-10-07' -- 只计算在此日期或之前上架的商品
         group by category_id
     ) t3
     on t2.category_id = t3.category_id;
;



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
                       -- 跟日期相关的时候, 一定要用字符串
                  where
                      create_date <= '2021-10-07' and create_date >= '2021-10-01'
              
              ) t1
         group by category_id
     )      t22
     join (
              --  想要动态获得某些数据, 就是使用 join 的方式
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
                       -- 缩小范围, 有助于提高效率
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


-- 2.40 同时在线最多的人数
-- 2.40.1 题目需求
-- 根据用户登录明细表（user_login_detail），求出平台同时在线最多的人数。
-- 结果如下：
-- Cn（人数）
-- 7
-- login logout , 先登录的后登出怎么算

select max( sum_flag )
from (
         select user_id, ts, flag, sum( flag ) over (order by ts) as sum_flag
         from (
                  select user_id, login_ts as ts, 1 as flag
                  from user_login_detail uld
                  union all
                  select user_id, logout_ts as ts, -1 as flag
                  from user_login_detail u
              ) flag_tab
     ) sum_tab



select *
from user_login_detail uld1 join user_login_detail uld2
                            on (
                                        uld1.user_id = uld2.user_id
                                    and uld1.login_ts != uld2.login_ts
                                    and uld1.login_ts < uld2.logout_ts
                                    and uld1.logout_ts > uld2.login_ts
                                )


-- 找出所有的有重合的, 并合并
select uld1.user_id, min( uld1.login_ts ) as unique_login_ts, max( uld1.logout_ts ) as unique_logout_ts
from user_login_detail uld1 join user_login_detail uld2
                            on (
                                        uld1.user_id = uld2.user_id
                                    --  很有必要, 不自连接.
                                    and uld1.login_ts != uld2.login_ts
                                    and uld1.login_ts < uld2.logout_ts
                                    and uld1.logout_ts > uld2.login_ts
                                )
group by uld1.user_id
;

-- 找出所有的没有重合的
-- 使用 left join where is null 实现没有重叠

select *
from user_login_detail uld1 left join user_login_detail uld2
                            on (
                                        uld1.user_id = uld2.user_id
                                    and uld1.login_ts != uld2.login_ts
                                    and uld1.login_ts < uld2.logout_ts
                                    and uld1.logout_ts > uld2.login_ts
                                )
where
    uld2.user_id is null
;


--  使用 EXIST 实现没有重叠
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

--  间隔连续问题, 只能用 lag, 因为使用 row_number不再有规律
-- 连续 3 天

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
             user_id
           , login_date
           , flag
           , sum( if( flag = 1 or flag = 2, 1, 0 ) )
                  over (partition by user_id order by login_date) as sum_flag
         from (
                  
                  select user_id, login_date, datediff( login_date, lag_logindate_1 ) as flag
                  from (
                           select
                               user_id
                             , login_date
                             , lag( login_date, 1, login_date )
                                    over (partition by user_id order by login_date) as lag_logindate_1
                           
                           from (
                                    select distinct user_id, date( login_datetime ) as login_date
                                    from login_events le
                                ) init_table
                       ) lag_table
              ) flag_table
         -- NOTE 别名应当体现目的,
         -- NOTE 起别名重视起来, 体现了代码修养
     ) max_sum_flag_table
group by user_id
;


-- NOTE 可以用 limit 看看表结构, 但是最后一定要去掉 limit

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
                    , row_number( ) over (partition by user_id order by date_login_time)
                    , lag( date_login_time, 1, date_login_time ) over (partition by user_id order by date_login_time)
                    , datediff( date_login_time,
                                lag( date_login_time, 1, date_login_time )
                                     over (partition by user_id order by date_login_time)
                          ) as flag
                  from (
                           select distinct user_id, date( login_datetime ) as date_login_time
                           from login_events le
                       ) init_table
              ) flag_table
     ) sum_table

group by user_id
;



-- 第4题 日期交叉问题 STAR STAR STAR
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


select brand, sum( datediff( end_date, new_start_date ) + 1 )
from (
         select
             brand
           , start_date
           , end_date
           , last_end_date
             -- P1 if 这里判断很关键
           , `if`( last_end_date <= start_date or last_end_date is null, start_date,
                 -- NOTE 日期不能直接相加, 最多只是用来比较大小
                   date_add( last_end_date, 1 ) ) as new_start_date
         from (
                  select
                      brand
                    , start_date
                    , end_date
                      -- last_end_date只是用来铺路的, 不是真的要计算, 是为了计算出新的 start_date
                    , max( end_date ) over (partition by brand order by start_date
                      rows between unbounded preceding and 1 preceding ) as last_end_date
                  from promotion_info pi
              ) max_tab
         -- 去掉完全包含的情况 P1
         --         where             last_end_date < end_date
     ) new_start_date_tab

where
    new_start_date <= end_date
group by brand
;



select brand, sum( datediff( end_date, new_start_date ) + 1 ) as promotion_days
from (
         select
             brand
           , start_date
           , end_date
           , max_last_enddate
           , if( max_last_enddate is null or max_last_enddate < start_date, start_date,
                 date_add( max_last_enddate, 1 ) ) as new_start_date
         from (
                  select
                      brand
                    , start_date
                    , end_date
                    , max( end_date )
                           over (partition by brand order by start_date
                               rows between unbounded preceding and 1 preceding) as max_last_enddate
                  from promotion_info
              ) max_last_enddate_table
     ) new_start_date
where
    new_start_date <= end_date
group by brand
;


-- 参考
select
    brand
  , sum( datediff( end_date, start_date ) + 1 ) as promotion_day_count
from (
         select
             brand
           , max_end_date
           --  使用 if, 很经典的判断
           -- 一发入魂, 不用迭代地去解决了
           , if( max_end_date is null or start_date > max_end_date, start_date,
                 --  日期不能直接加减
                 date_add( max_end_date, 1 ) ) as start_date -- 这里采用了重合时, 就往后调整起始日期.
           --  老老实实把原始数据弄出来, 最省力.
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
    end_date >= start_date
group by brand;



1)
建表语句
drop table if exists live_events;
create table if not exists live_events
(
    user_id      int comment '用户id',
    live_id      int comment '直播id',
    in_datetime  string comment '进入直播间时间',
    out_datetime string comment '离开直播间时间'
)
    comment '直播间访问记录';
2
）数据装载
INSERT overwrite table
    live_events
VALUES
    ( 100, 1, '2021-12-01 19:00:00', '2021-12-01 19:28:00' )
  , ( 100, 1, '2021-12-01 19:30:00', '2021-12-01 19:53:00' )
  , ( 100, 2, '2021-12-01 21:01:00', '2021-12-01 22:00:00' )
  , ( 101, 1, '2021-12-01 19:05:00', '2021-12-01 20:55:00' )
  , ( 101, 2, '2021-12-01 21:05:00', '2021-12-01 21:58:00' )
  , ( 102, 1, '2021-12-01 19:10:00', '2021-12-01 19:25:00' )
  , ( 102, 2, '2021-12-01 19:55:00', '2021-12-01 21:00:00' )
  , ( 102, 3, '2021-12-01 21:05:00', '2021-12-01 22:05:00' )
  , ( 104, 1, '2021-12-01 19:00:00', '2021-12-01 20:59:00' )
  , ( 104, 2, '2021-12-01 21:57:00', '2021-12-01 22:56:00' )
  , ( 105, 2, '2021-12-01 19:10:00', '2021-12-01 19:18:00' )
  , ( 106, 3, '2021-12-01 19:01:00', '2021-12-01 21:10:00' );
1.3
代码实现
select
    live_id
  , max( user_count ) as max_user_count
from (
         select
             user_id
           , live_id
           , sum( user_change ) over (partition by live_id order by event_time) as user_count
         from (
                  select
                      user_id
                    , live_id
                    , in_datetime as event_time
                    , 1           as user_change
                  from live_events
                  union all
                  select
                      user_id
                    , live_id
                    , out_datetime
                    , -1
                  from live_events
              ) t1
     ) t2
group by live_id;
第
2
题 会话划分问题
2.1
题目需求
现有页面浏览记录表（page_view_events
）如下，表中有每个用户的每次页面访问记录。

user_id	page_id	view_timestamp
100	home	1659950435
100	good_search	1659950446
100	good_list	1659950457
100	home	1659950541
100	good_detail	1659950552
100	cart	1659950563
101	home	1659950435
101	good_search	1659950446
101	good_list	1659950457
101	home	1659950541
101	good_detail	1659950552
101	cart	1659950563
102	home	1659950435
102	good_search	1659950446
102	good_list	1659950457
103	home	1659950541
103	good_detail	1659950552
103	cart	1659950563
规定若同一用户的相邻两次访问记录时间间隔小于60s
，则认为两次浏览记录属于同一会话。现有如下需求，为属于同一会话的访问记录增加一个相同的会话id
字段，期望结果如下：
user_id	page_id	view_timestamp	session_id
100	home	1659950435	100-1
100	good_search	1659950446	100-1
100	good_list	1659950457	100-1
100	home	1659950541	100-2
100	good_detail	1659950552	100-2
100	cart	1659950563	100-2
101	home	1659950435	101-1
101	good_search	1659950446	101-1
101	good_list	1659950457	101-1
101	home	1659950541	101-2
101	good_detail	1659950552	101-2
101	cart	1659950563	101-2
102	home	1659950435	102-1
102	good_search	1659950446	102-1
102	good_list	1659950457	102-1
103	home	1659950541	103-1
103	good_detail	1659950552	103-1
2.2
数据准备
1)
建表语句
drop table if exists page_view_events;
create table if not exists page_view_events
(
    user_id        int comment '用户id',
    page_id        string comment '页面id',
    view_timestamp bigint comment '访问时间戳'
)
    comment '页面访问记录';
2
）数据装载
insert overwrite table
    page_view_events
values
    ( 100, 'home', 1659950435 )
  , ( 100, 'good_search', 1659950446 )
  , ( 100, 'good_list', 1659950457 )
  , ( 100, 'home', 1659950541 )
  , ( 100, 'good_detail', 1659950552 )
  , ( 100, 'cart', 1659950563 )
  , ( 101, 'home', 1659950435 )
  , ( 101, 'good_search', 1659950446 )
  , ( 101, 'good_list', 1659950457 )
  , ( 101, 'home', 1659950541 )
  , ( 101, 'good_detail', 1659950552 )
  , ( 101, 'cart', 1659950563 )
  , ( 102, 'home', 1659950435 )
  , ( 102, 'good_search', 1659950446 )
  , ( 102, 'good_list', 1659950457 )
  , ( 103, 'home', 1659950541 )
  , ( 103, 'good_detail', 1659950552 )
  , ( 103, 'cart', 1659950563 );
2.3
代码实现
select
    user_id
  , page_id
  , view_timestamp
  , concat( user_id, '-', sum( session_start_point ) over (partition by user_id order by view_timestamp) ) as session_id
from (
         select
             user_id
           , page_id
           , view_timestamp
           , if( view_timestamp - lagts >= 60, 1, 0 ) as session_start_point
         from (
                  select
                      user_id
                    , page_id
                    , view_timestamp
                    , lag( view_timestamp, 1, 0 ) over (partition by user_id order by view_timestamp) as lagts
                  from page_view_events
              ) t1
     ) t2;
第
3
题 间断连续登录用户问题
3.1
题目需求
现有各用户的登录记录表（login_events
）如下，表中每行数据表达的信息是一个用户何时登录了平台。
user_id	login_datetime
100	2021-12-01 19:00:00
100	2021-12-01 19:30:00
100	2021-12-02 21:01:00
现要求统计各用户最长的连续登录天数，间断一天也算作连续，例如：一个用户在1,3,5,6
登录，则视为连续6
天登录。期望结果如下：
user_id	max_day_count
100	3
101	6
102	3
104	3
105	1
3.2
数据准备
1)
建表语句
drop table if exists login_events;
create table if not exists login_events
(
    user_id        int comment '用户id',
    login_datetime string comment '登录时间'
)
    comment '直播间访问记录';
2
）数据装载
INSERT overwrite table
    login_events
VALUES
    ( 100, '2021-12-01 19:00:00' )
  , ( 100, '2021-12-01 19:30:00' )
  , ( 100, '2021-12-02 21:01:00' )
  , ( 100, '2021-12-03 11:01:00' )
  , ( 101, '2021-12-01 19:05:00' )
  , ( 101, '2021-12-01 21:05:00' )
  , ( 101, '2021-12-03 21:05:00' )
  , ( 101, '2021-12-05 15:05:00' )
  , ( 101, '2021-12-06 19:05:00' )
  , ( 102, '2021-12-01 19:55:00' )
  , ( 102, '2021-12-01 21:05:00' )
  , ( 102, '2021-12-02 21:57:00' )
  , ( 102, '2021-12-03 19:10:00' )
  , ( 104, '2021-12-04 21:57:00' )
  , ( 104, '2021-12-02 22:57:00' )
  , ( 105, '2021-12-01 10:01:00' );
3.3
代码实现
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

第
4
题 日期交叉问题
4.1
题目需求
现有各品牌优惠周期表（promotion_info
）如下，其记录了每个品牌的每个优惠活动的周期，其中同一品牌的不同优惠活动的周期可能会有交叉。
promotion_id	brand	start_date	end_date
1	oppo	2021-06-05	2021-06-09
2	oppo	2021-06-11	2021-06-21
3	vivo	2021-06-05	2021-06-15
现要求统计每个品牌的优惠总天数，若某个品牌在同一天有多个优惠活动，则只按一天计算。期望结果如下：
brand	promotion_day_count
vivo	17
oppo	16
redmi	22
huawei	22
4.2
数据准备
1)
建表语句
drop table if exists promotion_info;
create table promotion_info
(
    promotion_id string comment '优惠活动id',
    brand        string comment '优惠品牌',
    start_date   string comment '优惠活动开始日期',
    end_date     string comment '优惠活动结束日期'
) comment '各品牌活动周期表';
2
）数据装载
insert overwrite table
    promotion_info
values
    ( 1, 'oppo', '2021-06-05', '2021-06-09' )
  , ( 2, 'oppo', '2021-06-11', '2021-06-21' )
  , ( 3, 'vivo', '2021-06-05', '2021-06-15' )
  , ( 4, 'vivo', '2021-06-09', '2021-06-21' )
  , ( 5, 'redmi', '2021-06-05', '2021-06-21' )
  , ( 6, 'redmi', '2021-06-09', '2021-06-15' )
  , ( 7, 'redmi', '2021-06-17', '2021-06-26' )
  , ( 8, 'huawei', '2021-06-05', '2021-06-26' )
  , ( 9, 'huawei', '2021-06-09', '2021-06-15' )
  , ( 10, 'huawei', '2021-06-17', '2021-06-21' );
4.3
代码实现
select
    brand
  , sum( datediff( end_date, start_date ) + 1 ) as promotion_day_count
from (
         select
             brand
           , max_end_date
           , if( max_end_date is null or start_date > max_end_date, start_date,
                 date_add( max_end_date, 1 ) ) as start_date
           , end_date
         from (
                  select
                      brand
                    , start_date
                    , end_date
                    , max( end_date )
                           over (partition by brand order by start_date
                               rows between unbounded preceding and 1 preceding) as max_end_date
                  from promotion_info
              ) t1
     ) t2
where
    end_date > start_date
group by brand;
