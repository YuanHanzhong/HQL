-- 同时在线人数 live_events
/*
1. 4列转3列，非常巧妙，两个列放到一个列里主要方便累加计数
2.
*/


select user_id, live_id, sum( flag ) over (partition by live_id order by datetime)
from (
         select user_id, live_id, in_datetime as datetime, 1 as flag
         from live_events le
         union all
         select user_id, live_id, out_datetime as datetime, -1 as flag
         from live_events le
     
     ) t1



SELECT
    CAST( 8 / 7 as int )
;

-- 第一次
explain formatted

select
    live_id
  , max( sum_user )

from (
         
         select
             user_id
           , live_id
           , event_time
           , sum( tag ) over (partition by live_id order by event_time) as sum_user
         from (
                  select
                      user_id
                    , live_id
                    , in_datetime as event_time
                    , 1           as tag
                  from live_events le
                  union all
                  select
                      user_id
                    , live_id
                    , out_datetime as event_time
                    , -1           as tag
                  from live_events le
              ) t1
     
     ) t2
group by live_id;


-- 第二次
explain formatted
select
    live_id
  , max( sum_live_user )
from (
         select
             *
           , sum( flag ) over (partition by live_id order by in_datetime) as sum_live_user
         from (
                  select
                      user_id
                    , live_id
                    , in_datetime
                    , 1 as flag
                  from ( live_events le )
                  union
                  select
                      user_id
                    , live_id
                    , out_datetime
                    , -1 as flag
                  from ( live_events le )
              ) t1
     ) t2
group by live_id


-- 划分会话 page_view_events
/*
打标签技巧
*/

select *, sum( `if`( stay_time > 60, 1, 0 ) ) over (partition by user_id order by view_timestamp)
from (
         select
             *
           , view_timestamp
                 -
             lag( view_timestamp, 1, 0 ) over (partition by user_id order by view_timestamp) as stay_time
         from page_view_events pve
     ) t1


-- 第一次
select
    user_id
  , page_id
  , view_timestamp
  , concat( user_id, "__", sum( tag ) over (partition by user_id order by view_timestamp) ) as session_id
from (
         select
             user_id
           , page_id
           , view_timestamp
           , window_size
           , if( window_size >= 60, 1, 0 ) as tag
         
         from (
                  select
                      *
                    , view_timestamp - lag_1_view_timestamp as window_size
                  
                  from (
                           select
                               *
                             , lag( view_timestamp, 1, 0 )
                                    over (partition by user_id order by view_timestamp) as lag_1_view_timestamp
                           from page_view_events pve
                       ) t
              ) t2
     ) t3
;
-- 第二次 
select
    *
  , concat( user_id, '_', sum( flag ) over (partition by user_id order by view_timestamp) )

from (
         select
             *
           , `if`( view_timestamp - lag( view_timestamp, 1, 0 ) over (partition by user_id order by view_timestamp) >
                   60, 1,
                   0 ) as flag
         from ( page_view_events )
     ) t1;

--  纯连续登录

-- 方法1：借助row_number, 注意要先去重

select
    user_id
  , date_sub_login_date
  , count( * ) as count
from (
         select
             user_id
           , login_date
           , date_sub( login_date, flag ) as date_sub_login_date
         from (
                  select
                      user_id
                    , date( login_date )                                            as login_date
                    , row_number( ) over (partition by user_id order by login_date) as flag
                  from (
                           select distinct user_id, date( login_datetime ) as login_date
                           from login_events
                           order by user_id, login_date
                       ) t0
              ) t1
     ) t2
group by user_id, date_sub_login_date
       -- 有了having就能省掉一个子查询
       -- having
       --     count > 2
order by user_id, date_sub_login_date
;
-- 方法2：借助lag做标签，这个适应性更广。两个方法都要借助标签flag来统计登录天数
-- 注意这个也要去重。一看到连续登录就应该先想到去重。
select
    concat_flag
  , count( * )
from (
         select
             *
           , concat( user_id, '_', sum_flag ) as concat_flag
         from (
                  select
                      *
                    , sum( flag_temp ) over (partition by user_id order by date_login_date) as sum_flag
                  from (
                           select
                               *
                             , `if`( flag_datediff > 1, 1, 0 ) as flag_temp
                           from (
                                    select
                                        *
                                        -- 打标记肯定是要相减的
                                      , datediff( date_login_date, lag_login_date ) as flag_datediff
                                    from (
                                             select
                                                 user_id
                                               , date_login_date
                                               , lag( date_login_date, 1, "1970-01-01" )
                                                      over (partition by user_id order by date_login_date) as lag_login_date
                                             from (
                                                      select distinct user_id, date( login_datetime ) as date_login_date
                                                      from login_events
                                                  ) t1
                                         ) t2
                                ) t3
                       ) t4
              ) t5
     ) t6
group by concat_flag;



--  每个用户最大的连续登录天数，1天不登录也算是连续。 login_events
/*
    -- 做的过程中，一层层套比较好，知道到哪里了

*/

-- 第一次

select
    user_id
  , max( continued_days )
from (
         
         select
             user_id
           , first_day
           , max( datediff( last_day, first_day ) ) + 1 as continued_days
         
         from (
                  
                  select
                      user_id
                    , user_flag
                    , first_value( login_date ) over (partition by user_flag order by login_date) as first_day
                    , last_value( login_date ) over (partition by user_flag order by login_date)  as last_day
                      --目的是计算连续登录的天数
                  
                  from (
                           select
                               *
                             , concat( user_id, '_', sum_flag ) as user_flag
                           
                           from (
                                    select
                                        *
                                      , sum( if_flag ) over (partition by user_id order by login_date) as sum_flag
                                    from (
                                             select
                                                 *
                                               , if( datediff > 2, 1, 0 ) as if_flag
                                                 -- 这一步很关键，间隔1天的也给筛选出来了
                                             from (
                                                      select
                                                          *
                                                        , datediff( login_date, lag_login_date ) as datediff
                                                          -- 做个标签
                                                      from (
                                                               select
                                                                   user_id
                                                                 , date( login_datetime )                                           as login_date
                                                                 , date( lag( login_datetime, 1, "1970-01-01" )
                                                                              over (partition by user_id order by login_datetime) ) as lag_login_date
                                                               from login_events le
                                                           
                                                           ) t1
                                                  ) t2
                                         
                                         ) t3
                                
                                ) t4
                       ) t5
                  
                  order by user_id
              ) t6
         group by user_id, user_flag, first_day
         order by user_id
     ) t7
group by user_id
;

-- 第二次
select
    flag_user_sum
  , firstday
  , datediff( max( login_date ), min( login_date ) ) + 1

from (
         select
             flag_user_sum
           , first_value( login_date ) over (partition by flag_user_sum order by login_date) as firstday
           , login_date
         from (
                  select
                      *
                      -- 关键在打标记这一步
                    , concat( user_id, '_', sum( `if`( datediff( login_date, lag_login_date ) > 2, 1, 0 ) )
                                                 over (partition by user_id order by login_date) ) as flag_user_sum
                  from (
                           select
                               user_id
                             , date( login_datetime )                                           as login_date
                             , date( lag( login_datetime, 1, "1970-01-01" )
                                          over (partition by user_id order by login_datetime) ) as lag_login_date
                           from login_events
                       ) t1
              ) t2
     ) t3

group by flag_user_sum, firstday
order by flag_user_sum, firstday
;
