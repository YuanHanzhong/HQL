-- STAR 同时在线人数 live_events
/*
1. 4列转3列，非常巧妙，两个列放到一个列里主要方便累加计数
2.
*/

-- 第一次
select
    live_id,
    max(sum_user)

from
    (
        
        select
            user_id,
            live_id,
            event_time,
            sum(tag) over (partition by live_id order by event_time) sum_user
        from
            (
                select
                    user_id,
                    live_id,
                    in_datetime event_time,
                    1           tag
                from
                    live_events le
                union all
                select
                    user_id,
                    live_id,
                    out_datetime event_time,
                    -1           tag
                from
                    live_events le
            ) t1
    
    ) t2
group by
    live_id;

-- 第二次
select
    live_id,
    max(sum_live_user)
from
    (
        select
            *,
            sum(flag) over (partition by live_id order by in_datetime) sum_live_user
        from
            (
                select
                    user_id,
                    live_id,
                    in_datetime,
                    1 flag
                from
                    (live_events le)
                union
                select
                    user_id,
                    live_id,
                    out_datetime,
                    -1 flag
                from
                    (live_events le)
            ) t1
    ) t2
group by
    live_id


--STAR 划分会话 page_view_events
/*
打标签技巧
*/

-- 第一次
select
    user_id,
    page_id,
    view_timestamp,
    concat(user_id, "__", sum(tag) over (partition by user_id order by view_timestamp)) session_id
from
    (
        select
            user_id,
            page_id,
            view_timestamp,
            window_size,
            if(window_size >= 60, 1, 0) tag
        
        from
            (
                select
                    *,
                    view_timestamp - lag_1_view_timestamp window_size
                
                from
                    (
                        select
                            *,
                            lag(view_timestamp, 1, 0)
                                over (partition by user_id order by view_timestamp) lag_1_view_timestamp
                        from
                            page_view_events pve
                    ) t
            ) t2
    ) t3
;
-- 第二次 
select
    *,
    concat(user_id, '_', sum(flag) over (partition by user_id order by view_timestamp))

from
    (
        select
            *,
            `if`(view_timestamp - lag(view_timestamp, 1, 0) over (partition by user_id order by view_timestamp) > 60, 1,
                 0) flag
        from
            (page_view_events)
    ) t1;

-- STAR 纯连续登录

-- 方法1：借助row_number, 注意要先去重

select
    user_id,
    date_sub_login_date,
    count(*) count
from
    (
        select
            user_id,
            login_date,
            date_sub(login_date, flag) date_sub_login_date
        from
            (
                select
                    user_id,
                    date(login_date)                                             login_date,
                    row_number() over (partition by user_id order by login_date) flag
                from
                    (
                        select distinct user_id, date(login_datetime) login_date
                        from login_events
                        order by user_id, login_date
                    ) t0
            ) t1
    ) t2
group by
    user_id, date_sub_login_date
    -- 有了having就能省掉一个子查询
    -- having
    --     count > 2
order by
    user_id, date_sub_login_date
;
-- 方法2：借助lag做标签，这个适应性更广。两个方法都要借助标签flag来统计登录天数
-- 注意这个也要去重。一看到连续登录就应该先想到去重。
select
    concat_flag,
    count(*)
from
    (
        select
            *,
            concat(user_id, '_', sum_flag) concat_flag
        from
            (
                select
                    *,
                    sum(flag_temp) over (partition by user_id order by date_login_date) sum_flag
                from
                    (
                        select
                            *,
                            `if`(flag_datediff > 1, 1, 0) flag_temp
                        from
                            (
                                select
                                    *,
                                    -- 打标记肯定是要相减的
                                    datediff(date_login_date, lag_login_date) flag_datediff
                                from
                                    (
                                        select
                                            user_id,
                                            date_login_date,
                                            lag(date_login_date, 1, "1970-01-01")
                                                over (partition by user_id order by date_login_date) lag_login_date
                                        from
                                            (
                                                select distinct user_id, date(login_datetime) date_login_date
                                                from login_events
                                            ) t1
                                    ) t2
                            ) t3
                    ) t4
            ) t5
    ) t6
group by
    concat_flag;



-- STAR 每个用户最大的连续登录天数，1天不登录也算是连续。 login_events
/*
    -- 做的过程中，一层层套比较好，知道到哪里了

*/

-- 第一次

select
    user_id,
    max(continued_days)
from
    (
        
        select
            user_id,
            first_day,
            max(datediff(last_day, first_day)) + 1 continued_days
        
        from
            (
                
                select
                    user_id,
                    user_flag,
                    first_value(login_date) over (partition by user_flag order by login_date) first_day,
                    last_value(login_date) over (partition by user_flag order by login_date)  last_day
                    --目的是计算连续登录的天数
                
                from
                    (
                        select
                            *,
                            concat(user_id, '_', sum_flag) user_flag
                        
                        from
                            (
                                select
                                    *,
                                    sum(if_flag) over (partition by user_id order by login_date) sum_flag
                                from
                                    (
                                        select
                                            *,
                                            if(datediff > 2, 1, 0) if_flag
                                            -- 这一步很关键，间隔1天的也给筛选出来了
                                        from
                                            (
                                                select
                                                    *,
                                                    datediff(login_date, lag_login_date) datediff
                                                    -- 做个标签
                                                from
                                                    (
                                                        select
                                                            user_id,
                                                            date(login_datetime)                                          login_date,
                                                            date(lag(login_datetime, 1, "1970-01-01")
                                                                     over (partition by user_id order by login_datetime)) lag_login_date
                                                        from
                                                            login_events le
                                                    
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
group by
    user_id
;

-- 第二次
select
    flag_user_sum,
    firstday,
    datediff(max(login_date), min(login_date)) + 1

from
    (
        select
            flag_user_sum,
            first_value(login_date) over (partition by flag_user_sum order by login_date) firstday,
            login_date
        from
            (
                select
                    *,
                    -- 关键在打标记这一步
                    concat(user_id, '_', sum(`if`(datediff(login_date, lag_login_date) > 2, 1, 0))
                                             over (partition by user_id order by login_date)) flag_user_sum
                from
                    (
                        select
                            user_id,
                            date(login_datetime)                                          login_date,
                            date(lag(login_datetime, 1, "1970-01-01")
                                     over (partition by user_id order by login_datetime)) lag_login_date
                        from
                            login_events
                    ) t1
            ) t2
    ) t3

group by
    flag_user_sum, firstday
order by
    flag_user_sum, firstday
;


-- STAR 日期交叉问题 promotion_info
select
    brand,
    sum(amount_day) over (partition by brand order by brand) -- 加上这一层之后报错 TODO
    -- [42000][3] Error while processing statement: FAILED: Execution Error,
    -- return code 3 from org.apache.hadoop.hive.ql.exec.spark.SparkTask.
    -- Spark job failed during runtime. Please check stacktrace for the root cause.
from
    (
        select
            brand,
            amount_day
        from
            (
                select
                    *,
                    datediff(end_date, real_start_date) + 1 amount_day
                from
                    (
                        select
                            promotion_id,
                            brand,
                            real_start_date,
                            end_date,
                            if(datediff(end_date, real_start_date) > 0, 1, 0) flag_guolv -- 直接用where即可，不用多此一举
                        from
                            (
                                select
                                    *,
                                    if(datediff(temp_start_date, start_date) > 0, temp_start_date,
                                       start_date) real_start_date
                                from
                                    (
                                        select
                                            *,
                                            lag(end_date, 1) over (partition by brand order by start_date) temp_start_date
                                        from (promotion_info)
                                    ) t1
                            ) t2
                    ) t3
                where
                    flag_guolv > 0
            ) t4
    ) t5
;


select
    brand,
    sum(amount_day)
from
    (
        select
            *,
            datediff(end_date, real_start_date) + 1 amount_day
        from
            (
                select
                    promotion_id,
                    brand,
                    real_start_date,
                    end_date,
                    if(datediff(end_date, real_start_date) > 0, 1, 0) flag_guolv
                from
                    (
                        -- 应该先筛选，再覆盖.
                        select
                            *,
                            if(datediff(temp_start_date, end_date) > 0, temp_start_date, start_date) real_start_date
                        from
                            (
                                select *, lag(end_date, 1,start_date) over (partition by brand order by start_date) temp_start_date
                                from (promotion_info)
                                
                            ) t1
                        where
                            temp_start_date < end_date

                    ) t2
                
            ) t3
        where
            flag_guolv > 0
    ) t4
group by
    brand
;

select
    *
from
    (promotion_info) 



-- 参考
select
    brand,
    sum(amount)
from
    (
        select
            brand,
            temp_start_date,
            end_date,
            datediff(end_date, temp_start_date) + 1 amount
        from
            (
                select
                    brand,
                    temp_start_date,
                    end_date,
                    `if`(datediff(start_date, temp_start_date) > 0, start_date, temp_start_date) real_start_date
                from
                    (
                        select
                            brand,
                            start_date,
                            lag(max_end_date, 1, start_date)
                                over (partition by brand order by start_date) temp_start_date,
                            end_date
                        from
                            (
                                -- 通过找规律，最大值选取时不包含本行
                                select
                                    *,
                                    max(end_date) over (partition by brand order by start_date) max_end_date
                                from
                                    (
                                        select
                                            *,
                                            lag(end_date, 1, end_date) over (partition by brand order by start_date) lag_end_date
                                        from (promotion_info pi)
                                    ) tt
                            ) t0
                    ) t1
                where
                    datediff(temp_start_date, end_date) <= 0
                -- TODO 什么时候用where，什么时候用flag筛选，什么时候用聚合sum，什么时候用开窗sum
            ) t2
    ) t3
group by
    brand
