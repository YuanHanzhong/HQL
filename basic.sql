-- 查看基本信息
show functions;
show databases like 'de*';
-- 匹配数据库的名字和表的名字时,使用*而不是%
-- 很多关键字和数据库匹配时,语法有些不同
show tables like "bu*";



desc database extended default;
desc business;
desc extended business;

use default;

-- 开窗函数的基本语法

select *, count() over (rows between 3 preceding and 4 following)
from business b;

select *, count() over (rows between unbounded preceding and unbounded following)
from business b;

select *, count() over (rows between 3 preceding and unbounded following)
from business b;

select *, count() over (rows between current row and 5 following)
from business b;

select *, count() over (rows between unbounded preceding and current row)
from business b;



select *, count() over ( partition by name order by orderdate rows between unbounded preceding and unbounded following )
from business b;


select *, count(*) over (rows between unbounded preceding and unbounded following)
from business b;

select *, count(*) over (rows between unbounded preceding and current row)
from business b;

-- 能不嵌套的就不嵌套
-- count
--      开窗时, count 里面可以没有参数
--      如果不开窗, 就一定要参数, 并且一定要和 group by连用
select name, count() over (partition by name rows between unbounded preceding and unbounded following)
from business b
where month(orderdate) = 4;

-- 想要消除重复行,2种方式

-- group by
--      自然消除冗余行
--      是 count 的必要条件

select distinct(name), cn
from (select name,
             count() over (partition by month(orderdate) rows between unbounded preceding and unbounded following) cn
      from business b
      where month(orderdate) = 4) t1
;

SELECT month(orderdate)     as month_date,
       collect_set(name)    as name_list,
       count(DISTINCT name) as cn
-- 不是只有 count(*), 还有 count 某个字段, count distinct 某字段
FROM business
GROUP BY month(orderdate)
ORDER BY month_date;


-- 1. 有了所有的数据, 再来一层加工, 但是有必要吗
select *, sum(cost) over (partition by month(orderdate))
from business b;

select *, sum(cost) over (partition by name,month(orderdate))
from business b;

select *, sum(cost) over (partition by name)
from business b;

select *, sum(cost) over (order by orderdate)
from business b;

-- 开窗函数和聚合函数有什么不同? 有什么优势和劣势? 为什么有了聚合函数还要发明开窗函数?

-- 集合
--      无序
--      去重
select collect_set(name)
from business b
;

-- 列表
--      有顺序
--      保持原样
select collect_list(name)
from business b;


-- row_number()如何使用
select *, row_number() over ()
from business b;



select *, count(*) row_number()
from business b;

select *
from business b;



explain formatted
select name,
       orderdate,
       cost,
       count(*) over (rows between unbounded preceding and unbounded following)
from business
;

explain formatted
select count(*)
from business b;


-- 2. 购买过的累加人次并保留所有信息
-- 我的
select name,
       orderdate,
       cost,
       row_number() over ()
from business
;

select name,
       orderdate,
       cost,
       count(*) over (rows between unbounded preceding and current row) cn
from business;


-- 3. 购买过的总人数
select name,
       count(*) over (rows between unbounded preceding and unbounded following)
from business
group by name
;


-- 4. 购买过的累加人数
select name,
       count(*) over (rows between unbounded preceding and current row )
from business
group by name
;
-- 2022年4月份购买过的顾客及总人数 STAR

-- 有了开窗 over 之后, 肯定会加列 NOTE
-- 当聚合函数之外没有列时, 也就不用 group by
select count(*)
from business b
where month(orderdate) = 4

---- 为何不能执行?
select *
from (select name, month(orderdate) ordermonth, cost
      from business b) t1
where ordermonth = 4
;

select *
from (select name, month(orderdate) ordermonth, cost
      from business b)

select *
from (select name, month(orderdate) ordermonth, cost
      from business b) t23

select
from (select name, month(orderdate) ordermonth, cost
      from business b) t2
;

desc business;

-- 11）求出每个顾客上一次和当前一次消费的和并保留明细

-- 当有红色标记的时候, 看看是不是单词写错了. 灰色标记是因为没有使用.


select name
     , orderdate
     , cost
     , sum(cost) over (partition by name order by orderdate rows between 1 preceding and current row)
from business b;

-- 这个开窗运用的很巧妙
select *, sum(cost) over (partition by name order by orderdate rows between 1 preceding and current row) last_two
from business b;


-- lag lead 等等用法

-- coalesce
select *,
       coalesce(cost, 0) + coalesce(preceed_cost, 0)
       -- 在 select 里两列相加的时候, 效率很低, I/O 也很大, 尽量避免.
       -- 它擅长统计列
-- 消除 null
--      lag lead 的时候,设置 default
--      coalesce
--      nvl

-- lag
from (select *, lag(cost, 1, 0) over (partition by name order by orderdate) preceed_cost
      from business b) t1;

--  ifnull
select *, nvl(preceed_cost, 0)
from (select *, lag(cost) over (partition by name order by orderdate) preceed_cost
      from business b) t1;

show tables;

-- 窗口, 不仅仅是多了一列, 还可以在列上滚动.


-- rank 做实验
-- rank dense_rank ntile row_number lag lead 这些函数不支持窗口子句。因为窗口子句是动态变化的, 并且某一行可能被使用多次.


-- ntile 必须要和 order by 一起使用, 有了顺序才能划分组 NOTE
select name, cost, ntile(3) over (order by name)
from business b;

select *, row_number() over (partition by name order by orderdate)
from business b;

select *, rank() over (partition by name order by orderdate)
from business b;

select *, dense_rank() over (partition by name order by orderdate)
from business b;

-- 很多函数必须要写参数,否则会报错
-- 如果有多个开窗函数, 他们之间是没有关系的

select *
     , first_value(orderdate) over (partition by month(orderdate) order by name )

     , last_value(orderdate) over (partition by month(orderdate) order by name)
from business b;

select *
     , last_value(orderdate) over (partition by month(orderdate) order by name)
     , first_value(orderdate) over (partition by name order by orderdate )
from business b;


select name,
       orderdate,
       cost,
       first_value(orderdate)
                   over (partition by name,month(orderdate) order by orderdate rows between UNBOUNDED PRECEDING and UNBOUNDED following) first_value,
       first_value(orderdate)
                   over (partition by name,month(orderdate) order by orderdate rows between UNBOUNDED PRECEDING and UNBOUNDED following) first_value,
       last_value(orderdate)
                  over (partition by name,month(orderdate) order by orderdate )                                                          last_value1,
       last_value(orderdate) over (partition by name,month(orderdate) order by orderdate )                                               last_value2
from business

-- 查询前20%时间的订单信息
select *
from (select *, ntile(5) over (order by orderdate) nt
      from business b) t1
where nt = 1
;

select *, dense_rank() over (partition by name order by sum(cost) desc)
from business b;


select *
from (select name, sum(cost) sum_cost

      from business b
      group by name) t1
order by sum_cost;

-- 不用 group by 求总和
select *
     , sum(cost) over (partition by name order by cost)
from business b
;

select *
     , sum(cost) over (partition by name )
from business b
;

select *
from (select name, month(orderdate) ordermonth, cost
      from business b) t1
select name
     , count(*) over (rows between unbounded preceding and unbounded following)
from (select distinct(name) name
      from (select name, month(orderdate) order_month, cost
            from business b) t1
      where order_month = 4) t2
-- 嵌套就要起别名
-- 如何消除重复的
--      distinct
--      group by


-- 如何通过执行计划对比两个 sql 的执行效率
--     1. 看 vertics, 顶点


explain extended
select name,
       count(*) over (rows between unbounded preceding and unbounded following)
from (select name
      from (select name, month(orderdate) date_month
            from business b) t1
      where date_month = 4
      group by name) t2
;

select name,
       count(*) over ()
from (select name
      from business b
      where month(orderdate) = 4 -- where后的条件不一定要出现在select后面
     ) t1
group by name
;


-- 最简单写法
select name
     , count(*) over ()
from business b
where month(orderdate) = 4
group by name;


select name,
       count(*) over () cn
from business
where month(orderdate) = 4
group by name;


--  6）查询顾客的月购买总额
select *, sum(cost) over (partition by month(orderdate))
from business b
;



select name,
       orderdate,
       cost,
       sum(cost) over (partition by month(orderdate))
from business;
--     7）查询每个顾客的购买明细及购买总额
select name,
       orderdate,
       cost,
       sum(cost) over (partition by name)
from (business b)

-- 8）查询每个顾客每个月的购买明细及购买总额
select name,
       month(orderdate),
       cost,
       sum(cost) over (partition by name,month(orderdate))
from (business b)

--     9）按照日期将cost累加并保留明细
select *,
       sum(cost) over (order by orderdate)
from (business b)
-- 10）按照日期将每个顾客cost累加并保留明细
select *,
       sum(cost) over (partition by name order by orderdate)
from (business b)
-- 11）求出每个顾客上一次和当前一次消费的和并保留明细
select *,
       sum(cost) over (partition by name order by orderdate rows between 1 preceding and current row )
from (business b)
-- 12）查询每个顾客购买明细以及上次的购买时间和下次购买时间
select *,
       lead(orderdate, 1) over (partition by name order by orderdate),
       lag(orderdate, 1) over (partition by name order by orderdate)
from (business b)

select name,
       orderdate,
       ntile(2) over (partition by name order by orderdate)
from (business b);
-- 13）查询顾客每个月第一次的购买时间 和 每个月的最后一次购买时间


-- [42000][40000] Error while compiling statement: FAILED: NullPointerException null
select *,
       first_value(orderdate) over (partition by name,month(orderdate) order by orderdate),
       last_value(orderdate)
                  over (partition by name,month(orderdate) order by orderdate rows between unbounded preceding and unbounded following)
from (business b)

select name,
       orderdate,
       cost,
       first_value(orderdate)
                   over (partition by name,month(orderdate) order by orderdate rows between UNBOUNDED PRECEDING and UNBOUNDED following) first_value,
       last_value(orderdate)
                  over (partition by name,month(orderdate) order by orderdate rows between UNBOUNDED PRECEDING and UNBOUNDED following)  last_value
from business
-- 14）查询前20%时间的订单信息
select *
from (select *, ntile(5) over (partition by name order by orderdate) ntile_5
      from (business b)) t1
where ntile_5 = 1
-- 15）按照花费的金额进行排名


select *,
       dense_rank() over (order by sum_cost desc),
       rank() over (order by sum_cost desc),
       row_number() over (order by sum_cost desc)
from (select *, sum(cost) over (partition by name) sum_cost
      from (business b)) t1

-- 16）按照每个顾客花费的金额进行排名
select *,
       rank() over (order by cost desc),
       row_number() over (order by cost desc)
from (business b)

select name,
       orderdate,
       cost,
       rank() over (partition by name order by cost desc)       rk,
       dense_rank() over (partition by name order by cost desc) drk,
       row_number() over (partition by name order by cost desc) drk
from business
;
show functions;

desc function extended lag;
select 8 * 8;

select 9 * 9;

select upper(9 * 9);

select upper("jack");
select substr("jack", 3, 2);

select trim("  wo jack  ");

-- 对空值的处理
--      nvl() 理解成 no value
--      coalesce()

select nvl(null, 88);
select concat_ws("_", 'a', 'b', 'col int not null');
select get_json_object()
-- json
select get_json_object('[{"name":"大海海","sex":"男","age":"25"},
    {"name":"小宋宋","sex":"男","age":"47"}]', '$.[0]');


select from_utc_timestamp(1659946088, "")

select from_unixtime(1659946088);

select unix_timestamp('2022/08/08 08-08-08', 'yyyy/MM/dd HH-mm-ss')
select to_unix_timestamp('2022/08/08 08-09-10', 'yyyy/MM/dd HH-mm-ss')

select date_format('2022-08-08 08:08:08', 'yyyy年/MM月/dd日/hh时/mm分/ss秒');


select *
from (employee e)

-- 4）每个月的入职人数


select distinct month(hiredate_fromatted)                                                                 month_num,
                count(*) over (partition by month(hiredate_fromatted) order by month(hiredate_fromatted)) cn_month
from (select name, hiredate, replace(hiredate, '/', '-') hiredate_fromatted from employee e) e


-- 使用 month()函数的时候,对数据格式有要求
--      日期
--      非空


-- 最复杂的一种
select month_hiredate,
       count(*)
from (select *,
             month(replace_hiredate) month_hiredate
      from (select *, replace(hiredate, '/', '-') replace_hiredate from (employee e)) t1) t2
group by month_hiredate

order by month_hiredate
;

-- 优化
select month(replace(hiredate, '/', '-')) month,
       count(*)                           cn
from (employee e)
group by month(replace(hiredate, '/', '-'))
;


select month(replace(hiredate, '/', '-')) as month,
       count(*)                           as cn
from employee
group by month(replace(hiredate, '/', '-'))
-- 每个人年龄（年 + 月）star


-- 最基础的数据拿出来, 放到一起
select name
     , birthday
     , year(`current_date`())             current_year
     , month(`current_date`())            current_month
     , year(replace(birthday, '/', '-'))  birth_year
     , month(replace(birthday, '/', '-')) birth_month
from employee e;


select name
     , birthday
     , `if`(current_month < birth_month, current_year - birth_year - 1, current_year - birth_year)      `years_old`
     , `if`(current_month < birth_month, 12 + current_month - birth_month, current_month - birth_month) `month_old`
from (
         -- 最基础的数据拿出来, 放到一起 NOTE 非常标准, 非常棒
         select name
              , birthday
              , year(`current_date`())             current_year
              , month(`current_date`())            current_month
              , year(replace(birthday, '/', '-'))  birth_year
              , month(replace(birthday, '/', '-')) birth_month
         from employee e) t1
;


-- 日期函数都对格式有要求
select datediff(`current_date`(), '2023-08-20');

select year(`current_date`())



-- 首次写
-- 最内层对数据进行预处理
-- 反复出现的函数应该提取出来, 作为原始数据 NOTE
-- 灰色注释代表没有使用
select name, concat(years_old, '岁', monthes, '个月')
from (select *
           , datediff(`current_date`()
        , birthday_formatted)
           -- if的格式, 怎么写更好看一些
           -- 函数重复使用, 一定会乱, 最基础的起别名就好了

           , `if`(
                month(`current_date`()) > month(birthday_formatted)
        , year(`current_date`()) - year(birthday_formatted)
        , year(`current_date`()) - year(birthday_formatted) - 1
        )                                                           years_old
           , `if`(
                month(`current_date`()) > month(birthday_formatted)
        , month(`current_date`()) - month(birthday_formatted)
        , month(`current_date`()) - month(birthday_formatted) + 12) monthes
      from (select name, (replace(birthday, '/', '-')) birthday_formatted
            from employee e) t1) t12

explain
select *
from (select name
      from (select name, sum(salary)
            from employee
            group by name) t1) t2

set hive.mapjoin.smalltable.filesize
-- 方法1：通过函数取出年和月
explain
select name,
       abs_year,
       abs_month,
       r_birthday,
       concat(age_year, "岁", age_month, "月") age
from (select name,
             abs_year,
             abs_month,
             r_birthday,
             `if`(abs_month < 0, abs_year - 1, abs_year)    age_year, --可以在这里concat，但只能用很长的式子，是就会显得乱
             `if`(abs_month < 0, 12 + abs_month, abs_month) age_month
      from (select name,
                   r_birthday,
                   year(`current_date`()) - year(r_birthday)   abs_year,
                   month(`current_date`()) - month(r_birthday) abs_month
            from (select *, replace(birthday, '/', '-') r_birthday
                  from (employee e)) t1) t12) t3
;

-- 方法二：通过取整，取余算出. 由于闰年原因，直接除以365不准确，用if判断则更麻烦
select name,
       r_birthday,
       datediff(`current_date`(), r_birthday) days
from (select *, replace(birthday, '/', '-') r_birthday
      from (employee e)) t1


select datediff('1999-9-8', '1999-7-8');


select month('1999-9-8')


-- 6）按照薪资，奖金的和进行倒序排序，如果奖金为null，置位0
select name,
       salary + nvl(bonus, 0) sum
from (employee e)
order by sum desc
-- 7）每个人有多少个朋友
-- 巧妙使用内置函数, 能省很大的力气. 比如集合或者 map 中的元素的数量

select collect_set(name)
from employee e;

select collect_list(name)
from employee e;

desc employee;

select size(children)
from employee e
;



select name, size(friends)
from employee e;

select name,
       friends,
       size(friends)
from (employee e)
-- 8）每个人的孩子的姓名
select name,
       children,
       map_keys(children) children_name,
       map_values(children),
       array_contains(friends, '小')
from (employee e);



-- 9）每个岗位男女各多少人
-- 首选的还是 group by, 他写起来简单
-- 未必用开窗函数就把问题弄简单了, 首选的还是 group by
select job,
       sex,
       count(*)
from (employee e)
group by job, sex
order by job, sex;


select job, sum(`男`) `男人数`, sum(`女`) `女人数`
from (
         -- 第一步就是把基本数据摘出来
         select name, sex, job, if(sex = '男', 1, 0) `男`, if(sex = '女', 1, 0) `女`
         from employee e) t1
group by job;


-- 每个 ==== group by


-- limit 限制的是具体的内容
-- 看表的结构使用* + limit 更专业一点

-- 10）每个岗位男女各多少人,结果要求如下
-- 每个岗位 ==== group by 岗位 NOTE

-- 总结1列变2列的方法
-- 一看到加列, 就想到开窗
-- 统计的第一步就是量化, 先量化, 是则为 1,不是则为 0


-- 都写到一起了, 并不好, 不方便维护
select job,
       sum(if(sex = "男", 1, 0))   `男`,
       sum(`if`(sex = '女', 1, 0)) `女`
from (employee e)
group by job;


-- 3.3.3 查询同姓（假设每个学生姓名的第一个字为姓）的学生名单并统计同姓人数大于2的姓
-- 给列填充 NOTE
--      if 判断
--      都填充 1, 直接 select 1
select first_name, sum(one) sum_one, collect_set(stu_name)
from (select stu_name, substr(stu_name, 1, 1) first_name, 1 one
      from student s) t1
group by first_name
-- having的好处就是减少一层 where
having sum_one >= 2
;

desc student;

select *
from student s
;

WITH SurnameCounts AS (SELECT
                           LEFT (stu_name, 1) AS surname, -- 提取学生姓名的第一个字符
                           COUNT(*) AS count
                       FROM
                           students -- 假设你的表名是students
                       GROUP BY
                           LEFT (stu_name, 1)
                       HAVING
                           COUNT(*)
                            > 2 -- 只选择那些人数大于2的姓
)

SELECT *
FROM student s2
         JOIN
     SurnameCounts sc ON
         LEFT(s.stu_name, 1) = sc.surname
ORDER BY
    sc.surname, s.stu_id;


-- 很多时候可以直接拿出来用, 不用放到 select 里面多加一层


select *
from student s
;
-- 准备基础数据

-- 灰色代表没有用过
-- 按照姓氏分组
select `First Name`
from (select *, substr(stu_name, 1, 1) `First Name`
      from student) t1
group by `First Name`
having count(*) >= 2
-- count(*) 不一定要在 select里面, 也可以在 having 中
;

select *
from student
where substr(stu_name, 1, 1) in
      (select `First Name`
       from (select *, substr(stu_name, 1, 1) `First Name`
             from student) t1
       group by `First Name`
       having count(*) >= 2);

select collect_list(stu_name)
from student s;


select first_name,
       count(*)
from (select *, substr(stu_name, 0, 1) first_name
      from (student s)) t1
group by first_name
having count(*) >= 2
;

-- 3.4.2 [课堂讲解]按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示 STAR
-- 学生id 语文 数学 英语 有效课程数 有效平均成绩

select *
from course c

select *
from score s

-- 原始数据
select *
from course c
         join score s on s.course_id = c.course_id



--
-- 1列变多列问题, sum if group by
select stu_id,
       sum(`if`(course_name = '语文', course, 0)) `语文`
       -- NOTE sum if 就是在一列变多列的时候使用
-- partition by不会去重, 只是多了一列
from (select stu_id, course_name, course
      from course c
               join score s on s.course_id = c.course_id) t1

group by stu_id
order by stu_id
;


select stu_id,
       sum(`if`(course_name = "语文", course, 0)) `语文`,
       sum(`if`(course_name = "数学", course, 0)) `数学`,
       sum(`if`(course_name = "数学", course, 0)) `数学`,
       sum(`if`(course_name = "体育", course, 0)) `体育`,
       sum(`if`(course_name = "音乐", course, 0)) `音乐`,
       count(*)                                   `有效课程数`,
       sum(course)                                `总成绩`,
       sum(course) / count(*)                     `平均成绩`,
       count(c.course_id)                         `有效科目数量`,
       count(*)                                   `有效科目数量`
-- count(*) 会统计所有行, 包括空行, NOTE
-- count(name) 会过滤掉 name 列中, 不为 null 的值
from score s
         join course c on c.course_id = s.course_id
group by stu_id


select *
from score s
         join course c on c.course_id = s.course_id

desc course;
desc student;
desc score;

-- 4.1.1 [课堂讲解]查询所有课程成绩均小于60分的学生的学号、姓名  STAR
select *
from student s
where stu_id not in(
    select s.stu_id
    from score s
    where course>60
) ;
-- from 某个表后, 一般需要起别名
-- in的时候不用给子查询起别名, NOTE

-- 2023年08月29日, 这个方法太麻烦了, 根本没必要开窗做标记.
-- 开窗做 flag, 功能强大, 但同时也太麻烦. NOTE
select stu_id,stu_name
from student s2
where s2.stu_id
          -- 所有的时候, 用 not in更准确 NOTE
          not in
      (select distinct (stu_id)
       from (select *, sum(bigger_60) over (partition by stu_id) sum_bigger_60
             from (select stu_id, course_id, course, `if`(course > 60, 1, 0) bigger_60
                   from score s) t1) t2
       where sum_bigger_60 > 0)
;


-- 最高效
select *
from student s3
where s3.stu_id not in
      (select distinct s.stu_id
       from score s
       where s.course > 60)
;


select *
from score s
where stu_id='002'

select s.stu_id
from score s
         join course c on c.course_id = s.course_id
         join student s2 on s2.stu_id = s.stu_id
where s.course_id > 60
-- 所有的, 都. ==== 所有的减掉任意一个 NOTE

select *
from course c;

select *
from score s
where s.course > 60
;



;


--  比对下，先查询出来再join效率最高
select s.stu_id,
       stu_name
from student s
         -- 要什么给什么即可, 没有必要全部都弄出来
         join (
-- 开窗, 设置标志位, 筛选是非常好的方式. 设置标志位非常巧妙 NOTE

    select stu_id, sum(if(course < 60, 0, 1)) sum_under_60
    from score s
    group by stu_id
    having sum_under_60 < 1) t1 on s.stu_id = t1.stu_id


-- 5.1.1 [课堂讲解]查询有两门以上的课程不及格的同学的学号及其平均成绩 STAR

-- 2023年08月29日
select stu_id,count(*) cn,avg(course)
from score s
where course<60
group by stu_id
having cn>2;





select stu_id,
       sum(if(course < 60, 1, 0)) sum_under_60,
       avg(course)
       -- 使用 avg 更能反应平均成绩, 而使用 sum/count(*) 则不行, 因为 count(*) 可能会算上空行. 至少得 count(列名)
-- 平均的算法, 主要区别就是对 null 的统计 NOTE
--      1. avg, 最简单
--              为 null 时, 不会算上. 为 0 会统计
--      2. sum(score)/count(*), 为 null 会统计
--      3. sum(score)/count(course_id), 兼容性最强, 准确性最高
--              空行就不是 null 了,null 只是针对单元格来说的. count 不会统计 null 的单元格


from score s
group by stu_id
having sum_under_60 > 2

-- 5.2.6 [课堂讲解]查询学过“李体音”老师所教的所有课的同学的学号、姓名
-- 思路: 把所有问题转化为统计问题, 然后恰好等于

-- 李老师教过的课
select course_id
from teacher t
         join course c on c.tea_id = t.tea_id
where tea_name = '李体音';

-- 学过任意一门课
select *
from student s
         join course c
where c.course_id in
      (select course_id
       from teacher t
                join course c on c.tea_id = t.tea_id
       where tea_name = '李体音') t1

group by s

-- 参考答案
select st.stu_id,
       st.stu_name
from student st
         join (
    -- 仅仅为了扩展, 从而找到所需的所有原始数据
    select *
    from score s
             join
         (
             -- 教过的课
             select course_id
             from course c
                      join teacher t
                           on c.tea_id = t.tea_id
             where tea_name = '李体音') t1
         on s.course_id = t1.course_id) t2
              on st.stu_id = t2.stu_id
group by st.stu_id, st.stu_name

having count(st.stu_id) = 2;


-- 5.2.7 [课堂讲解]查询学过“李体音”老师所讲授的任意一门课程的学生的学号、姓名
select *
from student s
         join score s2 on s2.stu_id = s.stu_id
where course_id in (select course_id
                    from teacher t
                             join course c on c.tea_id = t.tea_id
                    where tea_name = '李体音')

-- 5.2.8 [课堂讲解]查询没学过"李体音"老师讲授的任一门课程的学生姓名
-- not in 是怎么回事，怎么查出来还有那么多重复的？
-- 测试环境中就是使用*比较好, 更容易理解里面的逻辑
-- 并且数据不需要弄全部数据, 拿出些样例便好. 只有样例时, 就不用考虑用 limit 限制了.

select *
from student s
         join score s2 on s2.stu_id = s.stu_id
where course_id not in (select course_id
                        from teacher t
                                 join course c on c.tea_id = t.tea_id
                        where tea_name = '李体音')

-- 5.2.9 [课堂讲解]查询至少有一门课与学号为“001”的学生所学课程相同的学生的学号和姓名
select *
from score s2
where s2.course_id
          in
      (select course_id
       from score s
       where stu_id = "001") t2
;



with test as (select course_id
              from score s
              where stu_id = "001")
;


-- 使用 with 子句

-- with 仅仅相当于一个函数, 每次执行都会运行

-- 使用 hive 实现各种集合的算法
-- 最关键的就是做差集, 即 A-B A  left join B where b.id is null
