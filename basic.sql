drop table business;
create table business
(
    name      string, -- 顾客
    orderdate string, -- 下单日期
    cost      int     -- 购买金额
) row format delimited fields terminated by ',';


load data local inpath "/opt/data/business.txt" into table business;


select
    *
from
    (business b)


-- 1. 购买过的总人次, 并保留所有信息 STAR
select
    name,
    orderdate,
    cost,
    count(*) over (rows between unbounded preceding and unbounded following)
from
    business
;
-- 2. 购买过的累加人次并保留所有信息
-- 我的
select
    name,
    orderdate,
    cost,
    row_number() over ()
from
    business
;

select
    name,
    orderdate,
    cost,
    count(*) over (rows between unbounded preceding and current row) cn
from
    business;


-- 3. 购买过的总人数
select
    name,
    count(*) over (rows between unbounded preceding and unbounded following)
from
    business
group by
    name
;


-- 4. 购买过的累加人数
select
    name,
    count(*) over (rows between unbounded preceding and current row )
from
    business
group by
    name
;
-- 2022年4月份购买过的顾客及总人数 STAR
select
    name,
    count(*) over (rows between unbounded preceding and unbounded following)
from
    (
        select
            name
        from
            (
                select name, month(orderdate) date_month
                from business b
            ) t1
        where
            date_month = 4
        group by name
    ) t2


select
    name,
    count(*) over ()
from
    (
        select name
        from business b
        where month(orderdate) = 4 -- where后的条件不一定要出现在select后面
    ) t1
group by
    name
;


-- 最简单写法
select
    name,
    count(*) over () cn
from
    business
where
    month(orderdate) = 4
group by
    name;



-- star business 实现如下
-- date      name_list        cn
-- 2022-01	["小元","小海"]	2
-- 2022-02	["小元"]	        1
-- 2022-04	["小元","小辉"]	2
-- 2022-05	["小猛"]	        1
-- 2022-06	    ["小猛"]	        1


--  6）查询顾客的购买明细及月购买总额
select
    *,
    sum(cost) over (partition by month(orderdate))
from
    (business b)
;


select
    name,
    orderdate,
    cost,
    sum(cost) over (partition by month(orderdate))
from
    business
--     7）查询每个顾客的购买明细及购买总额
select
    name,
    orderdate,
    cost,
    sum(cost) over (partition by name)
from
    (business b)

-- 8）查询每个顾客每个月的购买明细及购买总额
select
    name,
    month(orderdate),
    cost,
    sum(cost) over (partition by name,month(orderdate))
from
    (business b)

--     9）按照日期将cost累加并保留明细 star
select
    *,
    sum(cost) over (order by orderdate)
from
    (business b)
-- 10）按照日期将每个顾客cost累加并保留明细
select
    *,
    sum(cost) over (partition by name order by orderdate)
from
    (business b)
-- 11）求出每个顾客上一次和当前一次消费的和并保留明细
select
    *,
    sum(cost) over (partition by name order by orderdate rows between 1 preceding and current row )
from
    (business b)
-- 12）查询每个顾客购买明细以及上次的购买时间和下次购买时间
select
    *,
    lead(orderdate, 1) over (partition by name order by orderdate),
    lag(orderdate, 1) over (partition by name order by orderdate)
from
    (business b)

select
    name,
    orderdate,
    ntile(2) over (partition by name order by orderdate)
from
    (business b);
-- 13）查询顾客每个月第一次的购买时间 和 每个月的最后一次购买时间

-- [42000][40000] Error while compiling statement: FAILED: NullPointerException null
select
    *,
    first_value(orderdate) over (partition by name,month(orderdate) order by orderdate),
    last_value(orderdate)
               over (partition by name,month(orderdate) order by orderdate rows between unbounded preceding and unbounded following)
from
    (business b)

select
    name,
    orderdate,
    cost,
    first_value(orderdate)
                over (partition by name,month(orderdate) order by orderdate rows between UNBOUNDED PRECEDING and UNBOUNDED following) first_value,
    last_value(orderdate)
               over (partition by name,month(orderdate) order by orderdate rows between UNBOUNDED PRECEDING and UNBOUNDED following)  last_value
from
    business
-- 14）查询前20%时间的订单信息
select
    *
from
    (
        select *, ntile(5) over (partition by name order by orderdate) ntile_5
        from (business b)
    ) t1
where
    ntile_5 = 1
-- 15）按照花费的金额进行排名

select
    *,
    dense_rank() over (order by sum_cost desc),
    rank() over (order by sum_cost desc),
    row_number() over (order by sum_cost desc)
from
    (
        select *, sum(cost) over (partition by name) sum_cost
        from (business b)
    ) t1

-- 16）按照每个顾客花费的金额进行排名
select
    *,
    rank() over (order by cost desc),
    row_number() over (order by cost desc)
from
    (business b)

select
    name,
    orderdate,
    cost,
    rank() over (partition by name order by cost desc)       rk,
    dense_rank() over (partition by name order by cost desc) drk,
    row_number() over (partition by name order by cost desc) drk
from
    business
;
show functions;

desc function extended lag;
select 8 * 8;

select upper("jack");
select substr("jack", 3, 2);

select trim("  wo jack  ");


select nvl(null, 88);
select concat_ws("_", 'a', 'b', 'col int not null');
select get_json_object()
-- json
select
    get_json_object('[{"name":"大海海","sex":"男","age":"25"},
    {"name":"小宋宋","sex":"男","age":"47"}]', '$.[0]');


select from_utc_timestamp(1659946088, "")

select from_unixtime(1659946088);

select unix_timestamp('2022/08/08 08-08-08', 'yyyy/MM/dd HH-mm-ss')
select to_unix_timestamp('2022/08/08 08-08-08', 'yyyy/MM/dd HH-mm-ss')

select date_format('2022-08-08 08:08:08', 'yyyy年/MM月/dd日/hh时/mm分/ss秒');


create table employee
(
    name     string,         --姓名
    sex      string,         --性别
    birthday string,         --出生年月
    hiredate string,         --入职日期
    job      string,         --岗位
    salary   double,         --薪资
    bonus    double,         --奖金
    friends  array<string>,  --朋友
    children map<string,int> --孩子
);

insert into
    employee
values
    ('张无忌', '男', '1980/02/12', '2022/08/09', '销售', 3000, 12000, array('阿朱', '小昭'),
     map('张小无', 8, '张小忌', 9)),
    ('赵敏', '女', '1982/05/18', '2022/09/10', '行政', 9000, 2000, array('阿三', '阿四'), map('赵小敏', 8)),
    ('宋青书', '男', '1981/03/15', '2022/04/09', '研发', 18000, 1000, array('王五', '赵六'),
     map('宋小青', 7, '宋小书', 5)),
    ('周芷若', '女', '1981/03/17', '2022/04/10', '研发', 18000, 1000, array('王五', '赵六'),
     map('宋小青', 7, '宋小书', 5)),
    ('郭靖', '男', '1985/03/11', '2022/07/19', '销售', 2000, 13000, array('南帝', '北丐'), map('郭芙', 5, '郭襄', 4)),
    ('黄蓉', '女', '1982/12/13', '2022/06/11', '行政', 12000, null, array('东邪', '西毒'), map('郭芙', 5, '郭襄', 4)),
    ('杨过', '男', '1988/01/30', '2022/08/13', '前台', 5000, null, array('郭靖', '黄蓉'), map('杨小过', 2)),
    ('小龙女', '女', '1985/02/12', '2022/09/24', '前台', 6000, null, array('张三', '李四'), map('杨小过', 2))


select
    *
from
    (employee e)

-- 4）每个月的入职人数 star

-- 最复杂的一种
select
    month_hiredate,
    count(*)
from
    (
        select
            *,
            month(replace_hiredate) month_hiredate
        from
            (
                select *, replace(hiredate, '/', '-') replace_hiredate from (employee e)
            ) t1
    ) t2
group by
    month_hiredate

order by
    month_hiredate
;

-- 优化
select
    month(replace(hiredate, '/', '-')) month,
    count(*)                           cn
from
    (employee e)
group by
    month(replace(hiredate, '/', '-'))
;

--TODO 比较下上面两种写法的效率

select
    month(replace(hiredate, '/', '-')) as month,
    count(*)                           as cn
from
    employee
group by
    month(replace(hiredate, '/', '-'))
-- 每个人年龄（年 + 月）star

explain
select
    *
from
    (
        select
            name
        from
            (
                select name, sum(salary)
                from employee
                group by name
            ) t1
    ) t2

set hive.mapjoin.smalltable.filesize
-- 方法1：通过函数取出年和月
-- todo 下面的可以简化下，写的简单了，看看效率有提高吗
explain
select
    name,
    abs_year,
    abs_month,
    r_birthday,
    concat(age_year, "岁", age_month, "月") age
from
    (
        select
            name,
            abs_year,
            abs_month,
            r_birthday,
            `if`(abs_month < 0, abs_year - 1, abs_year)    age_year, --可以在这里concat，但只能用很长的式子，是就会显得乱
            `if`(abs_month < 0, 12 + abs_month, abs_month) age_month
        from
            (
                select
                    name,
                    r_birthday,
                    year(`current_date`()) - year(r_birthday)   abs_year,
                    month(`current_date`()) - month(r_birthday) abs_month
                from
                    (
                        select *, replace(birthday, '/', '-') r_birthday
                        from (employee e)
                    ) t1
            ) t12
    ) t3
;

-- 方法二：通过取整，取余算出. 由于闰年原因，直接除以365不准确，用if判断则更麻烦
select
    name,
    r_birthday,
    datediff(`current_date`(), r_birthday) days
from
    (
        select *, replace(birthday, '/', '-') r_birthday
        from (employee e)
    ) t1


select datediff('1999-9-8', '1999-7-8');


select month('1999-9-8')


-- 6）按照薪资，奖金的和进行倒序排序，如果奖金为null，置位0
select
    name,
    salary + nvl(bonus, 0) sum
from
    (employee e)
order by
    sum desc
-- 7）每个人有多少个朋友
select
    name,
    friends,
    size(friends)
from
    (employee e)
-- 8）每个人的孩子的姓名
select
    name,
    children,
    map_keys(children) children_name
from
    (employee e)


-- 9）每个岗位男女各多少人 star
-- 未必用开创函数就把问题弄简单了
select
    job,
    sex,
    count(*)
from
    (employee e)
group by
    job, sex
order by
    job, sex

-- 10）每个岗位男女各多少人,结果要求如下
-- 总结1列变2列的方法
select
    job,
    sum(if(sex = "男", 1, 0))   `男`,
    sum(`if`(sex = '女', 1, 0)) `女`
from
    (employee e)
group by
    job
-- 3.3.3 [课堂讲解]查询同姓（假设每个学生姓名的第一个字为姓）的学生名单并统计同姓人数大于2的姓
select
    first_name,
    count(*)
from
    (
        select *, substr(stu_name, 0, 1) first_name
        from (student s)
    ) t1
group by
    first_name
-- having count(*)>2
;

-- 3.4.2 [课堂讲解]按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
-- 学生id 语文 数学 英语 有效课程数 有效平均成绩

select
    stu_id,
    sum(`if`(course_name = "语文", course, 0)) `语文`,
    sum(`if`(course_name = "数学", course, 0)) `数学`,
    sum(`if`(course_name = "数学", course, 0)) `数学`,
    sum(`if`(course_name = "体育", course, 0)) `体育`,
    sum(`if`(course_name = "音乐", course, 0)) `音乐`,
    count(*)                                   `有效课程数`,
    sum(course)                                `总成绩`,
    sum(course) / count(*)                     `平均成绩`
from
    score           s
        join course c on c.course_id = s.course_id
group by
    stu_id


select
    *
from
    score           s
        join course c on c.course_id = s.course_id


-- 4.1.1 [课堂讲解]查询所有课程成绩均小于60分的学生的学号、姓名
-- todo 比对下，先查询出来再join效率最高
select
    s.stu_id,
    stu_name
from
    student    s
        join (
                 select stu_id, sum(if(course < 60, 0, 1)) sum_under_60
                 from score s
                 group by stu_id
                 having sum_under_60 < 1
             ) t1 on s.stu_id = t1.stu_id
-- 5.1.1 [课堂讲解]查询有两门以上的课程不及格的同学的学号及其平均成绩
select
    stu_id,
    sum(if(course < 60, 1, 0)) sum_under_60,
    avg(course)
from
    score s
group by
    stu_id
having
    sum_under_60 > 2
-- 5.2.6 [课堂讲解]查询学过“李体音”老师所教的所有课的同学的学号、姓名
-- 需要完全包含 todo 用all in吗？

-- 李老师教过的课
select
    course_id
from
    teacher         t
        join course c on c.tea_id = t.tea_id
where
    tea_name = '李体音'


-- 5.2.7 [课堂讲解]查询学过“李体音”老师所讲授的任意一门课程的学生的学号、姓名
select
    s.stu_id,stu_name
from
    student        s
        join score s2 on s2.stu_id = s.stu_id
where
        course_id in (
        select
            course_id
        from
            teacher         t
                join course c on c.tea_id = t.tea_id
        where
            tea_name = '李体音'
                     )

-- 5.2.8 [课堂讲解]查询没学过"李体音"老师讲授的任一门课程的学生姓名
-- todo not in 是怎么回事，怎么查出来还有那么多重复的？
select
    s.stu_id,
    stu_name
from
    student        s
        join score s2 on s2.stu_id = s.stu_id
where
        course_id not in (
        select
            course_id
        from
            teacher         t
                join course c on c.tea_id = t.tea_id
        where
            tea_name = '李体音'
                     )

-- 5.2.9 [课堂讲解]查询至少有一门课与学号为“001”的学生所学课程相同的学生的学号和姓名 todo
