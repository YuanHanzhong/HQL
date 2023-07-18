// 2.1.2 查询姓“王”老师的个数


// 2.1.3 检索课程编号为“04”且分数小于60的学生学号，结果按分数降序排列

/*
 2022/10/6 15:29  做题步骤
 要哪些字段 --> 需要哪些表

 先筛选, 再关联, 效率高

*/

-- 2022/10/19 9:32 2.1.4 查询数学成绩不及格的学生和其对应的成绩，按照学号升序排序
-- 2022/10/8 10:12 NOTE 先写最里层, 一点一点往外包
-- 2022/10/8 10:19 NOTE join时两个表都要起别名, 子查询在前在后都行
-- 2022/10/8 10:21 NOTE on就是条件, 恒成立跟不写一样
-- 2022/10/8 10:35 NOTE 子查询, 括号单独占一行
-- 2022/10/18 19:47 NOTE 子查询可以用join替代, 但是效率会降低很多

select s.stu_id, s2.stu_name, course
from score s
         join course c on c.course_id = s.course_id
         join student s2 on s2.stu_id = s.stu_id
where course < 60
  and c.course_name = '数学'
order by s.stu_id
;


-- 方法1: 使用join
select s2.stu_id, s2.stu_name, s.course
from course c
         join score s on s.course_id = c.course_id
         join student s2 on s2.stu_id = s.stu_id
where course_name = '数学'
  and s.course < 60
order by s2.stu_id;

desc course;
select course_name
from course c;

-- 方法2: 使用子查询
select *
from student t1
         join
     (select stu_id, course
      from score
      where course_id in
            (select course_id
             from course
             where course_name = '数学')
        and course < 60) t2
     on t1.stu_id = t2.stu_id
order by t2.course desc;


-- 2022/10/7 10:01  2.2.1 查询各学生的年龄（精确到月份）
SELECT birthday
FROM student;


select stu_name,
       year(`current_date`()) - year(birthday)   year,
       month(`current_date`()) - month(birthday) month
from student;

-- 2022/10/7 10:16 获取各种时间
select `current_date`();
select `current_timestamp`();
select unix_timestamp();

select to_date('2022-10-11 11:10:23');

select year('2022-10-11 11:10:23');
select month(`current_date`());


select day('2022-10-11 11:10:23');
select hour('2022-10-11 11:10:23');
select minute('2022-10-11 11:10:23');
select second('2022-10-11 11:10:23');
select dayofmonth('2022-10-11 11:10:23');
select `dayofweek`('2022-10-11 11:10:23');
select weekofyear('2022-10-11 11:10:23');

-- 2022/10/7 10:26 取今天的0时0分0秒
select floor_day(`current_timestamp`());
-- 2022/10/7 10:30 NOTE 活了多少个月了
select months_between(`current_date`(), '1994-01-09');
select months_between(`current_timestamp`(), '1994-01-09');
-- 2022/10/7 10:35 活了多少天
select datediff(`current_timestamp`(), '1994-01-09');

select date_add(`current_date`(), 9);
select date_add(`current_timestamp`(), 9);

select last_day(`current_timestamp`());

-- 2022/10/7 10:45 随意转换日期格式
select date_format('2022-10-11 14:10:23', 'yyyy/MM-dd HH:mm:ss');


drop table if exists order_summary;
create table if not exists order_summary
(
    id          string,
    category_id string,
    sum         int,
    test_case   int
)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_summary';

insert into order_summary
values ('1', '1', 12, 1),
       ('2', '2', 32, 1),
       ('3', '3', 44, 1),
       ('4', '2', 56, 1),
       ('1', '3', 78, 2),
       ('1', '1', 12, 3),
       ('2', '2', 15, 3),
       ('3', '3', 17, 3),
       ('4', '3', 17, 3),
       ('5', '3', 19, 3),
       ('1', '1', 33, 4),
       ('2', '1', 55, 4),
       ('3', '1', 77, 4),
       ('4', '2', 23, 4);

-- 2022/10/7 11:20 查询商品销售汇总表中销量第二的商品，
-- 如果不存在返回null，如果存在多个排名第二的商品则需要全部返回。
select *
from (select *
      from (select id
                 , DENSE_RANK() over (order by `sum` desc) rk
            from order_summary) t1
         --          where rk = 2 -- 过滤出销量第二的商品

     ) t2
;



DROP TABLE IF EXISTS `user_active`;
CREATE TABLE user_active
(
    `user_id`     varchar(33) COMMENT '用户id',
    `active_date` date COMMENT '用户登录日期时间',
    `test_case`   int
) COMMENT '用户活跃表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_active';

INSERT INTO user_active
VALUES ('1', '2022-04-25', NULL),
       ('1', '2022-04-26', NULL),
       ('1', '2022-04-27', NULL),
       ('1', '2022-04-28', NULL),
       ('1', '2022-04-29', NULL),
       ('2', '2022-04-12', NULL),
       ('2', '2022-04-13', NULL),
       ('3', '2022-04-05', NULL),
       ('3', '2022-04-06', NULL),
       ('3', '2022-04-08', NULL),
       ('4', '2022-04-04', NULL),
       ('4', '2022-04-05', NULL),
       ('4', '2022-04-06', NULL),
       ('4', '2022-04-06', NULL),
       ('4', '2022-04-06', NULL),
       ('5', '2022-04-12', NULL),
       ('5', '2022-04-08', NULL),
       ('5', '2022-04-26', NULL),
       ('5', '2022-04-26', NULL),
       ('5', '2022-04-27', NULL),
       ('5', '2022-04-28', NULL);

select user_id
     , flag
     , count(1)
from (select user_id
           , active_date
           , row_number() over (partition by user_id order by active_date)                        rk
           , date_sub(active_date, row_number() over (partition by user_id order by active_date)) flag
      from (select user_id
                 , active_date
            from user_active
            group by user_id, active_date) t1 -- 同一天可能多个用户登录 进行去重
     ) t2 -- 计算一段数字是否连续：用这个数字减去它的排名，会得到一个相同的数
group by user_id, flag
having count(flag) >= 3 -- 连续登录大于等于三天
;


/*
STAR 2.2.1 查询各学生的年龄（精确到月份)
2023年2月13日18:37:49 实现

*/

select stu_id,
       stu_name,
       birthday,
       if(month(current_timestamp) - month(birthday) < 0,
          year(current_timestamp) - year(birthday) - 1,
          year(current_timestamp) - year(birthday)) `age`
from student s
;


-- 如何做标记

-- 通过修验模型掌握一个个点，


-- 2022/10/21 7:58 NOTE 日期先减再转化会报错, 要想转化, 再相减
-- 2022/10/21 8:09 包一层, 可读性会高很多. 就像是函数一样. 都写在一层会很乱

select stu_id, concat(`if`(yue > 0, sui, sui - 1), '岁零', `if`(yue > 0, yue, 12 + yue), '个月')
from (select stu_id,
             birthday,
             year(`current_date`()) - year(birthday)   `sui`,
             month(`current_date`()) - month(birthday) `yue`
      from student) t;



select stu_id, stu_name, concat(`if`(yue > 0, sui, sui - 1), '岁零', `if`(yue < 0, 12 + yue, yue), '个月')
from (select stu_id,
             stu_name,
             birthday,
             year(`current_date`()) - year(birthday)   sui
              ,
             month(`current_date`()) - month(birthday) yue
      from student s) t;



select stu_id,
       stu_name,
       birthday,
       year(`current_date`()) - year(birthday)   `sui`,
       month(`current_date`()) - month(birthday) `yue`

from student s;


select stu_id, stu_name, birthday, concat(`if`(yue < 0, sui - 1, sui), '岁零', `if`(yue < 0, yue + 12, yue), '月')
from (select stu_id,
             stu_name,
             birthday,
             year(`current_date`()) - year(birthday)   `sui`,
             month(`current_date`()) - month(birthday) `yue`

      from student) t



select stu_id
     , stu_name
     , ceil(months_between(`current_date`(), birthday) / 12)
     , concat(
        `floor`(months_between(`current_date`(), birthday) / 12), '岁'
    ,, '月') `年龄`
from student s
;

select `floor`(12.1);

select ceil(12.1);

-- 2022/10/19 10:50 NOTE if的使用

select `if`(1 = 1, 1, 2)



select `if`(1 > 0, '>', '<');
select `if`(1 < 0, '>', '<');

-- 2022/10/19 10:51  concat的使用
-- 2022/10/19 10:52 使用 , 而非 +
select concat('你好', '1', '2', '3')
select concat_ws('分隔符', '1', '2', '3')


-- 2022/10/19 10:42 NOTE 内容的话用concat, 分别取出年和月(直接取出就行, 不用自己除), if()
select stu_id
     , stu_name
     , birthday
     , concat(
        if
            (
                        month(`current_date`()) - month(birthday) >= 0
            , year(`current_date`()) - year(birthday)
            , year(`current_date`()) - year(birthday) - 1
            )
    , '岁'
    , if
            (
                        month(`current_date`()) - month(birthday) >= 0
            , month(`current_date`()) - month(birthday)
            , 12 + month(`current_date`()) - month(birthday)
            )
    , '个月'
    ) `年龄`
from student;


select stu_name
     , birthday
     , `if`(month(`current_date`()) - month(birthday) >= 0, year(`current_date`()) - year(birthday),
            year(`current_date`()) - year(birthday) - 1)    age_year
     , `if`(month(`current_date`()) - month(birthday) >= 0, month(`current_date`()) - month(birthday),
            month(`current_date`()) - month(birthday) + 12) age_month


from student s;

-- 2022/10/19 11:07 NOTE concat比较复杂的时候, 就弄个中间表

-- 2022/10/19 10:08 haoti 取小数


-- 2022/10/8 11:37 3.1.1 查询课程编号为“02”的总成绩
select course_id, sum(course)
from score
where (
          course_id = '02'
          )
group by course_id;

-- 2022/10/8 11:41  3.1.2 查询参加考试的学生个数


select stu_id
from score s
where course is not null
group by stu_id
;

select count(*)
from (select stu_id
      from score s
      where course is not null
      group by stu_id) t



select count(1)
from (select stu_id, count(*)
      from score s
      group by stu_id
               -- 2022/10/19 14:02 NOTE 至少参加1门考试
      having count(*) > 1) t;


-- 2022/10/8 11:44 NOTE count时, 注意1非空, 2重复
select count(distinct stu_id) `参加考试的学生个数`
from score
where course is not null;


-- 2022/10/8 12:34 3.2.1 查询各科成绩最高和最低的分，以如下的形式显示：课程号，最高分，最低分
select course_id, min(course) min, max(course) max
from score
group by course_id
;


select course_id, count(*)
from score
where course is not null
group by course_id;

select sex, count(*)
from student
group by sex;

-- 2022/10/8  12:43 3.3.1 查询平均成绩大于60分学生的学号和平均成绩
select stu_id, avg(course)
from score s
group by stu_id
having avg(course) > 60
;


select stu_id, avg(course) avg
from score
group by stu_id
having avg > 60;

select *
from (select stu_id, avg(course) as avg_score
      from score
      group by stu_id) t1
where t1.avg_score > 60;

-- 2022/10/8 12:49  3.3.2 查询至少选修两门课程的学生学号

select stu_id, count(*)
from score s
group by stu_id
having count(*) > 3


select stu_id, count(distinct course_id) num
from score
group by stu_id
having num >= 2
;

-- 2022/10/8 12:56 3.3.3 查询同姓（假设每个学生姓名的第一个字为姓）的学生名单并统计同名人数
select *
from student
group by
;

select firstname, count(*) `人数`
from (select substr(stu_name, 1, 1) firstname
      from student) t1
group by t1.firstname
;



select substr('123', 1, 2)
select substring('1234', 2, 2)


-- 2022/10/8 13:04 3.3.4 查询每门课程的平均成绩，结果按平均成绩升序排序，平均成绩相同时，按课程号降序排列


select course_id, avg(course) avg
from score
group by course_id
order by avg(course)
;

select course_id, avg
from (select course_id, avg(course) avg
      from score
      group by course_id) t1
order by avg desc, course_id desc
;

select course_id, avg(course)
from score
group by course_id
order by avg(course) asc, course_id desc;
-- 2022/10/8 14:06  执行顺序
-- FROM —> WHERE —>GROUP BY—> 聚合函数 —>HAVING—>SELECT —>ORDER BY —>LIMIT

-- 2022/10/8 14:08 3.3.5 统计参加考试人数大于等于15的学科
select course_id, count(*) sum
from score
group by course_id
having sum >= 15
;

-- 3.4.1 查询学生的总成绩并进行排名
select stu_id, sum(course) sum
from score
group by stu_id
order by sum desc
;

-- 3.4.2 查询平均成绩大于60分的学生的学号和平均成绩
select stu_id, avg(course) avg
from score
group by stu_id
having avg > 60
;


-- STAR 3.4.3 按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
-- stu_id, 语文, 数学, 英语, 有效课程数, 平均成绩

select stu_id,
       sum(if(course_name = "语文", course, 0)) `yuwen`,
       sum(if(course_name = "数学", course, 0)) `shuxue`,
       sum(if(course_name = "英语", course, 0)) `yingyu`,
       sum(if(course_name = "体育", course, 0)) `tiyu`,
       sum(if(course_name = "音乐", course, 0)) `yinyue`,
       avg(course)                              `avg_score`,
       sum(if(course = 0, 1, 1))                `有效课程数` --P2 应该有个更优美的写法

from score s
         join course c on c.course_id = s.course_id
group by stu_id

select *
from course c;

-- 2022/10/19 15:02 NOTE 这里有点bug, 只能人工写以下课程序号, 不能直接用='语文'来筛选
select stu_id                                                                                          as `学生ID`
     , (select nvl(course, 0) as course from score where score.stu_id = t.stu_id and course_id = '01') as `语文`
     , (select nvl(course, 0) as course from score where score.stu_id = t.stu_id and course_id = '02') as `数学`
     , (select nvl(course, 0) as course from score where score.stu_id = t.stu_id and course_id = '03') as `英语`
     , count(*)                                                                                        as `有效课程数` -- sum(if(course=0,1,0))这样用更好
     , avg(course)                                                                                     as `平均成绩`   -- avg可以直接用，
FROM score as t
group by stu_id
order by avg(course);



select stu_id, course `数学`
from score s
         join course c on c.course_id = s.course_id
where course_name = "数学"



-- 2022/10/21 8:17 NOTE 子查询一定要起别名

-- 2022/10/21 8:22 NOTE 难点在于在哪里筛选语文
-- 2022/11/1 15:42 NOTE 扩展列的个数用join, 扩展行的个数用union
-- 2022/10/21 8:40 有没有数据关键在于子查询连接一下why


-- 2022/11/2 19:02 把1列变成多列
-- 2022/11/2 19:03 下面用if, 转化的最巧妙
select s.stu_id,
       s.stu_name,
       sum(if(c.course_name = '数学', course, 0)) as `数学`,
       sum(if(c.course_name = '语文', course, 0))    `语文`
from student s
         join score s2 on s2.stu_id = s.stu_id
         join course c on c.course_id = s2.course_id
group by s.stu_id, s.stu_name
;



select tt1.stu_id, tt1.stu_name, tt1.`数学`, tt2.`语文`
from (
         (select t.stu_id, stu_name, course `数学`
          from (select *
                from student s
                         join score s2 on s2.stu_id = s.stu_id
                         join course c on c.course_id = s2.course_id) t
          where course_name = '数学') tt1
             join
             (select t.stu_id, stu_name, course `语文`
              from (select *
                    from student s
                             join score s2 on s2.stu_id = s.stu_id
                             join course c on c.course_id = s2.course_id) t
              where course_name = '语文') tt2
         on tt1.stu_id = tt2.stu_id
         )
;



select stu_id, (select course from score s2 where course_id = '01' and t.stu_id = t.stu_id) tt
from (select stu_id, course_id
      from score) t;

select stu_id                                                                                          as `学生ID`
     , (select course
        from score
                 join course c on c.course_id = score.course_id
        where score.stu_id = t.stu_id
          and course_name = '语文')                                                                    as `语文`
     , (select nvl(course, 0) as course from score where score.stu_id = t.stu_id and course_id = '02') as `数学`
     , (select nvl(course, 0) as course from score where score.stu_id = t.stu_id and course_id = '03') as `英语`
     , sum(if(course = 0, 1, 0))                                                                       as `有效课程数`
     , avg(t.course)                                                                                   as `平均成绩`
FROM score as t
group by stu_id
order by avg(t.course);



select stu_id, course
from score s
where stu_id = '017'

-- 2022/10/8 14:27 NOTE 为null时不计行数, 不为null则计数为1
select nvl(null, 2)
-- count(*)

select course_id
from course
where course_name = '数学'

select stu_id as                                        `学号`
     , (select nvl(course, 0) as course
        from score
        where score.stu_id = t1.stu_id
          and course_id =
              (select course_id
               from course
               where course_name = '语文'))             `语文`
     , (select nvl(course, 0) as course
        from score
        where score.stu_id = t1.stu_id
          and course_id = (select course_id
                           from course
                           where course_name = '数学')) `数学`
     , count(*)                                         `有效课程数`
     --     , avg(t1.course) `平均成绩`

from score t1
group by stu_id
order by stu_id, avg(t1.course)


-- 2022/10/8 15:19 NOTE `` 和 '' "" 的使用规则
--     别名用``, 字符串用'', ""一般不用

-- 2022/10/8 15:28 NOTE 多个表有重名时, 要指定某个列, 否则容易出错
-- 2022/10/19 9:44 NOTE 起别名很重要
select *
from score
         join student s on score.stu_id = s.stu_id
         join course c on score.course_id = c.course_id
     -- where score.stu_id='001'
order by score.stu_id
;


-- 2022/10/8 15:30 3.4.4 查询一共参加两门课程且其中一门为语文课程的学生的id和姓名
/*
 2022/10/9 11:27
 条件1: 语文
 条件2: 总共2门, count, having
 姓名: 需要student表
*/

-- 选了语文课的学生, 所有的的课程都拉出来
select stu_id, course_id
from score
where stu_id in
      (
          -- 保证有语文课
          select stu_id
          from score
          where course_id =
                (select course_id
                 from course
                 where course_name = '语文'))
;


select stu_id, count(*) c
from score
where stu_id in
      (
          -- 保证有语文课
          select stu_id
          from score
                   join course c2 on score.course_id = c2.course_id
          where course_name = '语文'
          -- 2022/10/9 12:09 NOTE 嵌套子查询报错不支持的时候, 考虑先join, 需要哪些数据, 哪些表, 先join下
          --                          (
          --                              select course_id
          --                              from course
          --                              where course_name = '语文'
          --                          )
      )
group by stu_id
having c = 3
;

select stu_id, count(*) c
from score
where stu_id in
      (
          -- 保证有语文课
          select stu_id
          from score
                   join course c2 on score.course_id = c2.course_id
          where course_name = '语文')
group by stu_id
having c = 5;


select t1.stu_id, count(course_id) as c
from (
         -- 选了语文的学生拉出来
         select stu_id, course_id
         from score
         where stu_id in
               (
                   -- 选了语文的学生
                   select stu_id
                   from score
                   where course_id = '01')) as t1
group by t1.stu_id
having c = 3;
;

--  4.1.1 查询所有课程成绩小于60分学生的学号、姓名

select *
from score s
where stu_id = '008'

select s2.stu_id, stu_name, course_id, course
from score
         join student s2 on s2.stu_id = score.stu_id

where s2.stu_id not in
      (select stu_id
       from score s
       where course >= 60)
order by s2.stu_id
;

select stu_id
from score s
where course >= 60
;
;

select stu_id, stu_name
from student
where stu_id not in
      (select stu_id
       from score
       where course > 60)
;

-- 4.1.2 查询没有学全所有课的学生的学号、姓名
-- 2022/10/19 9:46 select是最终想要的结果, 也是最后执行的
-- 2022/10/19 9:51 STAR 执行顺序是什么样的
-- 2022/10/19 9:46 NOTE 顺序: from --> where --> group by --> having  --> select --> order by --> limit
select student.stu_id, student.stu_name, count(course_id) c
from student
         join score s on student.stu_id = s.stu_id
group by student.stu_id, student.stu_name
having
    -- 2022/10/9 14:29 NOTE  不能使用别名, 因为having在select之前执行, 直接使用函数
    --  Only SubQuery expressions that are top level conjuncts are allowed
    --  c  <
    count(course_id) <
    (select count(course_id)
     from course)
;

select student.stu_id,
       student.stu_name
from student
         inner join
     score
     on
         student.stu_id = score.stu_id
group by student.stu_id, student.stu_name
having count(course_id) < (select count(course_id)
                           from course);


-- 4.1.3 查询出只选修了两门课程的全部学生的学号和姓名
select s.stu_id, s.stu_name
from score
         join student s on score.stu_id = s.stu_id
group by s.stu_id, s.stu_name
having count(*) = 2
;

-- 4.1.4 查找1990年出生的学生名单
select stu_name
from student
where year(birthday) = 1990
;


select *
from score s
order by stu_id;

-- 5.1.1 STAR 查询两门以上不及格课程的同学的学号及其平均成绩
select stu_id,count(course)
from score
where score.course<60
group by stu_id
having count(*) >2


-- 2022/10/20 17:59 这个题目很巧妙, 值得再做
-- 2022/11/1 15:59 一次做对了, 并且还用了优化
select stu_id, avg(course)
from score s2
         left semi
         join
     (select stu_id
      from score s
      where course < 60
      group by stu_id
      having count(*) > 2) t
     on s2.stu_id = t.stu_id
group by stu_id
;


select stu_id
from score s
where course < 60
group by stu_id
having count(*) > 2
;



select stu_id, avg(course) `不及格课程的平均成绩`, count(course) `不及格课程数`
from score s
where course < 60
group by stu_id
having count(course) > 2
order by stu_id
;



select stu_id, avg(course), count(course) `不及格课程数`
from score
where stu_id in
      (
          -- 2022/10/9 14:46 只有下面这个查询的时候, 这个只是算的不及格的同学的平均成绩 NOTE 这个是错的
          select stu_id
          from score
          where score.course < 60
          group by stu_id
          having count(course) > 2)
group by stu_id
;


-- 5.1.2 查询所有学生的学号、姓名、选课数、总成绩
select s.stu_id `学号`, stu_name `姓名`, count(course) `选课数`, sum(course) `总成绩`
from student
         join score s on student.stu_id = s.stu_id
group by s.stu_id, stu_name
order by s.stu_id
;
-- 5.1.3 查询平均成绩大于85的所有学生的学号、姓名和平均成绩
select score.stu_id, stu_name, avg(course)
from score
         join student s on score.stu_id = s.stu_id
group by score.stu_id, stu_name
having avg(course) > 85
;

select distinct stu_id,
                stu_name,
                ag
from (select stu.stu_id,
             stu.stu_name,
             avg(course) over (partition by stu.stu_id) ag
      from score sc
               left join student stu
                         on sc.stu_id = stu.stu_id) t1
where ag > 85;

-- 5.1.4 查询学生的选课情况：学号，姓名，课程号，课程名称
select s.stu_id, stu_name, s.course_id, course_name
from course
         join score s on course.course_id = s.course_id
         join student s2 on s.stu_id = s2.stu_id
;



-- 5.1.5 STAR 查询出每门课程的及格人数和不及格人数
-- 2022/10/20 18:08 NOTE 扩展列就用join
-- 2022/10/20 18:11 NOTE join 要有on, 没有on就是耍流氓


select course_name, t1.`不及格人数`, t2.`及格人数`
from (select course_id, count(*) `不及格人数`
      from score s
      where course < 60
      group by course_id) t1
         join
     (select course_id, count(*) `及格人数`
      from score s
      where course > 60
      group by course_id) t2
     on t1.course_id = t2.course_id
         join course on t1.course_id = course.course_id
order by t1.course_id
;



select tmp1.course_id,
       tmp1.course_name,
       tmp1.stu_num,
       tmp2.u_stu_num
from (select t1.course_id,
             co.course_name,
             t1.stu_num
      from (select count(stu_id) stu_num,
                   course_id
            from score
            where course >= 60
            group by course_id) t1
               left join course co
                         on t1.course_id = co.course_id) tmp1
         left join
     (select t1.course_id,
             co.course_name,
             t1.u_stu_num
      from (select count(stu_id) u_stu_num,
                   course_id
            from score
            where course < 60
            group by course_id) t1
               left join course co
                         on t1.course_id = co.course_id) tmp2
     on tmp1.course_id = tmp2.course_id
order by course_id
;


-- 2022/10/20 18:26 STAR 5.1.6 使用分段[100-85],[85-70],[70-60],[<60]来统计各科成绩，分别统计：各分数段人数，课程号和课程名称
-- 2022/10/20 18:38 NOTE 列转行就是where+union


(select c.course_id, c.course_name, count(*)
 from score s
          join course c on c.course_id = s.course_id
 where course <= 100
   and course > 85
 group by c.course_id, c.course_name
 order by c.course_id)

union
(select c.course_id, c.course_name, count(*)
 from score s
          join course c on c.course_id = s.course_id
 where course <= 100
   and course > 85
 group by c.course_id, c.course_name
 order by c.course_id)
;


select course_name,
       sec,
       stu_num,
       course_id

from (select '(85,100]' sec,
             t1.stu_num,
             t1.course_id,
             co.course_name
      from (select count(stu_id) stu_num,
                   course_id
            from score
            where course > 85
              and course <= 100
            group by course_id) t1
               left join course co
                         on t1.course_id = co.course_id
      union
      select '(70,85]' sec,
             t1.stu_num,
             t1.course_id,
             co.course_name
      from (select count(stu_id) stu_num,
                   course_id
            from score
            where course > 70
              and course <= 85
            group by course_id) t1
               left join course co
                         on t1.course_id = co.course_id
      union
      select '(60,70]' sec,
             t1.stu_num,
             t1.course_id,
             co.course_name
      from (select count(stu_id) stu_num,
                   course_id
            from score
            where course > 60
              and course <= 70
            group by course_id) t1
               left join course co
                         on t1.course_id = co.course_id
      union
      select '(0,60]' sec,
             t1.stu_num,
             t1.course_id,
             co.course_name
      from (select count(stu_id) stu_num,
                   course_id
            from score
            where course <= 60
            group by course_id) t1
               left join course co
                         on t1.course_id = co.course_id) tmp
order by course_id, sec
;

-- 2022/11/1 19:46 STAR 把join转化为union
-- 2022/11/2 19:00 NOTE 用flag还是low了, 用exists 最高级
-- 2022/11/1 19:49 1. flag, union, group, sum, where
select *, 1 flag
from student s;

-- 5.1.7 查询课程编号为03且课程成绩在80分以上的学生的学号和姓名
select *
from score s
         join student s2 on s2.stu_id = s.stu_id
where course_id = '03'
  and course > 80

--  P1 行转列的问题, 一定要用max? max起什么作用, 什么时候用? 有没有其他方法?
--   STAR  5.1.8 （重要！行转列）使用sql实现将该表行转列为下面的表结构
-- 如果没有该课程成绩用0代替。
-- 学号 课程01 课程02 课程03 课程04

select *
from score s;

select stu_id
from score s
group by stu_id;

select stu_id,
       min(if(course_id = '01', course, 0)) `01`,
       min(if(course_id = '02', course, 0)) `02`,
       min(if(course_id = '03', course, 0)) `03`,
       max(if(course_id = '04', course, 0)) `04`
from score
group by stu_id;

select min(2);

-- 5.2.1 检索"01"课程分数小于60，按分数降序排列的学生信息
-- 2022/11/1 20:26 P1 使用sort by没有效果
select s.stu_id, stu_name, course
from score s
         join student s2 on s2.stu_id = s.stu_id
where course_id = '01'
  and course < 60
    distribute by course
    sort by course desc
;


select st.*, sc.course
from student st
         left join score sc
                   on sc.stu_id = st.stu_id
where sc.course_id = '01'
  AND sc.course < 60
order by sc.course desc;

-- 2022/11/1 20:28 STAR 5.2.2 查询任何一门课程成绩在70分以上的学生的姓名、课程名称和分数


-- 2022/11/1 20:40 5.2.3 查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
select s.stu_id
from score s
         join student s2 on s2.stu_id = s.stu_id
where course < 60
group by s.stu_id
having count(course) > 2
;

select s3.stu_id, s3.stu_name, avg(course)
from student s3
         join score s4 on s4.stu_id = s3.stu_id
where s3.stu_id in
      (select s.stu_id
       from score s
                join student s2 on s2.stu_id = s.stu_id
       where course < 60
       group by s.stu_id
       having count(course) >= 2)
group by s3.stu_id, s3.stu_name
;


select s3.stu_id, s3.stu_name, course
from student s3
         join score s4 on s4.stu_id = s3.stu_id
where s3.stu_id in
      (select s.stu_id
       from score s
                join student s2 on s2.stu_id = s.stu_id
       where course < 60
       group by s.stu_id
       having count(course) >= 2)

select stu_id,
       stu_name,
       avgcourse
from (select stu_id,
             stu_name,
             count(1)    num,
             avg(course) avgcourse

      from (select s.stu_id,
                   stu_name,
                   course
            from student
                     join score s on student.stu_id = s.stu_id
            where course < 60
            group by s.stu_id, stu_name, course) t1
      group by stu_id, stu_name) t2
where num >= 2;


select stu_id, course
from score s
where stu_id = '015'
;


-- 5.2.4 查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
select *
from score s
         join score s2 on s.stu_id <> s2.stu_id and s.course = s2.course


--     5.2.5 查询课程编号为“01”的课程比“02”的课程成绩高的所有学生的学号
-- 2022/11/2 8:31 my
select *
from score s
         join score s2 on s.stu_id = s2.stu_id
where s.course_id = '01'
  and s2.course_id = '02'
  and s.course < s2.course


-- 2022/11/2 8:31 daan
select stu_id
from (select s1.stu_id,
             s1.stu_name,
             s1.course 01course,
             s2.course 02course
      from (select s.stu_id,
                   stu_name,
                   c.course_id,
                   course
            from student
                     join score s on student.stu_id = s.stu_id
                     join course c on s.course_id = c.course_id
            where c.course_id = '02') s1
               join (select s.stu_id,
                            stu_name,
                            c.course_id,
                            course
                     from student
                              join score s on student.stu_id = s.stu_id
                              join course c on s.course_id = c.course_id
                     where c.course_id = "01") s2 on s1.stu_id = s2.stu_id) s3
where 01course > 02course;


-- 5.2.6 STAR 查询学过编号为“01”的课程并且也学过编号为“02”的课程的学生的学号、姓名
select *
from score s
         join student s2 on s2.stu_id = s.stu_id
where s.course_id = '01'

-- 2022/11/2 8:39 交集

select *
from score s
         join student s2 on s2.stu_id = s.stu_id
where s.course_id = '02'


-- 2022/11/2 8:40 exists 关键字怎么用
select student.stu_id,
       student.stu_name
from student,
     score
where student.stu_id = score.stu_id
  and score.course_id = '01'
  and exists
    (select * from score as sc_2 where sc_2.stu_id = score.stu_id and sc_2.course_id = '02');


-- 5.2.7 STAR 查询学过“李体音”老师所教的所有课的同学的学号、姓名
select distinct s.course_id
from score s
         join teacher t
where t.tea_name = '李体音'


select *
from score s
         join student s2 on s2.stu_id = s.stu_id
where s.course_id


select st.stu_id,
       st.stu_name
from student st
         join (select stu_id
               from score s
                        join (select course_id
                              from course c
                                       join teacher t
                                            on c.tea_id = t.tea_id
                              where tea_name = '李体音') t1
                             on s.course_id = t1.course_id) t2
              on st.stu_id = t2.stu_id
group by st.stu_id, st.stu_name
having count(st.stu_id) = 2;


-- 5.2.8 查询学过“李体音”老师所讲授的任意一门课程的学生的学号、姓名

select distinct s.course_id
from score s
         join teacher t
where t.tea_name = '李体音'

select distinct s2.stu_id, s2.stu_name
from score s
         join student s2 on s2.stu_id = s.stu_id
where s.course_id in
      (select distinct s.course_id
       from score s
                join teacher t
       where t.tea_name = '李体音')
order by stu_id

-- 5.2.9 STAR 查询没学过"李体音"老师讲授的任一门课程的学生姓名
-- 2022/11/2 8:52 mine, 错的, 查出来为null

select distinct s2.stu_id, s2.stu_name
from score s
         join student s2 on s2.stu_id = s.stu_id
where s.course_id not in
      (select distinct s.course_id
       from score s
                join teacher t
       where t.tea_name = '李体音')
order by stu_id

select stu_id,
       stu_name
from student
where stu_id not in (select distinct (stu_id)
                     from score sc,
                          course c,
                          teacher t
                     where sc.course_id = c.course_id
                       and t.tea_id = c.tea_id
                       and t.tea_name = '李体音');

-- 5.2.10 查询选修“李体音”老师所授课程的学生中成绩最高的学生姓名及其成绩（与上题类似,用成绩排名，用 limit 1得出最高一个）

select st.stu_name,
       t2.course
from student st
         join (select stu_id,
                      course
               from score s
                        join (select course_id
                              from course c
                                       join teacher t
                                            on c.tea_id = t.tea_id
                              where tea_name = '李体音') t1
                             on s.course_id = t1.course_id) t2
              on st.stu_id = t2.stu_id
order by t2.course desc
limit 1;


-- 5.2.11 查询至少有一门课与学号为“001”的学生所学课程相同的学生的学号和姓名

select *
from score s
order by s.stu_id

select distinct course_id
from score s
where stu_id = '001'

-- 2022/11/2 9:08 我做的, 分析着对
select distinct s2.stu_id, s2.stu_name
from score s
         join student s2 on s2.stu_id = s.stu_id
where s.course_id in (select distinct course_id
                      from score s
                      where stu_id = '001')
order by s2.stu_id

-- 2022/11/2 9:08 下面的答案是错的
select distinct s3.stu_id,
                s3.stu_name
from student s3
         join score s4
              on s3.stu_id = s4.stu_id
where s3.stu_id != '001'
  and s4.course_id in (select s2.course_id
                       from student s1
                                join score s2
                                     on s1.stu_id = s2.stu_id
                       where s1.stu_id = '001')
order by s3.stu_id


-- STAR 5.2.12查询所学课程与学号为“001”的学生所学课程完全相同的学生的学号和姓名

select t3.stu_id,
       t3.stu_name
from (select *
      from (select stu_id,
                   concat_ws(',', collect_list(course_id)) as c_w
            from score
            group by stu_id) as t1
      where t1.c_w in (select concat_ws(',', collect_list(course_id))

                       from (select *
                             from score
                             where stu_id = '001'
                             order by course_id) as t1
                       group by t1.stu_id)) as t2
         inner join student t3
                    on t2.stu_id = t3.stu_id
where t3.stu_id != '001';


-- 5.2.13 按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
explain
select stu_id, avg(course) avg, sum(course) sum
from score s
group by stu_id
order by avg(course) desc

-- 2022/11/2 18:01 STAR 查看执行计划
explain
select stu_id, avg(course) avg, sum(course) sum
from score s
group by stu_id
order by avg(course) desc

-- 2022/11/2 9:18 下面答案很复杂, 以后看
select s.stu_name,
       a_c.course_name,
       a_c.course,
       a_c.avg_c
from (select a_s.stu_id,
             c.course_name,
             a_s.course,
             a_s.avg_c
      from (select stu_id,
                   course_id,
                   course,
                   avg(course) over (partition by stu_id ) avg_c
            from score) a_s
               join course c
                    on a_s.course_id = c.course_id) a_c
         join student s on a_c.stu_id = s.stu_id
order by a_c.avg_c desc;



-- 6.1.1 查询每个学生的学生平均成绩及其名次 star
-- 2022/11/3 19:31 NOTE 加一个成绩列就是多开一层窗
select stu_id, avg(course), rank() over (partition by stu_id) rk
from score
group by stu_id
order by avg(course) desc
;


select *, rank() over (order by avg_course desc)
from (select stu_id, avg(course) avg_course
      from score
      group by stu_id) t
;


-- 2022/11/2 9:20 答案
select student.stu_id
     , stu_name
     , birthday
     , sex
     , avg_score
     , rank() over (order by avg_score desc)
from student
         left join
     (select stu_id, round(avg(course), 2) avg_score
      from score
      group by stu_id) t1
     on student.stu_id = t1.stu_id;


-- 6.1.2 按各科成绩进行排序，并显示在这个学科中的排名
select *, rank() over (partition by course_id order by course desc)
from score s
;
-- 6.1.3 查询每门课程成绩最好的前两名学生姓名 star
-- 2022/11/3 19:52 note join的位置代表其含义 , 不是所有的join都有意义, 简单起见, 先join, 把所需要的数据先收集起来


select stu_id, stu_name, course
from (select sco.course_id,
             sco.stu_id,
             stu.stu_name,
             sco.course,
             dense_rank() over (partition by course_id order by course desc) rk
      from score sco
               join student stu on sco.stu_id = stu.stu_id) t

where t.rk <= 2
;


select t2.stu_name
from (select t1.stu_name
           , t1.course_name
           , t1.course
           , dense_rank() over (partition by course_name order by course desc) rk
      from (select *
            from student
                     left join score on student.stu_id = score.stu_id
                     left join course on score.course_id = course.course_id
            where course is not null) t1) t2
where rk <= 2;

-- 6.1.4 查询所有课程的成绩第2名到第3名的学生信息及该课程成绩
select s.stu_id, rank() over (partition by course_id order by course)
from score s
         join student s2 on s2.stu_id = s.stu_id
;

select *
from (select s.course_id, s.stu_id, rank() over (partition by course_id order by course) rk
      from score s
               join student s2 on s2.stu_id = s.stu_id) t
where rk = 2
   or rk = 3
;

-- 6.1.5 STAR 查询各科成绩前三名的记录（如果有并列，则全部展示，
-- 例如如果前7名为：80,80,80,79,79,77,75,70，则统计结果为数字的前三名，结果为80,80,80,79,79,77）


select stu_name, course_name, tea_name, course, rk
from (select stu_name,
             course_name,
             tea_name,
             course,
             dense_rank() over (partition by course_name order by course desc) rk
      from student
               left join score on student.stu_id = score.stu_id
               left join course on score.course_id = course.course_id
               left join teacher on teacher.tea_id = course.tea_id
      where course is not null) t2
where rk <= 3;



select stu_name, course_name, tea_name, course, rk
from (select t1.stu_name
           , t1.course_name
           , t1.course
           , t1.tea_name
           , rank() over (partition by course_name order by course desc) rk
      from (select *
            from student
                     left join score on student.stu_id = score.stu_id
                     left join course on score.course_id = course.course_id
                     left join teacher on teacher.tea_id = course.tea_id
            where course is not null) t1) t2
where rk <= 3;



DROP TABLE IF EXISTS order_summary;
CREATE TABLE order_summary
(
    `id`          varchar(32) COMMENT '商品id',
    `category_id` varchar(32) COMMENT '商品所属品类id',
    `sum`         int COMMENT '商品销售总额累计',
    `test_case`   int
) COMMENT '订单汇总表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_summary';


INSERT INTO order_summary
VALUES ('1', '1', 12, 1),
       ('2', '2', 32, 1),
       ('3', '3', 44, 1),
       ('4', '2', 56, 1),
       ('1', '3', 78, 2),
       ('1', '1', 12, 3),
       ('2', '2', 15, 3),
       ('3', '3', 17, 3),
       ('4', '3', 17, 3),
       ('5', '3', 19, 3),
       ('1', '1', 33, 4),
       ('2', '1', 55, 4),
       ('3', '1', 77, 4),
       ('4', '2', 23, 4);



-- 2022/11/2 14:40 STAR     好习惯
/*
建表
    见名知意
    有comment
    mysql指定用innodb
    字符集用UTF8
修改尤其是删除表时
    先where
    再limit 部分
    重要的要备份
    最后再delete, update, 特别慎重用drop


*/


-- SQL1 计算销量第二的商品

select *, dense_rank() over (partition by id order by sum) rk
from order_summary os
;


-- 2022/11/4 9:27 NOTE order by 往往和desc结合, 因为常常要查最高的
select *
from (select *, dense_rank() over (order by sum desc) rk
      from order_summary os) t
where rk = 2
;

select id
from (select id
      from (select id
                 , DENSE_RANK() over (order by `sum` desc) rk
            from order_summary) t1
      where rk = 2 -- 过滤出销量第二的商品
     ) t2
         right join
         (select 1) t3
         on 1 = 1
;


-- 2022/11/4 9:28 查询用户活跃记录表中连续登录大于等于三天的用户。 star
-- 2022/11/4 9:47 去重那一步非常巧妙


DROP TABLE IF EXISTS `user_active`;
CREATE TABLE user_active
(
    `user_id`     varchar(33) COMMENT '用户id',
    `active_date` date COMMENT '用户登录日期时间',
    `test_case`   int
) COMMENT '用户活跃表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_active';
INSERT INTO user_active
VALUES ('1', '2022-04-25', NULL),
       ('1', '2022-04-26', NULL),
       ('1', '2022-04-27', NULL),
       ('1', '2022-04-28', NULL),
       ('1', '2022-04-29', NULL),
       ('2', '2022-04-12', NULL),
       ('2', '2022-04-13', NULL),
       ('3', '2022-04-05', NULL),
       ('3', '2022-04-06', NULL),
       ('3', '2022-04-08', NULL),
       ('4', '2022-04-04', NULL),
       ('4', '2022-04-05', NULL),
       ('4', '2022-04-06', NULL),
       ('4', '2022-04-06', NULL),
       ('4', '2022-04-06', NULL),
       ('5', '2022-04-12', NULL),
       ('5', '2022-04-08', NULL),
       ('5', '2022-04-26', NULL),
       ('5', '2022-04-26', NULL),
       ('5', '2022-04-27', NULL),
       ('5', '2022-04-28', NULL);


select *, rank() over (partition by user_id order by active_date) rk
from user_active ua
;

select user_id, sub_temp, count(active_date)
from (select user_id, date_sub(active_date, rk) sub_temp, *
      from (select *, rank() over (partition by user_id order by active_date) rk
            from user_active ua) tt) ttt
group by user_id, sub_temp
;


select *
from (select user_id, sub_temp, count(active_date) c
      from (select user_id, date_sub(active_date, rk) sub_temp, *
            from (select *, rank() over (partition by user_id order by active_date) rk
                  from (select user_id, active_date
                        from user_active ua
                        group by user_id, active_date) t) tt) ttt
      group by user_id, sub_temp)
where c >= 3
;


select user_id
     , flag
     , count(1)
from (select user_id
           , active_date
           , row_number() over (partition by user_id order by active_date)                        rk
           , date_sub(active_date, row_number() over (partition by user_id order by active_date)) flag
      from (select user_id
                 , active_date
            from user_active
            group by user_id, active_date) t1 -- 同一天可能多个用户登录 进行去重
     ) t2 -- 计算一段数字是否连续：用这个数字减去它的排名，会得到一个相同的数
group by user_id, flag
having count(flag) >= 3 -- 连续登录大于等于三天
;

DROP TABLE IF EXISTS category;
CREATE TABLE category
(
    `category_id`   varchar(32),
    `category_name` varchar(32)
) COMMENT '品类表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/category';
INSERT INTO category
VALUES ('1', '数码'),
       ('2', '日用'),
       ('3', '厨房清洁');
;

-- 从商品销售明细表中查询各个品类共有多少个商品及销售最多的商品
select *
from category c
;

select *, rank() over (partition by category_id order by sum desc) rk
from order_summary os
;
-- 2022/11/4 10:01 我的, 非常棒

select os.category_id, category_name, count(id), max(sum)
from order_summary os
         join category c on c.category_id = os.category_id
group by os.category_id, category_name
;

-- 2022/11/4 10:01 答案
select category_name
     , product_count
     , top_sale_product
from (select category_id
           , count(1)                  product_count
           , max(if(rk = 1, id, null)) top_sale_product
      from (select category_id
                 , id
                 , rank() over (partition by category_id order by sum desc) rk
            from order_summary
            where test_case = 4 -- 测试用例数据集
           ) t1
      group by category_id) t2
         left join (select category_id
                         , category_name
                    from category) t3
                   on t2.category_id = t3.category_id;


select category_id
     , count(1)                  product_count
     , max(if(rk = 1, id, null)) top_sale_product
from (select category_id
           , id
           , rank() over (partition by category_id order by sum desc) rk
      from order_summary) t1
group by category_id



DROP TABLE IF EXISTS user_consum_details;
CREATE TABLE user_consum_details
(
    `user_id`  varchar(32),
    `buy_date` date,
    `sum`      int
) COMMENT '用户消费明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_consum_details';

INSERT INTO user_consum_details
VALUES ('1', '2022-04-26', 3000),
       ('1', '2022-04-27', 5000),
       ('1', '2022-04-29', 1000),
       ('1', '2022-04-30', 2000),
       ('2', '2022-04-27', 9000),
       ('2', '2022-04-29', 6000),
       ('3', '2022-04-22', 5000);

-- 给一张用户消费的明细表，该表中有每个用户每天消费的总额记录，需要汇总形成一张用户累计消费总额，
-- 求出每个用户在有消费记录的日期之前的所有累计总消费金额及vip等级。

select *, sum(sum) over (partition by user_id order by buy_date)
from user_consum_details ucd
;

DROP TABLE IF EXISTS user_consum_details;
CREATE TABLE user_consum_details
(
    `user_id`  varchar(32),
    `buy_date` date,
    `sum`      int
) COMMENT '用户消费明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_consum_details';
INSERT INTO user_consum_details
VALUES ('1', '2022-04-26', 3000),
       ('1', '2022-04-27', 5000),
       ('1', '2022-04-29', 1000),
       ('1', '2022-04-30', 2000),
       ('2', '2022-04-27', 9000),
       ('2', '2022-04-29', 6000),
       ('3', '2022-04-22', 5000);

-- 查询首次消费后第二天仍然消费的用户占所有用户的比率，结果保留一位小数，使用百分数显示。
select *, lag(buy_date, 1) over (partition by user_id order by buy_date) lag
from (select user_id, buy_date
      from user_consum_details ucd
      group by user_id, buy_date) t -- 去重
;

select datediff(lag_second_day, buy_date) date_sub
from (select *, lead(buy_date, 1) over (partition by user_id order by buy_date) lag_second_day
      from (select user_id, buy_date
            from user_consum_details ucd
            group by user_id, buy_date) t) tt
;

select count(*)
from (select *, datediff(lag_second_day, buy_date) date_sub
      from (select *, lead(buy_date, 1) over (partition by user_id order by buy_date) lag_second_day
            from (select user_id, buy_date
                  from user_consum_details ucd
                  group by user_id, buy_date) t) tt) ttt
where date_sub = 1
group by user_id
;


-- 2022/11/4 11:16  note 想要除, 用2个select
-- -- 2022/11/4 11:21 P1 我错在哪里了
select (select count(*)
        from (select *, datediff(lag_second_day, buy_date) date_sub
              from (select *, lead(buy_date, 1) over (partition by user_id order by buy_date) lag_second_day
                    from (select user_id, buy_date
                          from user_consum_details ucd
                          group by user_id, buy_date) t -- 2022/11/4 11:09 没有必要去重, 同一天没事
                   ) tt) ttt
        where date_sub = 1
        group by user_id)
           /
       (select count(distinct user_id)
        from user_consum_details)
;


-- 2022/11/4 11:00 就是拆分, 往那里凑而已

-- 2022/11/4 11:06 答案

select concat(
               round(
                               (select count(distinct user_id)
                                from (select user_id
                                           , buy_date
                                           , date_sub(
                                            lead(buy_date, 1) over (partition by user_id order by buy_date),
                                            1) sd
                                      from user_consum_details) t1
                                where buy_date = sd) / (select count(distinct user_id)
                                                        from user_consum_details) * 100
                   , 1),
               '%') user_fraction;
;

-- SQL6 每个销售产品第一年的销售数量、销售年份、销售总额
-- 6.1 题目需求
-- 下表中每行代表一次销售事实，请计算出每个商品的第一年的销售数量，销售年份和销售总额 star

DROP TABLE IF EXISTS order_detail;
CREATE TABLE order_detail
(
    `order_id`  varchar(32) NOT NULL,
    `id`        varchar(32) COMMENT '商品id',
    `price`     int,
    `sale_date` date,
    `num`       int
) COMMENT '订单明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail';
INSERT INTO order_detail
VALUES ('1', '1', 80, '2021-12-29', 8),
       ('2', '1', 10, '2021-12-30', 1),
       ('3', '2', 55, '2021-04-30', 5),
       ('3', '2', 55, '2020-04-30', 5),
       ('4', '3', 550, '2021-03-31', 10),
       ('5', '4', 550, '2021-05-04', 15),
       ('6', '2', 30, '2021-08-07', 3),
       ('7', '2', 60, '2020-08-09', 6);

select *
from order_detail
;

select *, rank() over (partition by id order by sale_date)
from order_detail od
;

select *
from order_detail od
where year(sale_date) = 2021
;

-- 2022/11/7 9:46 取出了第一年销售的年份
select id
     , sum(num)   `销售数量`
     , sum(price) `销售总额`
from (select *
           -- 2022/11/7 9:56 star, order by这里很关键
           , rank() over (partition by id order by year(sale_date)) rk
      from order_detail od) tt
where rk = 1
group by id
;
-- 2022/11/7 9:48 答案
select id
     , sum(num)
     , sum(price)
from (select order_id
           , id
           , price
           , num
           , rank() over (partition by id order by year(sale_date)) rk
      from order_detail) t1
where rk = 1
group by id;



DROP TABLE IF EXISTS product_attr;
CREATE TABLE product_attr
(
    `id`          varchar(32),
    `name`        varchar(32),
    `category_id` varchar(32),
    `from_date`   date
) COMMENT '商品属性表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_attr';


INSERT INTO product_attr
VALUES ('1', 'xiaomi', '1', '2021-12-23'),
       ('2', 'apple', '1', '2020-10-18'),
       ('3', 'nokia', '1', '2019-10-29'),
       ('4', 'vivo', '1', '2020-02-02');
-- SQL7 筛选去年总销量小于10的商品
-- 7.1 题目需求
-- 不考虑上架时间小于一个月的商品，假设今天的日期是2022-01-10。请筛选出去年总销量小于10（不包括10）的商品。
select *
from product_attr pa
;

-- 2022/11/7 10:23 使用explain查看计划, 显示为json格式

-- 2022/11/7 10:13 NOTE 容易想到, 但是效率不高
explain extended
select od.id, pa.name, sum(num) `总销量`
from order_detail od
         join product_attr pa on od.id = pa.id
where sale_date < date_sub('2022-01-10', 30)
  and year(sale_date) = 2021
group by od.id, pa.name
having sum(num) < 10
;


-- 2022/11/9 14:49 note 日期加减
select date_sub('2022-01-10', -3)
;


-- 2022/11/7 10:08 答案
select t1.id
     , name
from (select id
      from order_detail
      where year(sale_date) = 2021
        and id in (select id
                   from product_attr
                   where datediff('2022-01-10', from_date) > 30)
      group by id
      having sum(`num`) < 10) t1
         left join product_attr t2
                   on t1.id = t2.id
;

-- SQL8 查询每日新用户数
-- 8.1 题目需求
-- 查询从今天之前的90天内每个日期当天登录的新用户（新用户定义为在本次登录之前从未有过登录记录），
-- 假设今天是2022-01-10。

select user_id, rank() over (partition by user_id order by active_date) rk, active_date
from user_login_detail uld
;


-- 2022/11/9 14:52 去掉老用户

select *
from (select user_id, rank() over (partition by user_id order by active_date) rk, active_date
      from user_login_detail uld) tt
where rk = 1
;


select active_date, count(user_id) `新增用户数`
from (select *
      from (select user_id, rank() over (partition by user_id order by active_date) rk, active_date
            from user_login_detail uld) tt
      where rk = 1) ttt

group by active_date
;


DROP TABLE IF EXISTS user_login_detail;
CREATE TABLE user_login_detail
(
    `user_id`     varchar(32),
    `active_date` date
) COMMENT '用户登录明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_login_detail';

2
）插入数据
INSERT INTO user_login_detail
VALUES ('1', '2020-01-03'),
       ('2', '2020-01-03'),
       ('3', '2021-12-23'),
       ('3', '2021-11-09'),
       ('2', '2021-11-09'),
       ('1', '2021-11-09'),
       ('4', '2021-09-09'),
       ('5', '2021-01-09'),
       ('6', '2021-12-23');
8.4
代码实现
select active_date
     , count(1)
from (select user_id
           , active_date
      from (select user_id
                 , active_date
                 , row_number() over (partition by user_id order by active_date) rk
            from user_login_detail
            where active_date > date_sub('2022-01-10', interval 90 day)) t1
      where rk = 1) t2
group by active_date
;





SQL9
每个商品的销售最多的日期
9.1
题目需求
以下是订单明细，求出每件商品销售件数最多的日期，如果有销售数量并列情况，取最小日期，结果按照商品id
増序排序。
9.2
表结构
订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
订单总额
num	int
商品件数
sale_date	date
商品销售日期
9.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail2;
CREATE TABLE order_detail2
(
    `order_id`  varchar(32) NOT NULL,
    `id`        varchar(32) COMMENT '商品id',
    `price`     int,
    `num`       int,
    `sale_date` date
) COMMENT '订单明细表2'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail2';
2
）插入数据
INSERT INTO order_detail2
VALUES ('1', '1', 80, 8, '2021-12-29'),
       ('2', '1', 10, 1, '2021-12-30'),
       ('3', '2', 55, 5, '2021-04-30'),
       ('3', '2', 55, 5, '2021-04-30'),
       ('4', '3', 550, 10, '2021-03-31'),
       ('5', '4', 550, 15, '2021-05-04'),
       ('6', '2', 30, 3, '2021-08-07'),
       ('7', '2', 60, 6, '2020-08-09'),
       ('8', '4', 550, 15, '2021-05-05');
9.4
代码实现

-- 求出每件商品销售件数最多的日期，如果有销售数量并列情况，取最小日期，结果按照商品id増序排序。
select id, sale_date, sum(num)
from order_detail2 o
group by id, sale_date
;


select *, rank() over (partition by id order by sum_id_date desc) rk
from (select id, sale_date, sum(num) sum_id_date
      from order_detail2 o
      group by id, sale_date) tt
;

explain
select *
from (select *, rank() over (partition by id order by sum_id_date desc,sale_date ) rk
      from (select id, sale_date, sum(num) sum_id_date
            from order_detail2 o
            group by id, sale_date) tt) ttt
where rk = 1
order by id
;
set hive.exec.mode.local.auto;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto=false;

-- 2022/11/9 15:38 这个是答案, 有点问题, 不能满足并列时的最小销售日期
explain formatted
select id, sale_date, total_num
from (select id
           , sale_date
           , sum(num)                                                    total_num
           , row_number() over (partition by id order by sum(num) desc ) rk
      from order_detail2
      group by id, sale_date) t1
where rk = 1;
结果：
id	date			num
1	2021-12-29	8
2	2021-04-30	10
3	2021-03-31	10
4	2021-05-04	15
解释：
1
号商品，2021-12-29
售卖最多，共8
件
2
号商品，2021-04-30
共售卖10
件
3
号商品，2021-03-31
售卖10
件
4
号商品，2021-05-04
和2021-05-05
各售卖15
件，取最小日期。

SQL10
查询销售件数高于品类平均数的商品
10.1
题目需求
查询销售件数高于品类平均数的商品，结果按照商品id
排序。



set mapred.map.tasks.speculative.execution=true
set mapred.reduce.tasks.speculative.execution=true

-- 2022/11/9 17:17 mine
select *
from (select o.id, p.name, p.category_id, o.num, o.sale_date, avg(num) over (partition by category_id) avg_num_cate
      from order_detail_3 o
               join product_attr_2 p on o.id = p.id) tt
where num > avg_num_cate
;


select o.id, o.num, o.sale_date, avg(num) over (partition by category_id) avg_num_cate
from order_detail_3 o
         join product_attr_2 p on o.id = p.id
;


select *
from order_detail_3 o
where num > (select sum(num) / count(id) avg_num
             from order_detail_3 o)
order by id
;

select sum(num) / count(id) avg_num
from order_detail_3 o
;

select *
from order_detail_3 o
;

10.2
表结构
商品属性表
字段名	字段类型	字段含义
id	String
商品id
name	String
商品名称
category_id	String
商品所属品类id

订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
num	int
商品件数
10.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail_3;
CREATE TABLE order_detail_3
(
    `order_id`  varchar(32),
    `id`        varchar(32),
    `num`       int,
    `user_id`   varchar(32),
    `sale_date` date
) COMMENT '订单明细表3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_3';
DROP TABLE IF EXISTS product_attr_2;
CREATE TABLE product_attr_2
(
    `id`          varchar(32),
    `name`        varchar(32),
    `category_id` varchar(32)
) COMMENT '商品属性表2'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_attr_2';
2
）插入数据
INSERT INTO order_detail_3
VALUES ('1', '1', 10, '1', '2021-04-06'),
       ('2', '1', 10, '2', '2021-04-06'),
       ('3', '2', 5, '3', '2021-04-06'),
       ('4', '3', 17, '4', '2021-04-06'),
       ('5', '4', 10, '5', '2021-04-06'),
       ('6', '5', 10, '6', '2021-04-06'),
       ('7', '6', 5, '1', '2021-04-06'),
       ('8', '7', 15, '2', '2021-04-06');
INSERT INTO product_attr_2
VALUES ('1', 'xiaomi', '1\r'),
       ('2', 'apple', '1\r'),
       ('3', 'vivo', '1\r'),
       ('4', 'jianbing', '2\r'),
       ('5', 'jiaozi', '2\r'),
       ('6', 'bingxiang', '3\r'),
       ('7', 'xiyiji', '3\r');

-- 2022/11/9 16:56
select *
from product_attr_2
;


10.4
代码实现
select id
     , name
from (select t1.id
           , category_id
           , name
           , num
           , avg(num) over (partition by category_id) cg_avg
      from (select id
                 , sum(num) num
            from order_detail_3
            group by id) t1
               left join (select id
                               , name
                               , category_id
                          from product_attr_2) t2
                         on t1.id = t2.id) t3
where num > cg_avg
order by id
;
结果：
id	name
1	xiaomi
3	vivo
7	xiyiji
解释：
品类1
一共有三个商品，总售卖件数为42
，平均数为14
，商品1
和3
高于平均数。
品类2
一共有两个商品，总售卖件数为20
，平均数为10
，没有高于平均数的。
品类3
一共有两个商品，总售卖件数为20
，平均数为10
，商品7
高于平均数。
SQL11
查询每个用户的注册日期、总登录次数以及在2021
年的登录次数、订单数和订单总额
11.1
题目需求
11.2
表结构
用户登录明细表
字段名	字段类型	字段含义
user_id	String
用户id
active_date	date
每次登录的时间
订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
订单总额
num	int
商品件数
sale_date	date
商品销售日期
user_id	String
用户id
11.3
建表/
插数语句
1
）建表
CREATE TABLE order_info_3
(
    `order_id`     varchar(32),
    `user_id`      varchar(32),
    `event_time`   date,
    `total_amount` int,
    `total_count`  int
) COMMENT '订单明细表3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_3';
2
）插入数据
INSERT INTO order_info_3
VALUES ('301004', '102', '2021-09-30', 170, 1),
       ('301005', '104', '2021-10-01', 160, 1),
       ('301003', '101', '2021-10-02', 300, 2),
       ('301002', '102', '2021-10-03', 235, 2);
11.4
代码实现
select t2.user_id
     , register
     , total_login
     , total_login_2021
     , order_num_2021
     , order_sum_2021
from (select user_id
           , count(1)                                    total_login
           , max(if(register_rk = 1, active_date, null)) register
      from (select user_id
                 , active_date
                 , row_number() over (partition by user_id order by active_date) register_rk
            from user_login_detail) t1
      group by user_id) t2
         left join (select user_id
                         , count(1) total_login_2021
                    from user_login_detail
                    where year(active_date) = 2021
                    group by user_id) t3
                   on t2.user_id = t3.user_id
         left join (select user_id
                         , count(1)   order_num_2021
                         , sum(`num`) order_sum_2021
                    from order_detail_3
                    where year(sale_date) = 2021
                    group by user_id) t4
                   on t2.user_id = t4.user_id
;
SQL12
查询指定日期时候的全部商品价格
12.1
题目需求
（横断，求某一时刻的状态）
给一张商品价格修改表，每行都代表每次价格修改记录。请求出具体日期2022-04-20
的全部商品的价格表，假设所有商品在修改之前价格默认都是99
。
12.2
表结构
价格修改明细表
字段名	字段类型	字段含义
id	String
商品id
new_price	int
新的价格
changeprice_date	date
修改价格的日期

商品属性表
字段名	字段类型	字段含义
id	String
商品id
name	String
商品名称
category_id	String
商品所属品类id
12.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS price_modification_details;
CREATE TABLE price_modification_details
(
    `id`               varchar(32),
    `new_price`        int,
    `changeprice_date` date
) COMMENT '价格更改明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/price_modification_details';
2
）插入数据
INSERT INTO price_modification_details
VALUES ('1', 20, '2022-04-10'),
       ('1', 30, '2022-04-12'),
       ('1', 40, '2022-04-20'),
       ('1', 80, '2022-04-21'),
       ('2', 55, '2022-04-19'),
       ('2', 65, '2022-04-21'),
       ('3', 45, '2022-04-21');
12.4
代码实现
select t2.id
     , coalesce(new_price, 99)
from (select id
      from product_attr) t2 -- 拿到所有产品,如果有最新修改价格,则为最新价格,没有则为默认99
         left join (select t1.id
                         , new_price
                    from (select id
                               , max(changeprice_date) new_date
                          from price_modification_details
                          where changeprice_date <= '2022-04-20'
                          group by id) t1 -- 1. 找到指定日期前,商品的最新的修改日期
                             left join price_modification_details t2
                                       on t1.id = t2.id and new_date = t2.changeprice_date -- 2.得到指定日期前,已修改商品的最新修改价格
) t3
                   on t2.id = t3.id
;
结果：
id	price
1	40
2	55
3	99
4	99
商品1
在2022-04-20
改价为40
商品2
在2022-04-20
未改价，上一次改价是2022-04-19
，为55
商品3
在2022-04-20
未改价，且2022-04-20
之前未改价，所以为99
商品4
从未改价，为99
SQL13
即时订单比例
13.1
题目需求
订单配送中，顾客有时候希望自定配送日期，如果期望配送日期和下单日期相同，称为即时订单，如果配送日期和下单日期不同，称为计划订单。
每个用户最早下单的时间被称为首单。
请求出每个用户的首单中即时订单的比例，保留两位小数，以小数形式显示。
13.2
表结构
配送信息表
字段名	字段类型	字段含义
delivery_id	String
配送订单id
user_id	String
用户id
order_date	date
下单日期
custom_date	date
顾客希望的配送日期
13.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS delivery_info;
CREATE TABLE delivery_info
(
    `delivery_id` varchar(32),
    `user_id`     varchar(32),
    `order_date`  date,
    `custom_date` date
) COMMENT '邮寄信息表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/delivery_info';
2
）插入数据
INSERT INTO delivery_info
VALUES ('1', '1', '2021-08-01', '2021-08-02'),
       ('2', '2', '2021-08-02', '2021-08-02'),
       ('3', '1', '2021-08-11', '2021-08-12'),
       ('4', '3', '2021-08-24', '2021-08-24'),
       ('5', '3', '2021-08-21', '2021-08-22'),
       ('6', '2', '2021-08-11', '2021-08-13'),
       ('7', '4', '2021-08-09', '2021-08-09');
13.4
代码实现
select round((select count(user_id)
              from (select user_id
                         , custom_date
                         , order_date
                         , row_number() over (partition by user_id order by order_date) rk
                    from delivery_info) t1
              where rk = 1
                and custom_date = order_date)
                 /
             (select count(user_id)
              from (select user_id
                         , custom_date
                         , order_date
                         , row_number() over (partition by user_id order by order_date) rk
                    from delivery_info) t2
              where rk = 1), 2)
;
结果：
percentage
50.00
解释：
1
号顾客的 1
号订单是首次订单，并且是计划订单。
2
号顾客的 2
号订单是首次订单，并且是即时订单。
3
号顾客的 5
号订单是首次订单，并且是计划订单。
4
号顾客的 7
号订单是首次订单，并且是即时订单。
SQL14
向用户推荐朋友收藏的商品
14.1
题目需求
请向用户1
推荐他的朋友收藏的商品，但是不要包括用户1
已经收藏过的商品。
14.2
表结构
用户关系表
每行都代表user1
和user2
之间存在好友关系
字段名	字段类型	字段含义
user1_id	String
用户1id
user2_id	String
用户2id

用户收藏表
字段名	字段类型	字段含义
user_id	String
用户id
id	String
商品id
14.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS friendship;
CREATE TABLE friendship
(
    `user1_id` varchar(32),
    `user2_id` varchar(32)
) COMMENT '用户关系表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/friendship';
DROP TABLE IF EXISTS user_favorites;
CREATE TABLE user_favorites
(
    `user_id` varchar(32),
    `id`      varchar(32)
) COMMENT '用户收藏表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_favorites';
2
）插入数据
INSERT INTO friendship
VALUES ('1', '2'),
       ('1', '3'),
       ('1', '4'),
       ('2', '3'),
       ('2', '4'),
       ('2', '5'),
       ('6', '1');
INSERT INTO user_favorites
VALUES ('1', '88\r'),
       ('2', '23\r'),
       ('3', '24\r'),
       ('4', '56\r'),
       ('5', '11\r'),
       ('6', '23\r'),
       ('2', '77\r'),
       ('3', '77\r'),
       ('6', '88\r');
14.4
代码实现
select distinct id
from user_favorites
where user_id in
      (select if(user1_id = 1, user2_id, user1_id) uid
       from friendship
       where user1_id = 1
          or user2_id = 1) -- 用户1的所有好友收藏的商品
  and id not in (select id
                 from user_favorites
                 where user_id = 1);
结果：
recommended_goods_id
23
24
56
33
77
解释：
用户1
同用户2, 3, 4, 6
是朋友关系。
推荐商品为：商品23
来自于用户2
，商品24
来自于用户3
，商品56
来自于用户3
以及商品33
来自于用户6
。
商品77
同时被用户2
和用户3
推荐。
商品88
没有被推荐，因为用户1
已经收藏了它。
SQL15
查询所有用户的大于等于两天的连续登录区间
15.1
题目需求
15.2
表结构
用户活跃表
字段名	字段类型	字段含义
user_id	String
用户id
active_date	date
用户登录日期时间
15.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_active_2;
CREATE TABLE user_active_2
(
    `user_id`     varchar(32),
    `active_date` date
) COMMENT '用户活跃表2'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_active_2';
2
）插入数据
INSERT INTO user_active_2
VALUES ('1', '2021-04-21'),
       ('1', '2021-04-22'),
       ('1', '2021-04-23'),
       ('1', '2021-04-25'),
       ('1', '2021-04-26'),
       ('1', '2021-04-28'),
       ('2', '2021-04-23'),
       ('2', '2021-04-24'),
       ('2', '2021-04-25'),
       ('3', '2021-04-23');
15.4
代码实现
select user_id
     , min(active_date) start_date
     , max(active_date) end_date

from (select user_id
           , active_date
           , date_sub(active_date, rk) flag
      from (select user_id
                 , active_date
                 , row_number() over (partition by user_id order by active_date) rk
            from user_active_2) t1) t2
group by user_id, flag
having count(flag) >= 2
;
结果：
user_id	start_date	end_date
1		2021-04-21	2021-04-23
1		2021-04-25	2021-04-26
2		2021-04-23	2021-04-25
SQL16
男性和女性每日的购物总金额统计
16.1
题目需求
给两个表，用户信息表和订单明细表，求出男性和女性每日的购物总金额。
如果当天男性或者女性没有购物，则统计结果为0
。
16.2
表结构
用户信息表
字段名	字段类型	字段含义
user_id	String
用户id
sex	String
用户性别
订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
订单总额
num	int
商品件数
sale_date	date
商品销售日期
user_id	String
用户id
16.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_info;
CREATE TABLE user_info
(
    `user_id` varchar(32),
    `sex`     varchar(32)
) COMMENT '用户信息表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_info';
DROP TABLE IF EXISTS order_detail_4;
CREATE TABLE order_detail_4
(
    `order_id`  varchar(32),
    `id`        varchar(32),
    `price`     int,
    `num`       int,
    `sale_date` date,
    `user_id`   varchar(32)
) COMMENT '订单明细表4'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_4';
2
）插入数据
INSERT INTO user_info
VALUES ('1', '男'),
       ('2', '男'),
       ('3', '男'),
       ('4', '男'),
       ('5', '女'),
       ('6', '女'),
       ('7', '女'),
       ('8', '女');
INSERT INTO order_detail_4
VALUES ('1', '2', 500, 3, '2022-01-02', '1'),
       ('2', '2', 800, 3, '2022-01-03', '2'),
       ('3', '2', 1200, 3, '2022-01-03', '3'),
       ('4', '2', 200, 3, '2022-01-04', '2'),
       ('5', '2', 700, 3, '2022-01-04', '4'),
       ('6', '2', 300, 3, '2022-01-05', '1'),
       ('7', '2', 430, 3, '2022-01-06', '2'),
       ('8', '2', 230, 3, '2022-01-08', '3'),
       ('9', '2', 320, 3, '2022-01-02', '5'),
       ('10', '2', 590, 3, '2022-01-03', '6'),
       ('11', '2', 100, 3, '2022-01-04', '8'),
       ('12', '2', 40, 3, '2022-01-06', '7'),
       ('13', '2', 20, 3, '2022-01-07', '2');
16.4
代码实现
select sale_date
     , sum(if(sex = '男', price, 0)) male
     , sum(if(sex = '女', price, 0)) female

from (select sex
           , sale_date
           , price
      from order_detail_4 t1
               left join user_info t2
                         on t1.user_id = t2.user_id) t3
group by sale_date
order by sale_date
;
结果：
date			male		female
2022-01-02	500		320
2022-01-03	2000	590
2022-01-04	900		100
2022-01-05	300		0
2022-01-06	430		40
2022-01-07	20		0
2022-01-08	230		0
SQL17
订单平均总额趋势分析
17.1
题目需求
查询7
天内（该日期+
前六天）消费者的订单总额平均值，保留两位小数，四舍五入，按照日期升序排序。
17.2
表结构
订单消费明细表
字段名	字段类型	字段含义
order_id	String
订单id
price	int
订单总额
sale_date	date
商品销售日期
17.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail_5;
CREATE TABLE order_detail_5
(
    `order_id`  varchar(32),
    `price`     int,
    `sale_date` date
) COMMENT '订单明细表5'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_5';
2
）插入数据
INSERT INTO order_detail_5
VALUES ('1', 1500, '2022-01-01'),
       ('2', 2000, '2022-01-02'),
       ('3', 5000, '2022-01-02'),
       ('4', 6000, '2022-01-03'),
       ('5', 2000, '2022-01-04'),
       ('6', 2500, '2022-01-05'),
       ('7', 1000, '2022-01-06'),
       ('8', 500, '2022-01-07'),
       ('9', 300, '2022-01-07'),
       ('10', 200, '2022-01-07'),
       ('11', 3000, '2022-01-08'),
       ('12', 10000, '2022-01-09'),
       ('13', 8000, '2022-01-10'),
       ('14', 2000, '2022-01-10');
17.4
代码实现
select sale_date
     , total_7
     , round(total_7 / 7, 2)
from (select sale_date
           , sum(today_total) over (order by sale_date rows between 6 preceding and current row ) total_7
           , rk
      from (select sale_date
                 , row_number() over (order by sale_date) rk -- 过滤走不是一个完整的七天的数据
                 , sum(price)                             today_total
            from order_detail_5
            group by sale_date) t1 -- 计算出每日销售总额
     ) t2
where rk >= 7
;
结果：
date			amount	avg
2022-01-07	21000	3000
2022-01-08	22500	3214.29
2022-01-09	25500	3642.86
2022-01-10	29500	4214.29
SQL18
购买过商品1
和商品2
但是没有购买商品3
的顾客
18.1
题目需求
18.2
表结构
订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
订单总额
num	int
商品件数
sale_date	date
商品销售日期
user_id	String
用户id
18.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail_6;
CREATE TABLE order_detail_6
(
    `order_id`  varchar(32),
    `id`        varchar(32),
    `price`     int,
    `num`       int,
    `sale_time` date,
    `user_id`   varchar(32)
) COMMENT '订单明细表6'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_6';
2
）插入数据
INSERT INTO order_detail_6
VALUES ('1001', '1', 100, 10, '2022-04-01', '1'),
       ('1002', '2', 100, 10, '2022-04-01', '1'),
       ('1003', '3', 100, 10, '2022-04-02', '1'),
       ('1004', '1', 100, 10, '2022-04-02', '2'),
       ('1005', '2', 100, 10, '2022-04-03', '2'),
       ('1006', '1', 100, 10, '2022-04-03', '3'),
       ('1007', '2', 100, 10, '2022-04-04', '4'),
       ('1008', '4', 100, 10, '2022-04-04', '4'),
       ('1009', '1', 100, 10, '2022-04-05', '5'),
       ('1010', '4', 100, 10, '2022-04-06', '5');
18.4
代码实现
select user_id
from (select id
           , user_id
           , if(id = 1, 1, 0)  flag1
           , if(id = 2, 1, 0)  flag2
           , if(id = 3, -5, 0) flag3
      from order_detail_6) t1
group by user_id
having sum(flag1 + flag2 + flag3) = 2
;
结果：
user_id
2
SQL19
每日购买商品1
和商品2
的差值
19.1
题目需求
差值=
商品1
销量-
商品2
销量
如果当天没有该商品销售记录，则销量为0
。
19.2
表结构
订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
订单总额
num	int
商品件数
sale_date	date
商品销售日期
user_id	String
用户id
19.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail_7;
CREATE TABLE order_detail_7
(
    `order_id`  varchar(32),
    `id`        varchar(32),
    `price`     int,
    `num`       int,
    `sale_date` date,
    `user_id`   varchar(32)
) COMMENT '订单明细表7'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_7';
2
）插入数据
INSERT INTO order_detail_7
VALUES ('1001', '1', 100, 8, '2022-04-01', '1'),
       ('1002', '2', 100, 18, '2022-04-01', '1'),
       ('1003', '1', 100, 19, '2022-04-02', '1'),
       ('1004', '2', 100, 7, '2022-04-02', '2'),
       ('1005', '1', 100, 24, '2022-04-03', '2'),
       ('1006', '2', 100, 10, '2022-04-03', '3'),
       ('1007', '1', 100, 9, '2022-04-04', '4'),
       ('1008', '2', 100, 10, '2022-04-04', '4'),
       ('1009', '1', 100, 8, '2022-04-05', '5'),
       ('1010', '2', 100, 10, '2022-04-06', '5');
19.4
代码实现
select sale_date
     , f1 - f2
from (select sale_date
           , sum(if(id = 1, num, 0)) f1
           , sum(if(id = 2, num, 0)) f2
      from order_detail_7
      group by sale_date) t1
;
结果：
sale_date		diff
2022-04-01	-10
2022-04-02	12
2022-04-03	14
2022-04-04	-1
2022-04-05	8
2022-04-06	-10
SQL20
找出用户的最近三笔订单
20.1
题目需求
假定每个用户每天只有一笔订单。如果用户总订单小于3
，则输出用户的全部订单。
结果按照用户id
升序排序，每个用户的三笔订单按照日期升序排序。
20.2
表结构
订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
订单总额
num	int
商品件数
sale_date	date
商品销售日期
user_id	String
用户id
20.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail_8;
CREATE TABLE order_detail_8
(
    `order_id`  varchar(32),
    `id`        varchar(32),
    `price`     int,
    `num`       int,
    `sale_date` date,
    `user_id`   varchar(32)
) COMMENT '订单明细表8'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_8';
2
）插入数据
INSERT INTO order_detail_8
VALUES ('1001', '1', 100, 8, '2021-02-01', '1'),
       ('1002', '2', 100, 18, '2022-04-01', '1'),
       ('1003', '1', 100, 19, '2022-04-03', '1'),
       ('1004', '2', 100, 7, '2022-04-02', '2'),
       ('1005', '1', 100, 24, '2022-04-03', '2'),
       ('1006', '2', 100, 10, '2022-04-03', '3'),
       ('1007', '1', 100, 9, '2022-04-04', '3'),
       ('1008', '2', 100, 10, '2022-04-05', '3'),
       ('1009', '1', 100, 8, '2022-04-08', '3'),
       ('1010', '2', 100, 10, '2022-04-06', '4');
20.4
代码实现
select user_id
     , order_id
     , sale_date
from (select user_id
           , order_id
           , sale_date
           , row_number() over (partition by user_id order by sale_date desc) rk
      from order_detail_8) t1
where rk <= 3
order by user_id, sale_date
;
结果：
user_id	order_id	sale_date
1		1001	2021-02-01
1		1002	2022-04-01
1		1003	2022-04-03
2		1004	2022-04-02
2		1005	2022-04-03
3		1007	2022-04-04
3		1008	2022-04-05
3		1009	2022-04-08
4		1010	2022-04-06
SQL21
用户登录日期的最大空档期
21.1
题目需求
对于每个用户，求出每次访问和下一次访问之间的最大空档天数，如果是表中的最后一次访问，则需要计算最后一次访问和今天之间的天数。
假设今天是2021-01-01
21.2
表结构
用户活跃表
字段名	字段类型	字段含义
user_id	String
用户id
active_date	date
用户登录日期时间
21.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_active_3;
CREATE TABLE user_active_3
(
    `user_id`     varchar(32),
    `active_date` date
) COMMENT '用户活跃表3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_active_3';
2
）插入数据
INSERT INTO user_active_3
VALUES ('1', '2021-10-20'),
       ('1', '2021-11-28'),
       ('1', '2021-12-08'),
       ('2', '2021-10-05'),
       ('2', '2021-12-09'),
       ('3', '2020-11-11');
21.4
代码实现
select user_id
     , max(diff)
from (select user_id
           , active_date
           , lead(active_date, 1, '2021-01-01') over (partition by user_id order by active_date)
           , datediff(lead(active_date, 1, '2021-01-01') over (partition by user_id order by active_date),
                      active_date) diff
      from user_active_3) t1
group by user_id
;
结果：
user_id	biggest_window
1		39
1		65
2		51
对于第一个用户，问题中的空档期在以下日期之间：
2020-10-20
至 2020-11-28
，共计 39
天。
2020-11-28
至 2020-12-3
，共计 5
天。
2020-12-3
至 2021-1-1
，共计 29
天。
由此得出，最大的空档期为 39
天。
对于第二个用户，问题中的空档期在以下日期之间：
2020-10-5
至 2020-12-9
，共计 65
天。
2020-12-9
至 2021-1-1
，共计 23
天。
由此得出，最大的空档期为65
天。
对于第三个用户，问题中的唯一空档期在 2020-11-11
至 2021-1-1
之间，共计 51
天。
SQL22
账号多地登录
22.1
题目需求
22.2
表结构
用户活跃表
字段名	字段类型	字段含义
user_id	String
用户id
ip_address	String
用户登录ip
地址
login_ts	timestamp
登录时间
logout_ts	timestamp
登出时间
22.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_active_4;
CREATE TABLE user_active_4
(
    `user_id`    varchar(32),
    `ip_address` varchar(32),
    `login_ts`   timestamp NULL,
    `logout_ts`  timestamp NULL
) COMMENT '用户活跃表4'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_active_4';
2
）插入数据
INSERT INTO user_active_4
VALUES ('1', '1', '2021-02-01 01:00:00', '2021-02-01 01:30:00'),
       ('1', '2', '2021-02-01 00:00:00', '2021-02-01 03:30:00'),
       ('2', '6', '2021-02-01 12:30:00', '2021-02-01 14:00:00'),
       ('2', '7', '2021-02-02 12:30:00', '2021-02-02 14:00:00'),
       ('3', '9', '2021-02-01 08:00:00', '2021-02-01 08:59:59'),
       ('3', '13', '2021-02-01 09:00:00', '2021-02-01 09:59:59'),
       ('4', '10', '2021-02-01 08:00:00', '2021-02-01 09:00:00'),
       ('4', '11', '2021-02-01 09:00:00', '2021-02-01 09:59:59');
22.4
代码实现
select user_id
from (select user_id
           , ip_address
           , unix_timestamp(login_ts)                                                           `in`
           , unix_timestamp(logout_ts)                                                          `out`
           , lead(unix_timestamp(login_ts), 1) over (partition by user_id order by ip_address)  next_in
           , lead(unix_timestamp(logout_ts), 1) over (partition by user_id order by ip_address) next_out
      from (select user_id
                 , ip_address
                 , max(login_ts)  login_ts
                 , max(logout_ts) logout_ts
            from user_active_4
            group by user_id, ip_address) t2) t1
where (next_in < `in` and next_out >= `in`)
   or (next_in <= `out` and next_out > `out`)
   or (next_out > `in` and next_out < `out`)
   or (next_in < `in` and next_out > `out`)
group by user_id
;
结果：
user_id
1
4
解释：
1
该账户从 "2021-02-01 09:00:00"
到 "2021-02-01 09:30:00"
在两个不同的网络地址(1 and 2)
上激活了
2
该账户在两个不同的网络地址 (6, 7)
激活了，但在不同的时间上
3
该账户在两个不同的网络地址 (9, 13)
激活了，虽然是同一天，但时间上没有交集
4
该账户从 "2021-02-01 17:00:00"
到 "2021-02-01 17:00:00"
在两个不同的网络地址 (10 and 11)
上激活了
SQL23
销售额完成任务指标的商品
23.1
题目需求
假如每个商品每个月需要售卖出一定的销售总额
请写出SQL
查询连续两个月销售总额大于等于任务总额的商品
23.2
表结构
商品供应数量表
字段名	字段类型	字段含义
id	String
商品id
assignment	int
商品每个月固定的销售任务指标

订单详情
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
订单总额
num	int
商品件数
sale_date	date
商品销售日期
23.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_info;
CREATE TABLE order_info
(
    `order_id`  varchar(32),
    `id`        varchar(32),
    `price`     int,
    `num`       int,
    `sale_date` date
) COMMENT '订单详情表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_info';
DROP TABLE IF EXISTS product_supply;
CREATE TABLE product_supply
(
    `id`         varchar(32),
    `assignment` int
) COMMENT '商品供应数量表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_supply';
2
）插入数据
INSERT INTO order_info
VALUES ('2', '1', 107100, 1, '2021-06-02'),
       ('4', '2', 10400, 1, '2021-06-20'),
       ('11', '2', 58800, 1, '2021-07-23'),
       ('1', '2', 49300, 1, '2021-05-03'),
       ('15', '1', 75500, 1, '2021-05-23'),
       ('10', '1', 102100, 1, '2021-06-15'),
       ('14', '2', 56300, 1, '2021-07-21'),
       ('19', '2', 101100, 1, '2021-05-09'),
       ('8', '1', 64900, 1, '2021-07-26'),
       ('7', '1', 90900, 1, '2021-06-14');
INSERT INTO product_supply
VALUES ('1', 21000),
       ('2', 10400);
23.4
代码实现
select id
     , month(sale_date) - rk flag
     , max(total_mth)
     , max(assignment)
from (select t2.id
           , dt
           , row_number() over (partition by id order by dt) rk
           , sale_date
           , total_mth
           , assignment
      from (select id
                 , date_format(sale_date, '%y-%m') dt
                 , max((sale_date))                sale_date
                 , sum(price)                      total_mth
            from order_info
            group by date_format(sale_date, '%y-%m'), id) t1
               left join product_supply t2
                         on t1.id = t2.id
      where total_mth > assignment) t3
group by id, month(sale_date) - rk
having count(flag) >= 2
;
对于商品
1
：
在 2021
年6
月，销售额为 107100 + 102100 + 90900 = 300100
。
在 2021
年7
月，销售额为 64900
。
可见收入连续两月超过21000
，因此商品1
列入结果表中。
对于商品2
：
在 2021
年5
月，销售额为 49300
。
在 2021
年6
月，销售额为 10400
。
在 2021
年7
月，销售额为 56300
。
可见收入在5
月与7
月超过了，但6
月没有。
因为账户没有没有连续两月超过最大收入，商品2
不列入结果表中。
SQL24
根据商品销售情况进行商品分类
24.1
题目需求
按照销售件数对商品进行分类
24.2
表结构
商品数量分类表
字段名	字段类型	字段含义
type_name	String
商品分类名
bottomlimit	int
商品销售件数数量范围
冷门商品,0
一般商品,5001
热门商品,20000
商品销售详情表
字段名	字段类型	字段含义
id	String
商品id
num	int
商品销售件数
24.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS sales_num;
CREATE TABLE sales_num
(
    `id`  varchar(32),
    `num` int
) COMMENT '商品销售详情表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/sales_num';
2
）插入数据
INSERT INTO sales_num
VALUES ('1', 300),
       ('2', 4000),
       ('3', 9000),
       ('4', 40000);
24.4
代码实现
select type_name
     , count(1)
from (select id
           , case
                 when num >= 0 and num < 5000 then '冷门商品'
                 when num >= 5001 and num < 20000 then '一般商品'
                 when num >= 20000 then '热门商品'
        end type_name
      from sales_num) t1
group by type_name
;
结果：
type		count
冷门商品	2
一般商品	1
热门商品	1
SQL25
付款率
25.1
题目需求
用户下单之后需要付款，如果在30
分钟内未付款，则会超时。求每个用户的付款率。
25.2
表结构
用户信息表
字段名	字段类型	字段含义
user_id	String
用户id
active_date	date
用户注册日期
用户付款详情表
字段名	字段类型	字段含义
user_id	String
用户id
timestamp	timestamp
用户下单时间
action	String
下单是否超时
25.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS paoment_detail;
CREATE TABLE paoment_detail
(
    `user_id`   varchar(32),
    `timestamp` timestamp NULL,
    `action`    varchar(32)
) COMMENT '用户付款详情表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/paoment_detail';
DROP TABLE IF EXISTS user_info_2;
CREATE TABLE user_info_2
(
    `user_id`     varchar(32),
    `active_date` date
) COMMENT '用户信息表2'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_info2';
2
）插入数据
INSERT INTO paoment_detail
VALUES ('1', '2021-01-05 19:30:46', 'timeout'),
       ('1', '2021-07-14 06:00:00', 'timeout'),
       ('2', '2021-01-05 19:30:46', 'success'),
       ('2', '2021-07-14 06:00:00', 'success'),
       ('2', '2021-01-04 19:30:46', 'success'),
       ('3', '2021-07-14 06:00:00', 'success'),
       ('3', '2021-01-05 19:30:46', 'timeout');
INSERT INTO user_info_2
VALUES ('1', '2021-10-20'),
       ('2', '2021-10-05'),
       ('3', '2021-11-11'),
       ('4', '2022-04-12');
25.4
代码实现
select user_id
     , round(success / cnt, 2)
from (select user_id
           , sum(suc) success
           , count(1) cnt
      from (select t1.user_id
                 , if(action = 'success', 1, 0) suc
                 , if(action = 'timeout', 1, 0) fail
            from user_info_2 t1
                     left join paoment_detail pd
                               on t1.user_id = pd.user_id) t1
      group by user_id) t2
;
结果：
user_id	success_rate
1		0.00
2		1.00
3		0.50
4		0.00
用户1
，全部超时
用户2
，全部成功
用户3
，成功一半
用户4
，没有任何下单信息，为0
SQL26
商品库存变化
26.1
题目需求
26.2
表结构
商品库存明细表
字段名	字段类型	字段含义
id	String
商品id
date	date
变化时间
action	String
补货或者是售货
amount	int
补货或者售货数量
26.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS stock_detail;
CREATE TABLE stock_detail
(
    `id`     varchar(32),
    `date`   date,
    `action` varchar(32),
    `amount` int
) COMMENT '商品库存明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/sales_num';
2
）插入数据
INSERT INTO stock_detail
VALUES ('1', '2021-01-01', 'supply', 2000),
       ('1', '2021-01-03', 'sell', 1000),
       ('1', '2021-01-05', 'supply', 3000),
       ('2', '2021-01-01', 'supply', 7000),
       ('2', '2021-01-01', 'supply', 1000),
       ('2', '2021-01-04', 'sell', 8000);
26.4
代码实现
select id
     , date
     , sum(remain) over (partition by id order by date)
from (select id
           , date
           , sum(if(action = 'sell', -amount, amount)) remain
      from stock_detail
      group by id, date) t1
;
结果：
id	date			balance
1	2021-01-01	2000
1	2021-01-03	1000
1	2021-01-05	4000
2	2021-01-01	8000
2	2021-01-04	0
SQL27
各品类销量前三的所有商品
27.1
题目需求
从商品销售明细表中查询各个品类销售数量前三的商品
如果该品类小于三个商品，则输出所有的商品销量
27.2
表结构
订单汇总表
字段名	字段类型	字段含义
id	String
商品id
category_id	String
商品所属品类id
sum	int
商品销售总额累计
品类表
字段名	字段类型	字段含义
category_id	String
商品所属品类id
category_name	String
商品品类名
27.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_summary_1;
CREATE TABLE order_summary_1
(
    `id`          varchar(32),
    `category_id` varchar(32),
    `sum`         int
) COMMENT '订单汇总表1'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_summary_1';
2
）插入数据
INSERT INTO order_summary_1
VALUES ('1', '1', 66),
       ('2', '1', 23),
       ('3', '1', 78),
       ('4', '2', 23),
       ('5', '3', 89),
       ('6', '1', 99),
       ('9', '1', 128);
27.4
代码实现
select id, category_name
from (select id
           , c.category_id
           , sum
           , rank() over (partition by c.category_id order by sum desc) rk
           , category_name
      from order_summary_1
               left join category c on order_summary_1.category_id = c.category_id) t1
where rk <= 3
;
SQL28
各品类中商品价格的中位数
28.1
题目需求
如果是偶数则输出中间两个值的平均值，如果是奇数，则输出中间数即可。
28.2
表结构
商品详情表
字段名	字段类型	字段含义
id	String
商品id
category_id	String
商品所属品类id
price	int
商品售价

品类表
字段名	字段类型	字段含义
category_id	String
商品所属品类id
category_name	String
商品品类名

某品类下的商品售价统计表
字段名	字段类型	字段含义
price	int
商品价格
frequency	int
商品价格频次
28.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS produt_detail;
CREATE TABLE produt_detail
(
    `id`          varchar(32),
    `category_id` varchar(32),
    `price`       int
) COMMENT '商品详情表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/produt_detail';
2
）插入数据
INSERT INTO produt_detail
VALUES ('1', '1', 23),
       ('2', '1', 45),
       ('3', '1', 46),
       ('4', '2', 56),
       ('5', '2', 45),
       ('6', '3', 76),
       ('7', '3', 23),
       ('8', '3', 55);
28.4
代码实现
select category_id
     , round(avg(price), 2)
from (select category_id
           , price
      from (select category_id
                 , price
                 , row_number() over (partition by category_id order by price) rk
                 , count(category_id) over (partition by category_id)          cnt
            from produt_detail) t1
      where rk in (floor((cnt + 1) / 2), floor((cnt + 2) / 2)) -- 偶数中位数原样输出
         -- where rk in  (floor((cnt + 1) / 2), floor((cnt + 1) / 2)) -- 偶数中位数只输出一个
     ) t2
group by category_id
;
结果：
median
1000
SQL29
找出销售额连续多天超过100
的记录
29.1
题目需求
29.2
表结构
商品详情表
字段名	字段类型	字段含义
order_id	String
商品id
date	date
商品销售日期
price	int
商品当天销售额
29.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail_9;
CREATE TABLE order_detail_9
(
    `order_id` varchar(23),
    `date`     date,
    `price`    int
) COMMENT '订单明细表9'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_9';
2
）插入数据
INSERT INTO order_detail_9
VALUES ('1', '2021-01-01', 10),
       ('2', '2021-01-02', 109),
       ('3', '2021-01-03', 158),
       ('4', '2021-01-04', 99),
       ('5', '2021-01-05', 145),
       ('6', '2021-01-06', 1455),
       ('7', '2021-01-07', 1199),
       ('8', '2021-01-08', 188);
29.4
代码实现
select *
from (select *
           , count(*) over (partition by flag) cnt
      from (select order_id
                 , date
                 , price
                 , row_number() over (order by date)                 rk
                 , date_sub(date, row_number() over (order by date)) flag
            from order_detail_9
            where price > 100) t1) t2
where cnt >= 3
;
结果：
5,2021-01-05,145
6,2021-01-06,1455
7,2021-01-07,1199
8,2021-01-08,188
SQL30
查询有新注册用户的当天的新用户数量、新用户的第一天留存率
30.1
题目需求
30.2
表结构
用户登录明细表
字段名	字段类型	字段含义
user_id	String
用户id
date	date
商品销售日期
price	int
商品当天销售额
30.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_login_detail_1;
CREATE TABLE user_login_detail_1
(
    `user_id` varchar(32),
    `date`    date,
    `price`   int
) COMMENT '用户登录明细表1'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_login_detail_1';
2
）插入数据
INSERT INTO user_login_detail_1
VALUES ('1', '2022-01-01', 50),
       ('1', '2022-01-02', 100),
       ('2', '2022-03-01', 100),
       ('3', '2022-01-01', 100),
       ('3', '2022-02-01', 800);
30.4
代码实现
select register_date
     , register
     , remain_1 / register retention
from (select register_date
           , count(a.user_id) register -- 新增用户
           , count(b.user_id) remain_1 -- 次日留存
      from (select user_id,
                   min(date) register_date
            from user_login_detail_1
            group by user_id) a -- 找出新增用户
               left join user_login_detail_1 b
                         on a.user_id = b.user_id
                             and datediff(b.date, a.register_date) = 1 -- 找出第二天活跃的新增用户
      group by register_date) t1
;
结果
:
register_date	num	retention
新增用户的登录用户数
2022-01-01	2	0.50                        1
2022-03-01	1	0.00	                        0
SQL31
某商品售卖明细表求出连续售卖的时间区间和非连续售卖的时间区间
31.1
题目需求
只统计2021-01-01
至2021-12-31
之间的数据
如果有非售卖记录，那就是nosale
的起止日期
如果有售卖记录，那就是sale
的起止日期
31.2
表结构
售卖表
字段名	字段类型	字段含义
date	date
商品销售日期
无售卖表
字段名	字段类型	字段含义
date	date
商品销售日期
31.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS sales;
CREATE TABLE sales
(
    `date` date
) COMMENT '售卖表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/sales';
DROP TABLE IF EXISTS no_sales;
CREATE TABLE no_sales
(
    `date` date
) COMMENT '无售卖表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/no_sales';
2
）插入数据
INSERT INTO sales
VALUES ('2020-12-30'),
       ('2020-12-31'),
       ('2021-01-04'),
       ('2021-01-05');
INSERT INTO no_sales
VALUES ('2020-12-29'),
       ('2021-01-01'),
       ('2021-01-02'),
       ('2021-01-03'),
       ('2021-01-06');
31.4
代码实现
select status
     , min(date) start_date
     , max(date) end_date
from (select status
           , date
           , row_number() over (partition by status order by date)                 rk
           , date_sub(date, row_number() over (partition by status order by date)) diff
      from (select *
            from (select 'sales' as status
                       , date
                  from sales
                  union all
                  select 'nosales' as status
                       , date
                  from no_sales) t1
            where date >= '2021-01-01'
              and date <= '2021-12-31') t2) t3
group by status, diff
order by start_date
;
结果：
state		start_date	end_date
nosales		2021-01-01	2021-01-03
sale     	      2021-01-04	2021-01-05
nosales		2021-01-06	2021-01-06
SQL32
登录次数及交易次数统计
32.1
题目需求
有两张表，登录记录和交易记录
想要查询多少用户登录了但是没有交易，多少用户登录并进行一次交易、等等
32.2
表结构
登录记录表
字段名	字段类型	字段含义
user_id	String
用户id
date	date
用户登录日期
交易记录表
字段名	字段类型	字段含义
user_id	String
用户id
date	date
商品销售日期
price	int
商品当天销售额
32.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS register;
CREATE TABLE register
(
    `user_id` varchar(32),
    `date`    date
) COMMENT '登录记录表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/register';
DROP TABLE IF EXISTS translation_recode;
CREATE TABLE translation_recode
(
    `user_id` varchar(32),
    `date`    date,
    `price`   int
) COMMENT '交易记录表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/translation_recode';
2
）插入数据
INSERT INTO register
VALUES ('1', '2021-01-01'),
       ('2', '2021-01-02'),
       ('3', '2021-01-01'),
       ('6', '2021-01-03'),
       ('1', '2021-01-02'),
       ('2', '2021-01-03'),
       ('1', '2021-01-04'),
       ('7', '2021-01-11'),
       ('9', '2021-01-25'),
       ('8', '2021-01-28');
INSERT INTO translation_recode
VALUES ('1', '2021-01-02', 120),
       ('2', '2021-01-03', 120),
       ('7', '2021-01-11', 120),
       ('1', '2021-01-04', 120),
       ('9', '2021-01-25', 120),
       ('9', '2021-01-25', 120),
       ('8', '2021-01-28', 120),
       ('9', '2021-01-25', 120);
32.4
代码实现
select nvl(sale_count, rnb) sale_count
     , nvl(login_count, 0)  login_count
from (SELECT 0 AS rnb
      UNION
      SELECT ROW_NUMBER() OVER () AS rnb
      FROM translation_recode) t3 -- 第二步:通过交易表生成序列号,并且加上0号序列,主要是解决 结果表1中没有sale_count为2的问题
         left join
     (select sale_count,
             count(sale_count) login_count
      from (select register.user_id
                 , register.date
                 , nvl(sale_count, 0) sale_count-- 将null转为0方便统计
            from register
                     left join (select user_id
                                     , date
                                     , count(1) sale_count
                                from translation_recode
                                group by user_id, date) t1 -- 每个用户每天的交易次数
                               on register.user_id = t1.user_id and register.date = t1.date) t2
           -- 一行记录代表的含义是一个用户登录一次,做了几次交易
           -- sale_count为0即为一个用户登录,该用户没交易
           -- sale_count为1即为一个用户登录,该用户交易一次
           -- sale_count为2即为一个用户登录,该用户交易两次
      group by sale_count) t4 -- 第一步:初步得出结果表1
     on t3.rnb = t4.sale_count
where rnb <= (select sale_count
              from (select register.user_id
                         , register.date
                         -- , t1.user_id
                         -- , t1.date
                         , nvl(sale_count, 0) sale_count-- 将null转为0方便统计
                    from register
                             left join (select user_id
                                             , date
                                             , count(1) sale_count
                                        from translation_recode
                                        group by user_id, date) t1 -- 每个用户每天的交易次数
                                       on register.user_id = t1.user_id and register.date = t1.date) t2
              group by sale_count
              order by sale_count desc
              limit 1) -- 第三步: 与t4表内容大致一样 目的是限制序列号,也就是登录一次,交易五次\六次\七次的记录不要 因为大部分都为0
;
结果：
sale_count	login_count
0			4
1			5
2			0
3			1
解释：
1,2021-01-01
这次登录，没有购买
2,2021-01-02
这次登录，没有购买
3,2021-01-01
这次登录，没有购买
6,2021-01-03
这次登录，没有购买
1,2021-01-02
这次登录，购买一次
2,2021-01-03
这次登录，购买一次
1,2021-01-04
这次登录，购买一次
7,2021-01-11
这次登录，购买一次
9,2021-01-25
这次登录，购买三次
8,2021-01-28
这次登录，购买一次
所以统计
购买0
次的，共有4
人次
购买1
次的，共有5
人次
购买2
次的，共有0
人次
购买3
次的，共有1
人次
SQL33
按年度列出销售总额
33.1
题目需求
33.2
表结构
商品明细表
字段名	字段类型	字段含义
product_id	String
商品id
product_name	String
商品名称
交易记录表
字段名	字段类型	字段含义
product_id	String
商品id
start_date	date
商品销售起始日期
end_date	date
商品销售结束日期
avg	int
商品平均每日销售额
33.3
建表/
插数语句
1
）建表
CREATE TABLE `product_detal`
(
    `product_id`   varchar(32),
    `product_name` varchar(32)
) COMMENT '产品明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_detal';
DROP TABLE IF EXISTS deal_record;;
CREATE TABLE deal_record
(
    `product_id` varchar(32),
    `start_date` date,
    `end_date`   date,
    `avg`        int
) COMMENT '成交记录表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/deal_record';
2
）插入数据
INSERT INTO product_detal
VALUES ('1', 'xiaomi'),
       ('2', 'apple'),
       ('3', 'vivo');
INSERT INTO deal_record
VALUES ('1', '2019-01-25', '2019-02-28', 100),
       ('2', '2018-12-01', '2020-01-01', 10),
       ('3', '2019-12-01', '2020-01-31', 1);
33.4
代码实现
select t2.product_id
     , product_name
     , report
     , sum(amout)
from (
         -- 求出2018年的记录
         select product_id
              , '2018'            report
              , datediff(
                        if(end_date <= '2018-12-31', end_date, date('2018-12-31'))
                    , if(start_date >= '2018-01-01', start_date, date('2018-01-01'))
                    ) + 1         diff
              , (datediff(
                         if(end_date <= '2018-12-31', end_date, date('2018-12-31'))
                     , if(start_date >= '2018-01-01', start_date, date('2018-01-01'))
                     ) + 1) * avg amout
              , start_date
              , end_date
              , avg
         from deal_record
         having amout > 0 -- 可能会有负数,负数则不符合2018年范围内

         union all

         select product_id
              , '2019'            report
              , datediff(
                        if(end_date <= '2019-12-31', end_date, date('2019-12-31'))
                    , if(start_date >= '2019-01-01', start_date, date('2019-01-01'))
                    ) + 1         diff
              , (datediff(
                         if(end_date <= '2019-12-31', end_date, date('2019-12-31'))
                     , if(start_date >= '2019-01-01', start_date, date('2019-01-01'))
                     ) + 1) * avg amout
              , start_date
              , end_date
              , avg
         from deal_record
         having amout > 0

         union all

         select product_id
              , '2020'                      report
              , (datediff(
                         if(end_date <= '2020-12-31', end_date, date('2020-12-31'))
                     , if(start_date >= '2020-01-01', start_date, date('2020-01-01'))
                     ) + 1)                 diff
              , datediff(
                        if(end_date <= '2020-12-31', end_date, date('2020-12-31'))
                    , if(start_date >= '2020-01-01', start_date, date('2020-01-01'))
                    ) + 1 * deal_record.avg amout
              , start_date
              , end_date
              , avg
         from deal_record
         having amout > 0) t1
         left join product_detal t2
                   on t1.product_id = t2.product_id
group by t1.product_id, product_name, report
order by t1.product_id, report 结果： 1,xiaomi,2019,3500
2,apple ,2018,310
2,apple,2019,3650
2,apple,2020,10
3,vivo,2019,31
3,vivo,2020,31
SQL34 周内每天销售情况
34.1 题目需求
查询周内每天每个商品类别售卖了多少件
34.2 表结构
商品明细表
字段名	字段类型	字段含义
product_id	String 商品id
product_name	String 商品名称
category	String 商品类别
交易记录表
字段名	字段类型	字段含义
order_id	String 订单id
order_date	date 商品销售起始日期
product_id	String 商品id
num	int 商品售卖件数
34.3 建表/插数语句
1）建表
DROP TABLE IF EXISTS order_detail2;
CREATE TABLE order_detail2
(
    `order_id`  varchar(32) NOT NULL,
    `id`        varchar(32) COMMENT '商品id',
    `price`     int,
    `num`       int,
    `sale_date` date
) COMMENT '订单明细表2'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail2';
DROP TABLE IF EXISTS product_detail_2;
CREATE TABLE product_detail_2
(
    `product_id`   varchar(32),
    `product_name` varchar(32),
    `category`     varchar(32)
) COMMENT '产品明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_detail_2';
2
）插入数据
INSERT INTO order_detail2
VALUES ('1', '1', 80, 8, '2021-12-29'),
       ('2', '1', 10, 1, '2021-12-30'),
       ('3', '2', 55, 5, '2021-04-30'),
       ('3', '2', 55, 5, '2021-04-30'),
       ('4', '3', 550, 10, '2021-03-31'),
       ('5', '4', 550, 15, '2021-05-04'),
       ('6', '2', 30, 3, '2021-08-07'),
       ('7', '2', 60, 6, '2020-08-09'),
       ('8', '4', 550, 15, '2021-05-05');
INSERT INTO product_detail_2
VALUES ('1', 'bingxiang', 'dianqi'),
       ('2', 'xiyiji', 'dianqi'),
       ('3', 'xiaomi', 'phone'),
       ('4', 'apple', 'phone'),
       ('5', 'dami', 'food'),
       ('6', 'kuzi', 'cloth');
34.4
代码实现
select b.category,
       nvl(sum(case when dayofweek(a.order_date) = 2 then a.num end), 0) Monday,
       nvl(sum(case when dayofweek(a.order_date) = 3 then a.num end), 0) Tuesday,
       nvl(sum(case when dayofweek(a.order_date) = 4 then a.num end), 0) Wednesday,
       nvl(sum(case when dayofweek(a.order_date) = 5 then a.num end), 0) Thursday,
       nvl(sum(case when dayofweek(a.order_date) = 6 then a.num end), 0) Friday,
       nvl(sum(case when dayofweek(a.order_date) = 7 then a.num end), 0) Saturday,
       nvl(sum(case when dayofweek(a.order_date) = 1 then a.num end), 0) Sunday
from order_detail_2 a
         right join product_detail_2 b -- 没有销售的产品也需要列出,所以以产品表为基表
                    on a.product_id = b.product_id
group by b.category
;
结果：
| Category   | Monday    | Tuesday   | Wednesday | Thursday  | Friday    | Saturday  | Sunday    |
+------------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
| dianqi     | 20        | 5         | 0         | 0         | 10        | 0         | 0         |
| phone      | 0         | 0         | 0         | 0         | 5         | 0         | 0         |
| food       | 0         | 0         | 5         | 1         | 0         | 0         | 10        |
| cloth      | 0         | 0         | 0         | 0         | 0         | 0         | 0         |
SQL35
查看每件商品的售价涨幅情况
35.1
题目需求
查询每件商品的涨幅情况，按照涨幅升序排序。
35.2
表结构
商品表
字段名	字段类型	字段含义
product_id	String
商品id
product_name	String
商品名称
category	String
商品类别
date	date
上架日期

商品售价变化明细表
字段名	字段类型	字段含义
product_id	String
商品id
price	String
商品价格
start_date	date
起始日期
end_date	date
结束日期
35.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS price_change;
CREATE TABLE price_change
(
    `product_id` varchar(32),
    `price`      varchar(32),
    `start_date` date,
    `end_date`   date
) COMMENT '商品售价变化明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/price_changel';
2
）插入数据
INSERT INTO price_change
VALUES ('1', '5000', '2020-01-01', '2020-01-01'),
       ('1', '4500', '2020-01-01', '9999-01-01'),
       ('2', '6000', '2020-02-01', '9999-01-01'),
       ('3', '3000', '2020-03-01', '2020-03-08'),
       ('3', '4000', '2020-03-08', '9999-01-01');
35.4
代码实现
select product_id
     , growth
     , `date`
from (select product_id
           , price
           , start_date
           , end_date
           , last_value(price)
                        over (partition by product_id order by start_date rows between unbounded preceding and unbounded following)
        - first_value(price)
                      over (partition by product_id order by start_date rows between unbounded preceding and unbounded following) growth
           , lag(end_date, 1, '9999-01-01') over (partition by product_id order by start_date)                                    `date`
           , row_number() over (partition by product_id order by end_date desc )                                                  rk
      from price_change) t1
where rk = 1
;
结果：
product_id	growth	date
1			-500		2020-01-01
2			0		9999-01-01
3			1000	2020-03-08
SQL36
销售订单首购和次购分析
36.1
题目需求
分析如果有一个用户成功下单两个及两个以上的购买成功的手机订单（购买商品为xiaomi
，apple
，vivo
）那么输出这个用户的id
及第一次成功购买手机的日期和第二次成功购买手机的日期，以及购买手机成功的次数。
36.2
表结构
订单信息表
字段名	字段类型	字段含义
order_id	String
商品id
user_id	String
用户id
product_name	String
商品名称
status	String
是否成功购买
price	String
商品价格
date	date
购买日期
36.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_info_2;
CREATE TABLE order_info_2
(
    `order_id`     varchar(32),
    `user_id`      varchar(32),
    `product_name` varchar(32),
    `status`       varchar(32),
    `date`         date
) COMMENT '订单详情表2'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_info_2';
2
）插入数据
INSERT INTO order_info_2
VALUES ('1', '100001', 'xiaomi', 'fail', '2021-01-01'),
       ('2', '100002', 'apple', 'success', '2021-01-02'),
       ('3', '100003', 'xiyiji', 'success', '2021-01-03'),
       ('4', '100003', 'xiaomi', 'success', '2021-01-04'),
       ('5', '100001', 'vivo', 'success', '2021-01-03'),
       ('6', '100003', 'vivo', 'success', '2021-01-08'),
       ('8', '100001', 'apple', 'success', '2021-01-06'),
       ('7', '100001', 'xiaomi', 'success', '2021-01-05');
36.4
代码实现
select user_id
     , first
     , date second
     , cnt
from (select *
           , count(1) over (partition by user_id)                   cnt
           , lag(date, 1) over (partition by user_id order by date) first
           , row_number() over (partition by user_id order by date) rk
      from order_info_2
      where product_name in ('xiaomi', 'apple', 'vivo')
        and status = 'success') t1
where cnt >= 2
  and rk = 2
;
结果
:
user_id	first_date		second_date	count
100003	2021-01-04	2021-01-08	2
100001	2021-01-03	2021-01-05	3
对于用户100001
来说，分别在订单1/5/7/8
进行购买，其中578
成功，且均为手机订单。
对于用户100002
来说，只有一次购买手机，不计入
对于用户100003
来说，订单3/4/6
购买，只有46
为手机订单，进行输出
SQL37
同期商品售卖分析表
37.1
题目需求
现在有各个商品的当天售卖明细明细表
需要求出同一个商品在2021
年和2022
年中同一个月的售卖情况对比
37.2
表结构
商品当天售卖明细表
字段名	字段类型	字段含义
order_id	String
商品id
product_name	String
商品名称
num	int
商品售卖件数
date	date
购买日期
37.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS product_detail_3;
CREATE TABLE product_detail_3
(
    `order_id`     varchar(32),
    `product_name` varchar(32),
    `num`          int,
    `date`         date
) COMMENT '商品明细表3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_detail_3';
2
）插入数据
INSERT INTO product_detail_3
VALUES ('1', 'xiaomi', 53, '2021-01-02'),
       ('2', 'apple', 23, '2021-01-02'),
       ('3', 'vivo', 12, '2021-01-02'),
       ('4', 'xiaomi', 54, '2021-01-03'),
       ('5', 'apple', 43, '2021-01-03'),
       ('6', 'vivo', 41, '2021-01-03'),
       ('7', 'vivo', 24, '2021-02-03'),
       ('8', 'xiaomi', 23, '2021-02-03'),
       ('9', 'apple', 34, '2021-02-03'),
       ('10', 'vivo', 42, '2021-02-04'),
       ('11', 'xiaomi', 45, '2021-02-04'),
       ('12', 'apple', 59, '2021-02-04'),
       ('13', 'xiaomi', 230, '2022-01-04'),
       ('14', 'vivo', 764, '2022-01-04'),
       ('15', 'apple', 644, '2022-01-04'),
       ('16', 'xiaomi', 240, '2022-01-06'),
       ('17', 'vivo', 714, '2022-01-06'),
       ('18', 'apple', 624, '2022-01-06'),
       ('19', 'xiaomi', 260, '2022-01-04'),
       ('20', 'vivo', 721, '2022-02-14'),
       ('21', 'apple', 321, '2022-02-14'),
       ('22', 'xiaomi', 134, '2022-02-14'),
       ('23', 'vivo', 928, '2022-02-24'),
       ('24', 'apple', 525, '2022-02-24'),
       ('25', 'xiaomi', 231, '2020-02-06');
37.4
代码实现
select t1.product_name
     , t1.ym
     , t1.total
     , t2.ym
     , t2.total
from (select product_name
           , date_format(date, '%Y-%m') `ym`
           , sum(num)                   total
           , max(month(date))           mn
      from product_detail_3
      where year(date) = '2021'
      group by product_name, date_format(date, '%Y-%m')) t1
         join (select product_name
                    , date_format(date, '%Y-%m') `ym`
                    , sum(num)                   total
                    , max(month(date))           mn
               from product_detail_3
               where year(date) = '2022'
               group by product_name, date_format(date, '%Y-%m')) t2
              on t1.product_name = t2.product_name
                  and t1.mn = t2.mn
;
结果
:
apple,2025-02,93,2026-02,846
vivo,2025-02,66,2026-02,1649
xiaomi,2025-02,68,2026-02,394
apple,2025-01,66,2026-01,1268
vivo,2025-01,53,2026-01,1478
xiaomi,2025-01,107,2026-01,470
SQL38
库存最多的商品
38.1
题目需求
38.2
表结构
商品库存明细表
字段名	字段类型	字段含义
id	String
商品id
date	date
变化时间
action	String
补货或者是售货
amount	int
补货或者售货数量
38.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS product_detail;
CREATE TABLE product_detail
(
    `id`     varchar(32),
    `date`   date,
    `action` varchar(32),
    `amount` int
) COMMENT '商品品明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_detail';
2
）插入数据
INSERT INTO product_detail
VALUES ('1', '2021-01-01', 'supply', 2000),
       ('1', '2021-01-03', 'sell', 1000),
       ('1', '2021-01-05', 'supply', 3000),
       ('2', '2021-01-01', 'supply', 7000),
       ('2', '2021-01-01', 'supply', 1000),
       ('2', '2021-01-04', 'sell', 8000),
       ('3', '2021-01-01', 'supply', 4000),
       ('4', '2021-01-01', 'supply', 3000),
       ('4', '2021-01-03', 'supply', 1000),
       ('5', '2021-01-01', 'supply', 2000);
38.4
代码实现
select *
from (select id
           , agg
           , rank() over (order by agg desc) rk
      from (select id
                 , sum(agg) agg
            from (select id
                       , if(action = 'supply', amount, -amount) agg
                  from product_detail) t1
            group by id) t2) t3
where rk = 1
;
结果
:
id	result
1	4000
3	4000
4	4000
SQL39
国庆期间每个品类的商品的收藏量和购买量
39.1
题目需求
请统计国庆前三天的每一天的最近一周的每个品类下商品收藏量和购买量
假设前三天每天的最近一周都有记录
39.2
表结构
商品收藏明细表
字段名	字段类型	字段含义
id	String
商品id
user_id	String
收藏用户id
date	date
时间

商品购买明细表
 字段名	字段类型	字段含义
id	String
商品id
user_id	String
购买用户id
date	date
时间

商品属性表
字段名	字段类型	字段含义
id	String
商品id
name	String
商品名字
category	String
商品品类名
39.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS product_prop;
CREATE TABLE product_prop
(
    `id`       varchar(32),
    `name`     varchar(32),
    `category` varchar(32)
) COMMENT '商品属性表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_prop';
DROP TABLE IF EXISTS product_purchase;
CREATE TABLE product_purchase
(
    `id`      varchar(32),
    `user_id` varchar(32),
    `date`    date
) COMMENT '商品购买明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_purchase';
DROP TABLE IF EXISTS product_favor;
CREATE TABLE product_favor
(
    `id`      varchar(32),
    `user_id` varchar(32),
    `date`    date
) COMMENT '商品收藏明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/product_favor';
2
）插入数据
INSERT INTO product_prop
VALUES ('1', 'xiaomi', 'phone'),
       ('2', 'vivo', 'phone'),
       ('3', 'apple', 'phone'),
       ('4', 'dami', 'food'),
       ('5', 'kuzi', 'cloth');
INSERT INTO product_purchase
VALUES ('1', '1002', '2021-09-25'),
       ('1', '1003', '2021-09-27'),
       ('1', '1006', '2021-10-01'),
       ('1', '1007', '2021-10-03'),
       ('2', '1001', '2021-09-24'),
       ('2', '1002', '2021-09-25'),
       ('2', '1003', '2021-09-27'),
       ('2', '1005', '2021-09-30'),
       ('2', '1006', '2021-10-01'),
       ('2', '1007', '2021-10-02'),
       ('3', '1001', '2021-09-24'),
       ('3', '1002', '2021-09-25'),
       ('3', '1003', '2021-09-29'),
       ('3', '1005', '2021-09-30'),
       ('3', '1006', '2021-10-01'),
       ('3', '1007', '2021-10-02'),
       ('4', '1001', '2021-09-24'),
       ('4', '1002', '2021-09-25'),
       ('4', '1003', '2021-09-26'),
       ('5', '1005', '2021-09-27'),
       ('5', '1006', '2021-10-01'),
       ('5', '1008', '2021-10-02'),
       ('5', '1007', '2021-10-03');
INSERT INTO product_favor
VALUES ('1', '1001', '2021-09-24'),
       ('1', '1002', '2021-09-25'),
       ('1', '1003', '2021-09-26'),
       ('1', '1005', '2021-09-30'),
       ('1', '1006', '2021-10-01'),
       ('1', '1007', '2021-10-03'),
       ('2', '1001', '2021-09-24'),
       ('2', '1002', '2021-09-25'),
       ('2', '1003', '2021-09-26'),
       ('2', '1005', '2021-09-30'),
       ('2', '1006', '2021-10-01'),
       ('2', '1007', '2021-10-02'),
       ('3', '1001', '2021-09-24'),
       ('3', '1002', '2021-09-25'),
       ('3', '1003', '2021-09-26'),
       ('3', '1005', '2021-09-30'),
       ('3', '1006', '2021-10-01'),
       ('3', '1007', '2021-10-02'),
       ('4', '1001', '2021-09-24'),
       ('4', '1002', '2021-09-25'),
       ('4', '1003', '2021-09-26'),
       ('5', '1005', '2021-09-27'),
       ('5', '1006', '2021-10-01'),
       ('5', '1007', '2021-10-03');
39.4
代码实现
with favor as ( -- 初步获取收藏次数
    select category
         , favor_date
         , favor_cnt
         , sum(favor_cnt)
               over (partition by category order by favor_date range between interval '6' day preceding and current row ) favor_total

    from (select category
               , favor_date
               , count(1) favor_cnt
          from (select date favor_date
                     , category
                from product_favor t4
                         join product_prop t3
                              on t4.id = t3.id) t7
          group by category, favor_date) t8)
   , dic as ( -- 解决"cloth等品类没有在国庆前三天有操作,导致结果表的cloth品类没有10-1这个日期"的问题
    select *
    from (select distinct category
          from product_prop) t1,
         (select '2021-10-01' dt
          union all
          select '2021-10-02' dt
          union all
          select '2021-10-03' dt) t0)
   , t1 as ( -- 第一步
    select dic.category
         , dt
         , nvl(favor_total, 0) favor_total
         , nvl(favor_cnt, 0)   favor_cnt
    from dic
             left join favor
                       on dic.category = favor.category
                           and dic.dt = favor.favor_date)
   , favor_res as ( -- 第二步  因为没有full join  所以通过union方式关联,purchase表同理
    select category, dt as favor_date, favor_cnt, favor_total
    from t1
    union
    select category, favor_date, favor_cnt, favor_total
    from favor)
   , favor_final as ( -- 将前三天的日期补充进来后重算total,此步可以优化
    select category
         , favor_date
         , favor_cnt
         , favor_total
         , sum(favor_cnt)
               over (partition by category order by str_to_date(favor_date, '%Y-%m-%d') range between interval '6' day preceding and current row ) favor_final

    from favor_res)
   , purchase as (select category
                       , purchase_date
                       , puchase_cnt
                       , sum(puchase_cnt)
                             over (partition by category order by purchase_date range between interval '6' day preceding and current row ) puchase_total
                  from (select category
                             , purchase_date
                             , count(1) puchase_cnt
                        from (select date purchase_date
                                   , category
                              from product_purchase t1
                                       join product_prop t2
                                            on t1.id = t2.id) t5
                        group by category, purchase_date) t6)
   , t2 as (select dic.category
                 , dt
                 , nvl(puchase_total, 0) puchase_total
                 , nvl(puchase_cnt, 0)   puchase_cnt
            from dic
                     left join purchase
                               on dic.category = purchase.category
                                   and dic.dt = purchase.purchase_date)
   , purchase_res as (select category, dt as purchase_date, puchase_cnt, puchase_total
                      from t2
                      union
                      select category, purchase_date, puchase_cnt, puchase_total
                      from purchase)
   , purchase_final as (select category
                             , purchase_date
                             , puchase_cnt
                             , puchase_total
                             , sum(puchase_cnt)
                                   over (partition by category order by str_to_date(purchase_date, '%Y-%m-%d') range between interval '6' day preceding and current row ) puchase_final
                             -- order字段必须是date类型
                        from purchase_res)
select a1.category
     , purchase_date
     , favor_final
     , puchase_final
from favor_final a1
         left join purchase_final a2
                   on a1.category = a2.category
                       and a1.favor_date = a2.purchase_date
where purchase_date between '2021-10-01' and '2021-10-03'
;
结果：
category	date			favourite	buy
phone	2021-10-01	12		11
phone	2021-10-02	11		10
phone	2021-10-03	9		11
food		2021-10-01	2		2
food		2021-10-02	1		1
food		2021-10-03	0		0
cloth		2021-10-01	2		2
cloth		2021-10-02	2		3
cloth		2021-10-03	3		4
SQL40
每个商品同一时刻最多浏览人数
40.1
题目需求
统计每个商品同一时刻最多的在浏览人数，如果同一时刻有进入也有离开，先记录用户数增加再记录减少，按照最大的人数降序排序。
40.2
表结构
用户行为日志表
字段名	字段类型	字段含义
user_id	String
用户id
id	String
商品id
start_time	timestamp
起始时间
end_time	timestamp
起始时间
40.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_action_log;
CREATE TABLE user_action_log
(
    `user_id`    varchar(32),
    `id`         varchar(32),
    `start_time` timestamp NULL,
    `end_time`   timestamp NULL
) COMMENT '用户行为日志表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_action_log';
2
）插入数据
INSERT INTO user_action_log
VALUES ('101', '9001', '2021-11-01 02:00:00', '2021-11-01 02:00:11'),
       ('102', '9001', '2021-11-01 02:00:09', '2021-11-01 02:00:38'),
       ('103', '9001', '2021-11-01 02:00:28', '2021-11-01 02:00:58'),
       ('104', '9002', '2021-11-01 03:00:45', '2021-11-01 03:01:11'),
       ('105', '9001', '2021-11-01 02:00:51', '2021-11-01 02:00:59'),
       ('106', '9002', '2021-11-01 03:00:55', '2021-11-01 03:01:24'),
       ('107', '9001', '2021-11-01 02:00:01', '2021-11-01 02:01:50');
40.4
代码实现
select id
     , max(cnt) cnt
from (select id
           , sum(flag) over (partition by id order by `time`) cnt
      from (select id
                 , start_time `time`
                 , 1          flag
            from user_action_log
            union
            select id
                 , end_time `time`
                 , -1       flag
            from user_action_log) t1) t2
group by id
order by cnt desc
;
结果：
id		max_uv
9001	3
9002	2
SQL41
统计活跃间隔对用户分级结果
41.1
题目需求
用户等级：
忠实用户：近7
天活跃且非新用户
新晋用户：近7
天新增
沉睡用户：近7
天未活跃但是在7
天前活跃
流失用户：近30
天未活跃但是在30
天前活跃
假设今天是数据中所有日期的最大值
41.2
表结构
用户行为日志表
字段名	字段类型	字段含义
user_id	String
用户id
id	String
商品id
start_time	timestamp
起始时间
end_time	timestamp
起始时间
41.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_action_log_2;
CREATE TABLE user_action_log_2
(
    `user_id`    varchar(32),
    `id`         varchar(32),
    `start_time` timestamp NULL,
    `end_time`   timestamp NULL
) COMMENT '用户行为日志表2'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_action_log_2';
2
）插入数据
INSERT INTO user_action_log_2
VALUES ('109', ' 9001', '2021-08-31 02:00:00', '2021-08-31 02:00:09'),
       ('109', ' 9002', '2021-11-04 03:00:55', '2021-11-04 03:00:59'),
       ('108', ' 9001', '2021-09-01 02:00:01', '2021-09-01 02:01:50'),
       ('108', ' 9001', '2021-11-03 02:00:01', '2021-11-03 02:01:50'),
       ('104', ' 9001', '2021-11-02 02:00:28', '2021-11-02 02:00:50'),
       ('104', ' 9003', '2021-09-03 03:00:45', '2021-09-03 03:00:55'),
       ('105', ' 9003', '2021-11-03 03:00:53', '2021-11-03 03:00:59'),
       ('102', ' 9001', '2021-10-30 02:00:00', '2021-10-30 02:00:09'),
       ('103', ' 9001', '2021-10-21 02:00:00', '2021-10-21 02:00:09'),
       ('101', ' 9001', '2021-10-01 02:00:00', '2021-10-01 02:00:42');
41.4
代码实现
select level                                                                        user_grade
     , round(count(1) / (select count(distinct user_id) from user_action_log_2), 2) ratio
from (select user_id
           , case
                 when (max(start_time) < date_sub(today, 30))
                     then '流失用户'-- 最近登录时间三十天前
                 when (min(start_time) < date_sub(today, 7) and max(start_time) > date_sub(today, 7))
                     then '忠实用户' -- 最早登陆时间是七天前,并且最近七天登录过
                 when (min(start_time) > date_sub(today, 7))
                     then '新增用户' -- 最早登录时间是七天内
                 when (min(start_time) < date_sub(today, 7) and max(start_time) < date_sub(today, 7))
                     then '沉睡用户'-- 最早登陆时间是七天前,最大登录时间也是七天前

        end level
      from user_action_log_2
               left join (select max(end_time) today
                          from user_action_log_2) t1
                         on 1 = 1
      group by user_id, today) t2
group by level
;
结果：
user_grade	ratio
忠实用户		0.43
新晋用户		0.29
沉睡用户		0.14
流失用户		0.14
解释：
今天是2021.11.04
，根据分级，忠实用户有109,108,104
，新晋用户为105,102
，沉睡用户为103
，流失用户为101
，一共7
个用户，则比例可输出。
SQL42
连续签到领金币数
42.1
题目需求
用户每天签到可以领1
金币，并可以累计签到天数，连续签到的第3
、7
天分别可以额外领2
和6
金币。
每连续签到7
天重新累积签到天数。
计算用户从2021
年7
月以来每个月获得的金币数，结果按照月份、ID
升序排序。
42.2
表结构
订单明细表
字段名	字段类型	字段含义
user_id	String
用户id
id	String
商品id
start_time	timestamp
起始时间
end_time	timestamp
起始时间
sign	int
是否签到，1
为签到，0
为未签到
42.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_sign;
CREATE TABLE user_sign
(
    `user_id`    varchar(32),
    `id`         varchar(32),
    `start_time` timestamp NULL,
    `end_time`   timestamp NULL,
    `sign`       int
) COMMENT '用户签到表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_sign';
2
）插入数据
INSERT INTO user_sign
VALUES ('101', '0', '2021-07-07 02:00:00', '2021-07-07 02:00:09', 1),
       ('101', '0', '2021-07-08 02:00:00', '2021-07-08 02:00:09', 1),
       ('101', '0', '2021-07-09 02:00:00', '2021-07-09 02:00:42', 1),
       ('101', '0', '2021-07-10 02:00:00', '2021-07-10 02:00:09', 1),
       ('101', '0', '2021-07-11 15:59:55', '2021-07-11 15:59:59', 1),
       ('101', '0', '2021-07-12 02:00:28', '2021-07-12 02:00:50', 1),
       ('101', '0', '2021-07-13 02:00:28', '2021-07-13 02:00:50', 1),
       ('102', '0', '2021-10-01 02:00:28', '2021-10-01 02:00:50', 1),
       ('102', '0', '2021-10-02 02:00:01', '2021-10-02 02:01:50', 1),
       ('102', '0', '2021-10-03 03:00:55', '2021-10-03 03:00:59', 1),
       ('102', '0', '2021-10-04 03:00:45', '2021-10-04 03:00:55', 0),
       ('102', '0', '2021-10-05 03:00:53', '2021-10-05 03:00:59', 1),
       ('102', '0', '2021-10-06 03:00:45', '2021-10-06 03:00:55', 1);
42.4
代码实现
select user_id
     , sum(coin)
from (select *
           , case
                 when ctn = 7 then 7
                 when ctn = 3 then 3
                 else 1
        end coin
      from (select user_id
                 , login
                 , row_number() over (partition by user_id,flag order by login) ctn
            from (select user_id
                       , date_format(start_time, '%Y-%m-%d')                                                         login
                       , row_number()
                        over (partition by user_id order by date_format(start_time, '%Y-%m-%d') )                    rk
                       , date_sub(date_format(start_time, '%Y-%m-%d'),
                                  row_number()
                                          over (partition by user_id order by date_format(start_time, '%Y-%m-%d') )) flag
                  from user_sign
                  where sign = 1) t1) t2) t3
group by user_id
;
结果：
user_id	month	coin
101		202107	15
102		202110	7
SQL43
统计2021
年10
月每个退货率不大于0.5
的商品各项指标
43.1
题目需求
请统计2021
年10
月每个有展示记录的退货率不大于0.5
的商品各项指标
商品点展比=
点击数÷展示数；
加购率=
加购数÷点击数；
成单率=
付款数÷加购数；
退货率=
退款数÷付款数，
当分母为0
时整体结果记为0
，结果中各项指标保留3
位小数，并按商品ID
升序排序。
43.2
表结构
用户行为统计表
字段名	字段类型	字段含义
user_id	String
用户id
id	String
商品id
time	timestamp
事件时间
click	int
点击，1
为是
cart	int
购物车，1
为是
payment	int
付款，1
为是
refund	int
退货，1
为是
43.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS user_action_2;
CREATE TABLE user_action_2
(
    `user_id` varchar(32),
    `id`      varchar(32),
    `time`    timestamp NULL,
    `click`   int,
    `cart`    int,
    `payment` int,
    `refund`  int
) COMMENT '用户行为统计表2'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/user_action_2';
2
）插入数据
INSERT INTO user_action_2
VALUES ('101', ' 8001', '2021-10-01 02:00:00', 0, 0, 0, 0),
       ('102', ' 8001', '2021-10-01 02:00:00', 1, 0, 0, 0),
       ('103', ' 8001', '2021-10-01 02:00:00', 1, 1, 0, 0),
       ('104', ' 8001', '2021-10-02 02:00:00', 1, 1, 1, 0),
       ('105', ' 8001', '2021-10-02 02:00:00', 1, 1, 1, 0),
       ('101', ' 8002', '2021-10-03 02:00:00', 1, 1, 1, 0),
       ('109', ' 8001', '2021-10-04 02:00:00', 1, 1, 1, 1);
43.4
代码实现
select id
     , round(cli_cnt / show_cnt, 3) click
     , round(cart_cnt / cli_cnt, 3) cart
     , round(pay_cnt / cart_cnt, 3) pay
     , round(re_cnt / pay_cnt, 3)   refund
from (select id
           , sum(if(refund = 1, 1, 0))  re_cnt
           , sum(if(payment = 1, 1, 0)) pay_cnt

           , sum(if(click = 1, 1, 0))   cli_cnt
           , count(1)                   show_cnt
           , sum(if(cart = 1, 1, 0))    cart_cnt

      from user_action_2
      where month(time) = 10
        and year(time) = 2021
      group by id) t1
;
结果：
id		click		cart		pay		refund
8001	0.833	0.800	0.750	0.333
8002	1.000	1.000	1.000	0.000
解释：
在2021
年10
月商品8001
被展示了6
次，点击了5
次，加购了4
次，付款了3
次，退款了1
次，因此点击率为5/6=0.833
，加购率为4/5=0.800
，
成单率为3/4=0.750
，退货率为1/3=0.333
（保留3
位小数）；
SQL44 10
月的新户客单价和获客成本
44.1
题目需求
请计算2021
年10
月商城里所有新用户的首单平均交易金额（客单价）和平均获客成本（保留一位小数）。
注：订单的优惠金额 =
订单明细里的{
该订单各商品单价×数量之和} -
订单总表里的{
订单总金额}
。
44.2
表结构
商品信息表
字段名	字段类型	字段含义
id	String
商品id
in_price	int
进货价格

订单表
字段名	字段类型	字段含义
order_id	String
订单id
user_id	String
用户id
event_time	date
时间
total_amount	int
订单总额
total_count	int
订单中商品个数

订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
单价价格
count	int
商品个数
44.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail_10;
CREATE TABLE order_detail_10
(
    `order_id` varchar(32),
    `id`       varchar(32),
    `price`    int,
    `count`    int
) COMMENT '订单明细表10'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_10';
2
）插入数据
INSERT INTO order_detail_10
VALUES ('301002', '8001', 85, 1),
       ('301002', '8003', 180, 1),
       ('301003', '8004', 140, 1),
       ('301003', '8003', 180, 1),
       ('301005', '8003', 180, 1),
       ('301006', '8003', 180, 1);
44.4
代码实现
select round(sum(total_amount) / count(1), 1)
     , round(sum(product_total - total_amount) / count(1), 1)
from (select a.order_id
           , user_id
           , total_amount
           , row_number() over (partition by user_id order by event_time ) rk
           , sum(price * `count`) over (partition by b.order_id)           product_total
      from order_detail_10 a
               left join `order` b
                         on a.order_id = b.order_id
      where date_format(event_time, '%Y-%m') = '2021-10') t1
where rk = 1
;
结果：
avg_amount	avg_cost
231.7		23.3
解释：
2021
年10
月有3
个新用户，102
的首单为301002
，订单金额为235
，商品总金额为85+180=265
，优惠金额为30
；
101
的首单为301003
，订单金额为300
，商品总金额为140+180=320
，优惠金额为20
；
104
的首单为301005
，订单金额为160
，商品总金额为180
，优惠金额为20
；
平均首单客单价为(235+300+160)/3=231.7
，平均获客成本为(30+20+20)/3=23.3
SQL45
国庆期间的7
日动销率和滞销率
45.1
题目需求
动销率定义为店铺中一段时间内有销量的商品占当前已上架总商品数的比例（有销量的商品/
已上架总商品数)
。
滞销率定义为店铺中一段时间内没有销量的商品占当前已上架总商品数的比例。（没有销量的商品/
已上架总商品数)
。
只要当天任一店铺有任何商品的销量就输出该天的结果，即使店铺901
当天的动销率为0
。
45.2
表结构
商品信息表
字段名	字段类型	字段含义
id	String
商品id
in_price	int
进货价格

订单表
字段名	字段类型	字段含义
order_id	String
订单id
user_id	String
用户id
event_time	date
时间
total_amount	int
订单总额
total_count	int
订单中商品个数

订单明细表
字段名	字段类型	字段含义
order_id	String
订单id
id	String
商品id
price	int
单价价格
count	int
商品个数
45.3
建表/
插数语句
1
）建表
DROP TABLE IF EXISTS order_detail_11;
CREATE TABLE order_detail_11
(
    `order_id` varchar(32),
    `id`       varchar(32),
    `price`    int,
    `count`    int
) COMMENT '订单明细表11'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_11';
DROP TABLE IF EXISTS order_info_3;
CREATE TABLE order_info_3
(
    `order_id`     varchar(32),
    `user_id`      varchar(32),
    `event_time`   date,
    `total_amount` int,
    `total_count`  int
) COMMENT '订单明细表3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/sdc/rds/order_detail_3';
2
）插入数据
INSERT INTO order_detail_11
VALUES ('301004', '8002', 180, 1),
       ('301005', '8002', 170, 1),
       ('301002', '8001', 85, 1),
       ('301002', '8003', 180, 1),
       ('301003', '8002', 150, 1),
       ('301003', '8003', 180, 1);
INSERT INTO order_info_3
VALUES ('301004', '102', '2021-09-30', 170, 1),
       ('301005', '104', '2021-10-01', 160, 1),
       ('301003', '101', '2021-10-02', 300, 2),
       ('301002', '102', '2021-10-03', 235, 2);
45.4
代码实现
selectect t1.event_time
     , count(distinct prodct_id)
     , count(distinct prodct_id) / (select count(1) from product_info_1)     sale_rate
     , 1 - count(distinct prodct_id) / (select count(1) from product_info_1) nosale_rate
from (select event_time
      from order_info_3
      where event_time >= '2021-10-01'
        and event_time <= '2021-10-03') t1 -- 做出基表
         left join (select id prodct_id
                         , event_time
                    from order_detail_11 t1
                             left join order_info_3 o on t1.order_id = o.order_id) t2 -- 每个下单日期里面的产品id
                   on datediff(t1.event_time, t2.event_time) <= 6 -- 笛卡尔积 基表的每条记录(每天)最近七天合计会有几个产品被卖出
                       and datediff(t1.event_time, t2.event_time) >= 0
group by t1.event_time
;
结果：
dt			sale_rate	unsale_rate
2021-10-01	0.333	0.667
2021-10-02	0.667	0.333
2021-10-03	1.000	0.000
解释：
10
月1
日的近7
日（9
月25
日---10月1日）有销量的商品有8002，截止当天在售商品数为3，动销率为0.333，滞销率为0.667；
10
月2
日的近7
日（9
月26
日---10月2日）有销量的商品有8002、8003，截止当天在售商品数为3，动销率为0.667，滞销率为0.333；
10
月3
日的近7
日（9
月27
日---10月3日）有销量的商品有8002、8003、8001，截止当天店铺901在售商品数为3，动销率为1.000，滞销率为0.000；
