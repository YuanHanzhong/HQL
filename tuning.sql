create database tunning;
use tunning;

drop table if exists order_detail;
create table order_detail
(
    id           string comment '订单id',
    user_id      string comment '用户id',
    product_id   string comment '商品id',
    province_id  string comment '省份id',
    create_time  string comment '下单时间',
    product_num  int comment '商品件数',
    total_amount decimal(16, 2) comment '下单金额'
) partitioned by (dt string) row format delimited fields terminated by '\t';

load data local inpath '/opt/data/order_detail.txt' overwrite into table order_detail partition (dt = '2020-06-14');

drop table if exists payment_detail;
create table payment_detail
(
    id              string comment '支付id',
    order_detail_id string comment '订单明细id',
    user_id         string comment '用户id',
    payment_time    string comment '支付时间',
    total_amount    decimal(16, 2) comment '支付金额'
) partitioned by (dt string) row format delimited fields terminated by '\t';
load data local inpath '/opt/data/payment_detail.txt' overwrite into table payment_detail partition (dt = '2020-06-14');
-- 如果直接上传到hdfs，需要用命令修复下关联, 实际上是加了下元数据对应。
msck repair table order_detail;
show partitions order_detail;


drop table if exists product_info;
create table product_info
(
    id           string comment '商品id',
    product_name string comment '商品名称',
    price        decimal(16, 2) comment '价格',
    category_id  string comment '分类id'
) row format delimited fields terminated by '\t';
load data local inpath '/opt/data/product_info.txt' overwrite into table product_info;
drop table if exists province_info;
create table province_info
(
    id            string comment '省份id',
    province_name string comment '省份名称'
) row format delimited fields terminated by '\t';
load data local inpath '/opt/data/province_info.txt' overwrite into table province_info;


-- map端优化
--启用map-side聚合, 这里打开之后，后续会根据设置自动启用map端聚合。
-- 启用后，有可能OOM, 对集群资源要求比较高。不启用则一定能出结果。
set hive.map.aggr=true;

--用于检测源表数据是否适合进行map-side聚合。检测的方法是：先对若干条数据进行map-side聚合，
-- 若聚合后的条数和聚合前的条数比值小于该值，则认为该表适合进行map-side聚合；
-- 否则，认为该表数据不适合进行map-side聚合，后续数据便不再进行map-side聚合。
set hive.map.aggr.hash.min.reduction=0.5;

--用于检测源表是否适合map-side聚合的条数。
set hive.groupby.mapaggr.checkinterval=100000;

--map-side聚合所用的hash table，占用map task堆内存的最大比例，若超出该值，则会对hash table进行一次flush。
set hive.map.aggr.hash.force.flush.memory.threshold=0.9;

-- 查看本次是否有启用map端聚合，可以通过explain看map下是否有group by，有则启用了。
explain
select
    product_id,
    count(*)
from
    order_detail
group by
    product_id;

--
--启动Map Join自动转换
set hive.auto.convert.join=true;
set hive.auto.convert.join=false;
set hive.auto.convert.join;

--一个Common Join operator转为Map Join operator的判断条件,
-- 若该Common Join相关的表中,存在n-1张表的大小总和<=该值,则生成一个Map Join计划,
-- 此时可能存在多种n-1张表的组合均满足该条件,则hive会为每种满足条件的组合均生成一个Map Join计划,
-- 同时还会保留原有的Common Join计划作为后备(back up)计划,实际运行时,优先执行Map Join计划，
-- 若不能执行成功，则启动Common Join后备计划。
set hive.mapjoin.smalltable.filesize=250000;

--开启无条件转Map Join
set hive.auto.convert.join.noconditionaltask=true;

--无条件转Map Join时的小表之和阈值,若一个Common Join operator相关的表中，
-- 存在n-1张表的大小总和<=该值,此时hive便不会再为每种n-1张表的组合均生成Map Join计划,
-- 同时也不会保留Common Join作为后备计划。而是只生成一个最优的Map Join计划。
set hive.auto.convert.join.noconditionaltask.size=10000000;

explain
select
    *
from
    order_detail           od
        join product_info  product on od.product_id = product.id
        join province_info province on od.province_id = province.id;
