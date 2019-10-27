# 综合实习二

***

<center><b>杨舒文 1600012915</b></center>

<center><b>张智涵 1600013019</b></center>

<center><b>范越 1600012746</b></center>

***

### 任务一：层次数据

实验思路：递归向下查找子结点并累计层数；递归向上查找父结点 

实验代码：

``` sql

create table NameTree
(
	father_name nvarchar(50) not null,
	son_name	nvarchar(50) not null
);
--此处将数据从txt导入NameTree表中

declare @root nvarchar(50) = '大禹';

with subs as
(
  select son_name, 0 as lev
  from NameTree
  where father_name=@root
	and father_name != son_name

  union all

  select C.son_name, P.lev + 1
  from subs as P
    inner join NameTree as C
      on C.father_name = P.son_name
  where C.father_name != C.son_name
)

select distinct * from subs order by lev;
go

declare @root nvarchar(50) = '卜';

with subs as
(
  select father_name
  from NameTree
  where son_name=@root
	and father_name != son_name

  union all

  select C.father_name
  from subs as P
    inner join NameTree as C
      on C.son_name = P.father_name
  where C.father_name != C.son_name
)

select distinct * from subs;
go
```

输出结果：
```
son_name                                           lev
-------------------------------------------------- -----------
姒                                                  0
越                                                  0
鲍                                                  1
窦                                                  1
顾                                                  1
扈                                                  1
嵇                                                  1
计                                                  1
楼                                                  1
欧                                                  1
欧阳                                                 1
夏                                                  1
辛                                                  1
莘                                                  1
余                                                  1
禹                                                  1
曾                                                  1
卜                                                  2
娄                                                  2
夏侯                                                 2
```
(20 行受影响)

father_name
--------------------------------------------------
大禹
风
姬
姒
辛
莘

(6 行受影响)

***

### 任务二(1)：调度的冲突可串行化判定

实验思路，输入某一调度中事务读写数据的顺序，建立优先图（实现为sql table），递归遍历优先图，若找到环路，则输出NO（不是冲突可串行化），否则输出YES（是冲突可串行化）

实验代码：

``` sql
create table graph
(
	edge_num	int	primary key identity,
	prior_tran	nvarchar(10)	not null,
	posterior_tran	nvarchar(10)	not null
);

create table schedule
(
	order_num	int	not null,
	trans_id	nvarchar(10)	not null,
	operation_type	nvarchar(10)	not null,
	data_item	nvarchar(10)	not null,
	constraint PK_SCHEDULE primary key(order_num)
);


insert into schedule --插入调度的顺序
values  (1, 'T1', 'read', 'A'),
		(2, 'T2', 'read', 'A'),
		(3, 'T2', 'write', 'A'),
		(4, 'T1', 'read', 'B'),
		(5, 'T1', 'write', 'A'),
		(6, 'T1', 'write', 'B'),
		(7, 'T2', 'read', 'B'),
		(8, 'T2', 'write', 'B')

insert into graph(prior_tran, posterior_tran)
select distinct S.trans_id, T.trans_id
from schedule S join schedule T on S.data_item = T.data_item
where S.order_num < T.order_num
	and S.trans_id != T.trans_id
		and ((S.operation_type = 'write' and T.operation_type = 'read')
			or (S.operation_type = 'read' and T.operation_type = 'write')
				or (S.operation_type = 'write' and T.operation_type = 'write'))

select * from graph --输出优先图的表格
go

if (select count(*) from graph) = 0
	print 'YES' -- no cycle exists

declare @root nvarchar(10) = (select prior_tran from graph where edge_num = 1);

with subs as --检测环路
(
	
	select prior_tran, posterior_tran, 
		cast('.' + cast(prior_tran as varchar(10)) + '.' as varchar(max)) as path,
		0 as cycle
	from graph
	where prior_tran = @root
	
	union all
	
	select C.prior_tran, C.posterior_tran,
		cast(P.path + cast(C.prior_tran as varchar(10)) + '.' as varchar(max)),
		case when P.path like '%.' + cast(C.prior_tran as varchar(10)) + '.%' then 1 else 0 end
	from subs P inner join graph C on P.posterior_tran = C.prior_tran
		and P.cycle = 0
	
)

select * into paths from subs
select * from paths --输出递归遍历图的表格

if exists (select * from paths
				where cycle = 1)
	print 'NO' -- cycle detected
else
	print 'YES'
	
```

输出结果（两个表格分别表示优先图的结构和递归遍历的结果）：


```

(8 行受影响)

(2 行受影响)
edge_num    prior_tran posterior_tran
----------- ---------- --------------
1           T1         T2
2           T2         T1

(2 行受影响)


(3 行受影响)
prior_tran posterior_tran path   			cycle
---------- -------------- ---------------- -----------
T1         T2             .T1.             0

T2         T1             .T1.T2.          0     

T1         T2             .T1.T2.T1.       1                               

(3 行受影响)

NO

```



***

### 任务二(2)：调度的视图可串行化判定

实验思路：根据原schedule表的调度顺序建立graph表（带标记的优先图），先在全为0标记的优先图中检测环路，若有环路，则直接判断不可串行化；否则，在优先图中加入p标记(p>0)，以每一条p标记的边为起始边检测环路，并规定在检测过程中相同标记的边不能出现在同一路径中。对每一个子图，如果图中没有环路，则直接判断可以串行化；否则继续换下一条p标记边作为起始边。若所有子图全被遍历，则判断不可串行化

实验代码：

``` sql
create table schedule
(
	order_num	int	not null,
	trans_id	nvarchar(10)	not null,
	operation_type	nvarchar(10)	not null,
	data_item	nvarchar(10)	not null,
	constraint PK_SCHEDULE primary key(order_num)
);

create table read_from
(
	from_tran	nvarchar(10)	not null,
	read_tran	nvarchar(10)	not null
);

create table graph
(
	edge_num	int	primary key identity,
	edge_label	int	not null,
	prior_tran	nvarchar(10)	not null,
	posterior_tran	nvarchar(10)	not null
);

create table subgraphs
(
	start_node nvarchar(10) not null,
	edge_label int not null,
	has_circle int not null
);

insert into schedule
values  (1, 'T1', 'read', 'A'),
		(2, 'T2', 'write', 'A'),
		(3, 'T3', 'read', 'A'),
		(4, 'T1', 'write', 'A'),
		(5, 'T3', 'write', 'A')

insert into read_from(from_tran, read_tran)
select distinct S.trans_id, T.trans_id
from schedule S join schedule T on S.data_item = T.data_item
where S.order_num < T.order_num
	and S.trans_id != T.trans_id
		and S.operation_type = 'write' 
			and T.operation_type = 'read'

-- add Tb into the beginning of the schedule
insert into read_from(from_tran, read_tran)
select distinct 'Tb', S.trans_id
from schedule S
where S.operation_type = 'read' 
	and not exists (select *
					from schedule T
					where T.operation_type = 'write'
						and T.order_num < S.order_num)

-- add Tf into the end of the schedule
insert into read_from(from_tran, read_tran)
select distinct S.trans_id, 'Tf'
from schedule S
where S.operation_type = 'write' 
	and not exists (select *
					from schedule T
					where T.operation_type = 'write'
						and T.order_num > S.order_num)

select * from read_from -- 输出read_from表格

insert into graph(edge_label, prior_tran, posterior_tran)
select 0, from_tran, read_tran
from read_from

insert into graph(edge_label, prior_tran, posterior_tran)
select 0, R.read_tran, S.trans_id
from read_from R, schedule S
where R.from_tran = 'Tb'
	and S.operation_type = 'write'
		and S.trans_id != R.read_tran
			and not exists (select *
							from graph
							where prior_tran = R.read_tran 
								and posterior_tran = S.trans_id)

insert into graph(edge_label, prior_tran, posterior_tran)
select 0, S.trans_id, R.from_tran
from read_from R, schedule S
where R.read_tran = 'Tf'
	and S.operation_type = 'write'
		and S.trans_id != R.from_tran
			and not exists (select *
							from graph
							where prior_tran = S.trans_id 
								and posterior_tran = R.from_tran)

declare @root nvarchar(10) = 'Tb';
with subs as --检测环路
(
	
	select prior_tran, posterior_tran, 
		cast('.' + cast(prior_tran as varchar(10)) + '.' as varchar(max)) as path,
		0 as cycle
	from graph
	where prior_tran = @root
	
	union all
	
	select C.prior_tran, C.posterior_tran,
		cast(P.path + cast(C.prior_tran as varchar(10)) + '.' as varchar(max)),
		case when P.path like '%.' + cast(C.prior_tran as varchar(10)) + '.%' then 1 else 0 end
	from subs P inner join graph C on P.posterior_tran = C.prior_tran
		and P.cycle = 0
	
)

select * into paths from subs

if exists (select * from paths
				where cycle = 1)
	print 'NO' -- cycle detected


declare read_cursor cursor for
	select R.from_tran, R.read_tran, S.trans_id
	from read_from R, schedule S
	where R.from_tran != 'Tb' 
		and R.read_tran != 'Tf'
			and S.operation_type = 'write'
				and S.trans_id != R.from_tran
					and S.trans_id != R.read_tran

declare @Ti nvarchar(10), @Tj nvarchar(10), @Tk nvarchar(10)
declare @p int = 1
open read_cursor
fetch next from read_cursor into @Ti, @Tj, @Tk
while @@FETCH_STATUS = 0
	begin
		insert into graph
		values(@p, @Tk, @Ti),
			  (@p, @Tj, @Tk)
		set @p = @p + 1
		fetch next from read_cursor into @Ti, @Tj, @Tk
	end
close read_cursor
deallocate read_cursor

select * from graph

declare circle_cursor cursor for
	select prior_tran, edge_label
	from graph
	where edge_label != 0  --所有标记大于0的边

open circle_cursor
declare @label int, @flag int = 0;
fetch next from circle_cursor into @root, @label;

while @@FETCH_STATUS = 0
begin
	with subs2 as --检测环路
	(
		select prior_tran, posterior_tran, 
			cast('.' + cast(prior_tran as varchar(10)) + '.' as varchar(max)) as path,
			0 as cycle, 
			cast('.' + cast(edge_label as varchar(10)) + '.' as varchar(max)) as label
		from graph
		where prior_tran = @root
			and edge_label = @label
	
		union all
	
		select C.prior_tran, C.posterior_tran,
			cast(P.path + cast(C.prior_tran as varchar(10)) + '.' as varchar(max)),
			case when P.path like '%.' + cast(C.prior_tran as varchar(10)) + '.%' then 1 else 0 end,
			cast(P.label + cast(C.edge_label as varchar(10)) + '.' as varchar(max))
		from subs2 P inner join graph C on P.posterior_tran = C.prior_tran
			and P.cycle = 0
		where C.edge_label != @label
			and (P.label not like '%.' + cast(C.edge_label as varchar(10)) + '.%'
				or C.edge_label = 0) -- 限制相同标记的边不能两次出现（标记为0的边除外）
		
	)

	select * into paths2 from subs2
	select * from paths2

	if not exists (select * from paths2
				where cycle = 1)
		begin
			print 'YES' -- 有子图无环
			set @flag = 1;
		end

	fetch next from circle_cursor into @root, @label;
end

if @flag = 0
	print 'NO'
```

输出结果：
```

(5 行受影响)

(1 行受影响)

(1 行受影响)

(1 行受影响)
from_tran  read_tran
---------- ----------
T2         T3
Tb         T1
T3         Tf

(3 行受影响)

(3 行受影响)

(2 行受影响)

(0 行受影响)

(6 行受影响)

(2 行受影响)

edge_num    edge_label  prior_tran posterior_tran
----------- ----------- ---------- --------------
1           0           T2         T3
2           0           Tb         T1
3           0           T3         Tf
4           0           T1         T2
5           0           T1         T3
6           1           T1         T2
7           1           T3         T1

(7 行受影响)

(3 行受影响)

prior_tran posterior_tran path          cycle       label
---------- -------------- ------------- ------------ -----------
T1         T2             .T1.           0           .1.
T2         T3             .T1.T2.        0           .1.0.
T3         Tf             .T1.T2.T3.     0           .1.0.0.

(3 行受影响)

YES
```

***

### 任务三：序列数据（谈股论金）

实验思路：实现函数计算股票指标：威廉指标（W%R），并实现函数进行K线识别：三只乌鸦

指标定义：威廉指标主要通过分析一段时间内股价最高价、最低价和收盘价之间的关系，来判断股市的超买超卖现象，预测股价中短期的走势。以日威廉指标为例，计算主要利用分析周期内的最高价、最低价以及周期结束的收盘价等三者关系展开的，其计算公式为：

$$
W\%R = \frac{H_n - C}{H_n - L_n} \times 100
$$

其中$C$为当日的收盘价，$H_n$为过去N天内的最高价，$L_n$为过去N天内的最低价，这里取N=14。

威廉指标表示当天的收盘价在过去一段时间里的全部价格范围内所处的相对位置，因此，计算出的W%R值位于0-100之间。越接近0值，表明目前的价位越接近过去14日内的最低价，越接近100值，表明目前的价位越接近过去14日内的最高价。

K线——三只乌鸦含义：三只乌鸦表示三根向下的阴线持续下跌，后市看淡。三只乌鸦出现的特点是K线图连续出现三根阴线，每日收盘价都向下跌并接近每日的最低价位，每日的开盘价都在上一根K线的实体部分之内。三只乌鸦的出现是行情逐渐疲软的象征，说明股票需要及时卖出。

实验代码：

``` sql
create table stock
(
	date_idx	date	not null primary key,
	open_price	decimal(6,2)	not null,
	high_price	decimal(6,2)	not null,
	low_price	decimal(6,2)	not null,
	close_price	decimal(6,2)	not null,
	volume		bigint		not null,
	adjclose_price	decimal(6,2)	not null
);
go

--计算威廉指标：计算周期取14天
create function William(@date_idx date)
returns decimal(6,2)
as
begin
	declare @close decimal(6,2) = (select adjclose_price
									from stock
									where date_idx = @date_idx)
	declare @lowest decimal(6,2) = (select min(low_price)
									from stock
									where date_idx <= @date_idx
										and date_idx > (select dateadd(day, -14, @date_idx)))
	declare @highest decimal(6,2) = (select max(high_price)
									from stock
									where date_idx <= @date_idx
										and date_idx > (select dateadd(day, -14, @date_idx)))
	declare @william decimal(6,2) = (@highest - @close) / (@highest - @lowest) * 100
	return @william
end
go

--判断三只乌鸦是否出现
create function Crows(@date_idx date)
returns bit
as
begin
	declare @flag bit = 0
	declare @open1 decimal(6,2), @close1 decimal(6,2),
			@open2 decimal(6,2), @close2 decimal(6,2),
			@open3 decimal(6,2), @close3 decimal(6,2)

	select @open3 = open_price, @close3 = close_price
	from stock
	where date_idx = @date_idx

	select @open2 = open_price, @close2 = close_price
	from stock
	where date_idx = (select dateadd(day, -1, @date_idx))

	select @open1 = open_price, @close1 = close_price
	from stock
	where date_idx = (select dateadd(day, -2, @date_idx))

	if @open1 > @close1 and @open2 > @close2 and @open3 > @close3
		and @close1 > @close2 and @close2 > @close3
			and @open1 > @open2 and @open2 > @open3
		set @flag = 1

	return @flag
end
go

declare @william decimal(6,2) =  dbo.William('2005-09-22')
print @william

declare @crow bit = dbo.Crows('1970-01-28')
if @crow = 1
	print 'YES'
else
	print 'NO'

```

示例与结果：

利用kp500.csv中的股票数据，计算2005-09-02那一天的威廉指标为75.46，处于20-80区间内，表明市场上多空暂时取得平衡，股票价格处于横盘整理之中，可考虑持股或持币观望。

判断1970-01-28附近是否出现“三只乌鸦”，输出结果为YES，说明行情逐渐疲软，需要及时卖出。