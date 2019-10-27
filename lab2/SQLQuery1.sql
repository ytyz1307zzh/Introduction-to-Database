use NAMES
if object_id('dbo.schedule') is not null drop table schedule
if object_id('dbo.read_from') is not null drop table read_from
if object_id('dbo.graph') is not null drop table graph
if object_id('dbo.temp') is not null drop table temp
if object_id('dbo.paths') is not null drop table paths
if object_id('dbo.paths2') is not null drop table paths2
if object_id('dbo.subgraphs') is not null drop table subgraphs

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
	where edge_label != 0

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
				or C.edge_label = 0)
		
	)

	select * into paths2 from subs2
	select * from paths2

	if not exists (select * from paths2
				where cycle = 1)
		begin
			print 'YES'
			set @flag = 1;
		end

	fetch next from circle_cursor into @root, @label;
end

if @flag = 0
	print 'NO'