use NAMES
if OBJECT_ID('dbo.William') is not null drop function William
if OBJECT_ID('dbo.Crows') is not null drop function Crows
go

if object_id('dbo.stock') is not null drop table stock
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

--select * from stock
go
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
