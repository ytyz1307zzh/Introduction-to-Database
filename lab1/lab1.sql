if OBJECT_ID('Trans') is not null drop table Trans
if OBJECT_ID('My_Stock') is not null drop table My_Stock
if OBJECT_ID('dbo.cal_profit') is not null drop function cal_profit

create table My_Stock
(
	stock_id		int		not null,
	volume			int		not null,
	avg_price		float	not null,
	profit			int		not null,
	constraint PK_STOCK primary key(stock_id)
);

create table Trans
(
	trans_id		int		not null,
	stock_id		int		not null,
	date			int		not null,	
	price			int		not null,
	amount			int		not null,
	sell_or_buy		nvarchar(10)		not null,
	constraint PK_TRANS primary key(trans_id),
	constraint FK_STOCK foreign key(stock_id) references my_stock(stock_id)
);
go

create function cal_profit(@sell_price int, @sell_amount int, @stock_id int)
returns int
begin
	declare @total_sold int = (select sum(amount) from Trans where stock_id=@stock_id and sell_or_buy='S') - @sell_amount
	declare my_curs cursor for
		select price, amount
		from Trans
		where stock_id=@stock_id and sell_or_buy='B'
	declare @buy_price int, @hold int, @profit int = 0
	open my_curs
	fetch next from my_curs into @buy_price, @hold
	while @@FETCH_STATUS = 0
	begin
		if @hold <= @total_sold -- already sold
			set @total_sold = @total_sold - @hold
		else if (@hold - @total_sold) > @sell_amount -- enough for sale
		 begin
			set @profit = @profit + (@sell_price - @buy_price) * @sell_amount
			return @profit
		 end
		else -- not enough for sale
		 begin
			set @profit = @profit + (@sell_price - @buy_price) * (@hold - @total_sold)
			set @sell_amount = @sell_amount - @hold + @total_sold
			set @total_sold = 0
		 end
		fetch next from my_curs into @buy_price, @hold
	end
	return @profit
end
go

create trigger TRANS_VIEW on Trans
instead of insert as
begin
	declare @stock_id int, @date int, @price int, @amount int, @sb nvarchar(10)
	select @stock_id=stock_id, @date=date, @price=price, @amount=amount, @sb=sell_or_buy from inserted

	if @sb = 'S' and @amount > (select volume
								from My_Stock
								where stock_id=@stock_id) -- invalid trade
	begin
		return
	end

	if not exists (select *
					from My_Stock
					where stock_id=@stock_id) -- new stock
	begin
		insert into My_Stock
		values(@stock_id, @amount, @price, 0)
		insert into Trans select * from inserted
		return
	end

	insert into Trans select * from inserted

	if @sb = 'B'
	begin
		update My_Stock
		set avg_price = (volume * avg_price + @price * @amount) / (volume + @amount),
			volume = volume + @amount
		where stock_id=@stock_id
	end

	else if @sb = 'S'
	begin
		update My_Stock
		set avg_price = (volume * avg_price - @price * @amount) / (volume - @amount),
			volume = volume - @amount,
			profit = profit + dbo.cal_profit(@price, @amount, @stock_id)
		where stock_id=@stock_id
	end
end
go

insert into Trans
values(1, 1, 1, 10, 1000, 'B')
insert into Trans
values(2, 1, 2, 11, 500, 'B')
insert into Trans
values(3, 1, 3, 12, 800, 'S')
insert into Trans
values(4, 1, 4, 12, 1000, 'S')
insert into Trans
values(5, 1, 5, 9, 1000, 'B')
insert into Trans
values(6, 1, 6, 12, 800, 'S')

insert into Trans
values(7, 1, 7, 7, 800, 'S')

go

select * from Trans
select * from My_Stock

declare @stock_id int=1, @sell_amount int=800, @sell_price int=12
declare @total_sold int = (select sum(amount) from Trans where stock_id=@stock_id and sell_or_buy='S') - @sell_amount
	declare my_curs cursor for
		select price, amount
		from Trans
		where stock_id=@stock_id and sell_or_buy='B'
	declare @buy_price int, @hold int, @profit int = 0
	open my_curs
	fetch next from my_curs into @buy_price, @hold
	while @@FETCH_STATUS = 0
	begin
		print @buy_price
		print @hold
		if @hold <= @total_sold -- already sold
		begin
			print 'a'
			set @total_sold = @total_sold - @hold
		end
		else if (@hold - @total_sold) > @sell_amount -- enough for sale
		 begin
			set @profit = @profit + (@sell_price - @buy_price) * @sell_amount
			print 'b'
			print @profit
			break
		 end
		else -- not enough for sale
		 begin
			set @profit = @profit + (@sell_price - @buy_price) * (@hold - @total_sold)
			print 'c'
			print @profit
			set @sell_amount = @sell_amount - @hold + @total_sold
			set @total_sold = 0
		 end
		fetch next from my_curs into @buy_price, @hold
	end
close my_curs
deallocate my_curs