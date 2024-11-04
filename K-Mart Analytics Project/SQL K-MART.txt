select * from sales
select * from salesperson
select * from product
select * from store_locations


SET ROWCOUNT 0
----------------- Bottom 3 salespeople--------------------------------------------------------------------------------------------------------

select x.Sales_Team, sum(s.Revenue) as Revenue from salesperson x join sales s on x.Salesteamindex = s.Salesteamindex group by Sales_Team order by Revenue asc OFFSET 0 rows fetch next 3 rows only;
----------Top 3 salespeople-----------------------------------

select x.Sales_Team, sum(s.Revenue) Revenue from salesperson x join sales s on x.Salesteamindex = s.Salesteamindex group by Sales_Team order by Revenue desc OFFSET 0 rows fetch next 3 rows only;

-------------Best performing product category--------------------------------------
select p.product_category, sum(s.Revenue) as Revenue from product p join sales s on p.Productindex = s.Productindex group by Product_Category order by Revenue desc;

------------------Stock increase or decrease-------------------------------------------------------
select p.product_category, count(s.Productindex) as Stock_number from product p join sales s on p.Productindex = s.Productindex group by Product_Category order by Stock_number DESC; 


-----------------------Revenue driver-----------------------------------------------------------------
select Sales_Date, sum(unit_cost) as UnitCost, sum(unit_price) as UnitPrice, sum(revenue) as Revenue from [sales ] group by Sales_Date order by unitcost asc;