-- PROJECT 2

-- messima 1

go
WITH Annualsales as (
select year(o.orderdate) as 'Year', 
sum(ol.UnitPrice*ol.PickedQuantity) as IncomePerYear, count(distinct(month(o.orderdate))) as NumberOfDistinctMonths,
  case
     when count(distinct(month(o.orderdate))) = 12 then sum(ol.UnitPrice*ol.PickedQuantity)
	 else (sum(ol.UnitPrice*ol.PickedQuantity)/5)*12
end as YearlyLinearIncome
from sales.orders o
join sales.OrderLines ol
on ol.OrderID = o.OrderID
group by year(o.orderdate)
)
select Year, IncomePerYear, NumberOfDistinctMonths, YearlyLinearIncome,
substring
(cast(
case
     when
	     LAG(YearlyLinearIncome) over (order by Year) is not null 
          then (YearlyLinearIncome-LAG(YearlyLinearIncome) over (order by year ))*100.00 / LAG( YearlyLinearIncome) over (order by year)
		  else NULL
end as varchar),1,len(case
     when
	     LAG(YearlyLinearIncome) over (order by Year) is not null 
          then (YearlyLinearIncome-LAG(YearlyLinearIncome) over (order by year ))*100.00 / LAG( YearlyLinearIncome) over (order by year)
		  else NULL
end) - 4) as GrowthRate2
from Annualsales

--messima 2

go

with cte as
(
select year(o.orderdate) as TheYear,
datepart(quarter,o.OrderDate) as TheQuarter,
c.CustomerName ,
sum(il.UnitPrice*il.Quantity) AS IncomePerYear
from sales.Orders O
join sales.Invoices i
on o.OrderID = i.OrderID
join sales.InvoiceLines il
on il.InvoiceID = i.InvoiceID
join sales.Customers c
on c.CustomerID = o.CustomerID
group by year(o.orderdate) , datepart(quarter,o.OrderDate),  c.CustomerName
), CTE2 AS(
select theyear , thequarter, incomeperyear,CustomerName,
RANK() OVER (PARTITION BY THEYEAR, THEquarter ORDER BY INCOMEPERYEAR DESC) AS DNR
from cte 
)
SELECT TheYear, TheQuarter, CustomerName, IncomePerYear, DNR
FROM CTE2
where DNR <= 5
order by TheYear, TheQuarter

--- MESSIMA 3

go

with cte as 
( select s.StockItemID, s.StockItemName, sum(il.ExtendedPrice-il.taxamount) as TotalProfit
   from Warehouse.StockItems s
   join Sales.InvoiceLines il
   on il.StockItemID = s.StockItemID
   group by s.StockItemID, s.StockItemName
)
select StockItemID, StockItemName, TotalProfit
from cte
order by TotalProfit desc
offset 0 rows fetch next 10 rows ONLY


-- MESSIMA 4

go 
with cte as
(
select S.StockItemID, s.StockItemName, s.UnitPrice, s.RecommendedRetailPrice
, (s.RecommendedRetailPrice-s.UnitPrice) as NominalProductProfit
from Warehouse.StockItems S
JOIN Purchasing.Suppliers SP
ON S.SupplierID = SP.SupplierID
WHERE SP.ValidTo > GETDATE()
)
select ROW_NUMBER() over ( order by NominalProductProfit desc ) as Rn, StockItemID, StockItemName, UnitPrice, RecommendedRetailPrice
,NominalProductProfit, DENSE_RANK() over ( order by NominalProductProfit desc) as DNR
from cte

--- messima 5
go
select cast(s.supplierID as varchar) + ' '+'-'+' ' + s.suppliername,
STRING_AGG(CONCAT(st.StockItemID,' ' ,st.StockItemName), ' /, ') AS ProductDetails
from Purchasing.Suppliers s
right join Warehouse.StockItems st
on st.SupplierID = s.SupplierID
group by s.SupplierID, s.SupplierName

--- messima 6

go 
with cte as
(
select c.CustomerID, ci.CityName, co.CountryName, co.Continent, co.Region, il.ExtendedPrice
from Sales.Customers c
right join Application.Cities ci
on ci.CityID = c.PostalCityID
RIGHT join Application.StateProvinces s
on s.StateProvinceID = ci.StateProvinceID
right join Application.Countries co
on co.CountryID = s.CountryID
right join sales.Invoices i
on i.CustomerID = c.CustomerID
right join sales.InvoiceLines il
on il.InvoiceID = i.InvoiceID
), CTE2 AS
(select CustomerID, CityName, CountryName, Continent, Region,
sum(extendedprice) as TotalExtendedPrice
from CTE 
group by CustomerID, CityName, CountryName, Continent, Region
order by TotalExtendedPrice desc
offset 0 rows fetch next 5 rows ONLY
)
SELECT CustomerID, CityName, CountryName, Continent, Region, FORMAT(TotalExtendedPrice, 'N2') AS TotalExtendedPrice
FROM CTE2

-- MESSIMA 7

go 
with cte as
(
select year(o.orderdate) as OrderYear, MONTH(o.orderdate) as OrderMonth,
sum(ol.PickedQuantity*ol.UnitPrice)  as MonthlyTotal
from sales.orders o 
join sales.OrderLines ol
on ol.OrderID = o.OrderID
group by year(o.orderdate), MONTH(o.orderdate)
), cte2 as
(
select OrderYear,
cast(OrderMonth as varchar ) as OrderMonth , MonthlyTotal,
sum(monthlytotal) over ( partition by ( orderyear ) order by ordermonth ) as CumulativeTotal
from cte
)
select OrderYear, OrderMonth, MonthlyTotal, cast( cumulativetotal as varchar)
from cte2

union all
select OrderYear ,'Grand Total' as OrderMonth , sum(monthlytotal) , sum(monthlytotal) 
from cte2
group by OrderYear
order by OrderYear


-- MESSIMA 8
go
select OrderMonth, [2013], [2014], [2015], [2016]
from ( select o.orderid, year(o.orderdate) as OrderYear, month(o.orderdate) as OrderMonth
from sales.Orders o ) p
pivot(count(orderid) for orderyear in ([2013], [2014], [2015], [2016])) PVT
order by OrderMonth


-- MESSIMA 9

go 
with cte as 
( select c.CustomerID, c.CustomerName, o.OrderDate
from sales.Customers c
join sales.Orders o 
on o.CustomerID = c.CustomerID
), cte2 as
( select * , lag(orderdate) over ( partition by customerid order by customerid, orderdate ) as PreviousOrderDate
from cte
), cte3 as
(select max(orderdate) as MaxOrder
 from sales.orders 
 ), cte4 as
 ( select *, datediff(dd, max(OrderDate) over ( partition by customerid), MaxOrder ) as DaysSinceLastOrder, 
 datediff(dd,previousorderdate, orderdate) as diff
from cte2,cte3
) 
select CustomerID, CustomerName, OrderDate, PreviousOrderDate, DaysSinceLastOrder, avg(diff) over ( partition by customerid ) as AvgDaysBetweenOrders,
case when DaysSinceLastOrder > 2* avg(diff) over ( partition by customerid ) then 'Potential Churn'
else 'Active'
end as CustomerStatus
from cte4

-- messima 10

go
with Cte as 
(
select distinct c.CustomerCategoryID,
case when c.CustomerName like 'Wingtip%' then 'Wingtip'
     when c.CustomerName like 'Tailspin%' then 'Tailspin'
     else c.CustomerName
end as CustomerName
from sales.Customers c
), cte2 as 
(
select cc.CustomerCategoryName, count(customername) as CustomerCOUNT
from sales.CustomerCategories cc
join cte 
on cte.customercategoryid = cc.customercategoryid
group by cc.CustomerCategoryName
)
select *,(select count (*) from Cte) as TotalCustCount,
CONCAT( FORMAT (customerCOUNT*100.00/(select count (*) from Cte), '0.00'),'%') as DistributionFactor
FROM cte2
order by CustomerCategoryName
