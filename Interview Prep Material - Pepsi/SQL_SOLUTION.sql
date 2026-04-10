-- Databricks notebook source
-- Pivot sales quaterly
-- We have sales table with Year, Quater and sale colm. pivot sales data on quater and provide sum of sales in each quater


select year, Q1, Q2, Q3, Q4
FROM (select year, Quarter, sale from company_sales)
pivot(
  sum(sale)
  for Quarter in ("Q1", "Q2", "Q3", "Q4")
)


-- COMMAND ----------

--select * from students;

-- Pivot student marks 
-- We have student table with studentid , subject and marks column. pivot subject column for each studentid

select * from 
(select * from students)
pivot(
  max(marks)
  for subject in ("Math", "Eng", "Phy","SC")
)

-- COMMAND ----------

select *
from (select * from students)
pivot(
  max(Marks)
  for subject in ("Math", "Eng", "Phy")
)

-- COMMAND ----------

-- Rolling Sum

-- write sql to find the rolling sum on sales column 
-- column -- year, quater, sales

select year, quarter, sale,
sum(sale) over(partition by year order by Quarter asc rows between unbounded preceding and current row) as roll_sale
from company_sales;

-- COMMAND ----------

-- select a.*,
-- month(a.date) as month,
-- sum(a.sales_amount) over(partition by a.customer_id order by a.date asc rows between unbounded preceding and current row) as rolling
-- from sales_data a 
-- where a.customer_id = 5 

-- Rolling sum for specific customer id 
-- write pysoark to find the rolling sum on sales column 
-- column -- date, customer_id,  sales_amount

select a.*,
month(date) as month,
sum(sales_amount) over(partition by customer_id order by date asc rows between unbounded preceding and current row) as roll
from sales_data a
where a.customer_id = 5

-- COMMAND ----------

select * from person_year;

-- COMMAND ----------


-- write sql query to select person attending event for continuos three year.

-- sample data 
-- pid, year
-- 1, 2019
-- 1, 2020
-- 1, 2021
-- 2, 2022
-- 2, 2021

with dataset as (select pid, year,
lag(year, 1) over(partition by pid order by year asc) as year_1,
lag(year, 2) over(partition by pid order by year asc) as year_2
from person_year)
select * 
from dataset
where year_2 = year - 2 and year_1 = year - 1

-- COMMAND ----------

--select * from cutomer_level;

-- write query to shift data in another column to specific column
--sample data

-- custid, level1, level2, level3 
-- 1, 2, 3 , 1

-- op 
-- 1, 1, 2, 3

select custid,
case when Level1 = 1 or level2 = 1 or level3 = 1 then 1 else 0 end as level1,
case when Level1 = 2 or level2 = 2 or level3 = 2 then 2 else 0 end as level2,
case when Level1 = 3 or level2 = 3 or level3 = 3 then 3 else 0 end as level3
from cutomer_level


-- COMMAND ----------

-- select specific rank from emp and salary table based on salary drawn by employee in specific  department

--table employee -- empid, a.EmpFname, a.Department,
-- table salary  -- empid, b.empposition, b.salary,

with dataset as(select a.EmpFname, a.Department,
b.empposition, b.salary,
dense_rank() over(partition by a.department order by b.salary desc) as sal_rank
from employee a 
inner join salary b
on a.empid = b.empid)
select * from dataset 
where sal_rank <= 2

-- COMMAND ----------

select * from product limit 1;

-- COMMAND ----------

select * from product_sales limit 1;

-- COMMAND ----------



-- write sql query to get max sales for a product for particular year 

-- product table -- product_id , Product_name
-- product_sales table -- sales_id, sale_date, sale_product_id, unitprice, Quantity

with dataset as (select a.product_id,
year(b.sale_date) as sale_year,
b.unitprice * b.Quantity as total_sales
from product a
inner join product_sales b 
on a.product_id = b.sale_product_id)

select product_id, sale_year,
max(total_sales)  as max_sales
from dataset
group by  product_id, sale_year
order by max_sales desc


-- COMMAND ----------

--write query to Interchage M to F and F to M in gender column
-- table columns - id, gender

select id,gender,
case when gender = 'M' then 'F' else 'M' end as op_gen
from gender

-- COMMAND ----------


-- We have country table with teams column as mentioned in below data frame. Create match Schedule wo repeating the game 
-- show all matches between them but it shouldn't have ind-pak match 

with cte as (select 
a.Teams || ' vs ' || b.Teams as match
from  country a
cross join country b 
on 1 = 1
where a.Teams > b.Teams )
select * from cte
where match != 'Ind vs pak ' and match != 'pak  vs Ind'


-- COMMAND ----------

-- code to fetct 50% records from table 

select * from table_name
where rownum <= (select count(*) from table_name)/2;


----COMMAND --------

--If you have 4 different teams and must schedule match between them and cannot be duplicated.  
--Example - Country,  Ind,sl,Aus, Nz and output should be like ind vs sl, ind vs Aus,Ind vsNz likewise other teams and it should not --be duplicated like ind vs ind  
 
Select Distinct c.country as team1, 
                          c1.country as teams2 
From country c join country c1 on c.country < c1.country 

----------------------------------------------------------------------
Orders Table:
OrderID	CustomerID	OrderDate	OrderAmount
  
Customers Table: 
CustomerID	CustomerName	Country

You need to write a SQL query that returns the top 3 highest spending customers from each country along with their total order amounts.
However, you should only consider customers who have placed at least 5 orders.


;
with regcustomers as 
	(
	select customerId 
	from customers
	group by customerId
	having count(1)>=5
	),
	topSpenders as
	(
	select c.CustomerId, c.Country, sum(o.OrderAmount) over (Partition by c.Country, c.CustomerId) as totalOrderAmount  
	from customers c 
	inner join regcustomers r on r.customerId=c.customerId
	inner join orders o on o.customerId=c.customerId
	),
	topRanker as 
	(
	select *, row_number() over (partition by country order by totalOrderAmount desc) as rn
	from topSpenders
	)
	select customerId, totalOrderAmount
	from topRanker where rn<4
------------------------------------------------------------------------------------------------

Given two tables, Employee and Department, generate a summary of how many employees are in each department. Each department should be listed, whether they currently have any employees or not. The results should be sorted from high to low by number of employees, and then alphabetically by department when departments have the same number of employees. The results should list the department name followed by the employee count. The column names are not tested, so use whatever is appropriate.

SELECT 
    Department.department_name,
    COUNT(Employee.employee_id) AS employee_count
FROM 
    Department
LEFT JOIN 
    Employee ON Department.department_id = Employee.department_id
GROUP BY 
    Department.department_name
ORDER BY 
    employee_count DESC, Department.department_name;