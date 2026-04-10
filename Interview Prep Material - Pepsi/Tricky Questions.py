# Databricks notebook source
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

#We have teams table with country column as mentioned in below data frame. Create match Schedule wo repeating the game 

countries_df = spark.createDataFrame([
    {"country": "Ind"},
    {"country": "Ban"},
    {"country": "SL"},
    {"country": "Pak"}
])


res_df = countries_df.alias("a").crossJoin(countries_df.alias("b")).filter(col("a.country") > col("b.country"))

res_df = res_df.withColumn("match", concat(col("a.country") , lit(" vs "), col("b.country")))\
    .drop(col("a.country"), col("b.country"))

res_df.display()

# COMMAND ----------

countries_df.createOrReplaceTempView("teams") 

# COMMAND ----------

res = spark.sql("""
                select
                a.country || " vs" || b.country
                from teams a 
                cross join teams b
                on 1 = 1
                where a.country > b.country
                """)

display(res)     
                         

# COMMAND ----------

#We have Table 1 and Table 2 with col1 provide the output for inner join

table1 = spark.createDataFrame([
    {"col1": "1"},
    {"col1": "1"},
    {"col1": "2"},
    {"col1": "2"},
    {"col1": "3"},
    {"col1": "4"},
    {"col1": None}
])


table2 = spark.createDataFrame([
    {"col1": "1"},
    {"col1": "1"},
    {"col1": "1"},
    {"col1": "2"},
    {"col1": "2"},
    {"col1": "3"},
    {"col1": "5"},
    {"col1": None}
])

join_res = table1.join(table2, table1.col1 == table2.col1, "inner")

display(join_res)

# COMMAND ----------

# we have product table with product_id, quantity and sale_date write pyspark or sql code to get prev 1 and 2 day sales and rolling sum for product id  ordered by sales date

from pyspark.sql.window import Window

product = spark.createDataFrame([
    {"product_id": "p1", "sale_date": "01-Jan", "quantity": "10"},
    {"product_id": "p1", "sale_date": "03-Jan", "quantity": "200"},
    {"product_id": "p1", "sale_date": "04-Jan", "quantity": "30"},
    {"product_id": "p1", "sale_date": "02-Jan", "quantity": "400"},
    {"product_id": "p1", "sale_date": "05-Jan", "quantity": "50"},
    {"product_id": "p2", "sale_date": "01-Jan", "quantity": "10"},
    {"product_id": "p2", "sale_date": "03-Jan", "quantity": "20"},
    {"product_id": "p2", "sale_date": "02-Jan", "quantity": "30"}    
])

product.display()

window_spec = Window.partitionBy(col("product_id")).orderBy(col("sale_date").asc())

res  = product.withColumn("prev_1", lag(col("quantity"), 1).over(window_spec))\
    .withColumn("prev_2", lag(col("quantity"),2 ).over(window_spec))\
        .withColumn("roll_sum", sum(col("quantity")).over(window_spec))

res.display()
   


# COMMAND ----------

#provide python code to copy list wo effecting the primary list

list1 = [1,2,3]

list2 = list1.copy()

list2.append(4)

print(list1)
print(list2)

# COMMAND ----------

#provide pyspark or sql code to get the movie with more thn 2 actors and actors must be both A1 and A2

bollywood  = spark.createDataFrame([
    {"movie": "m1", "actor": "A1"},
    {"movie": "m1", "actor": "A2"},
    {"movie": "m2", "actor": "A1"},
    {"movie": "m2", "actor": "A2"},
    {"movie": "m3", "actor": "A1"},
    {"movie": "m4", "actor": "A1"},
    {"movie": "m5", "actor": "A1"},
    {"movie": "m5", "actor": "A2"}    
])

bollywood.createOrReplaceTempView("bollywood")

res = spark.sql("""
                select movie, count(actor) as cnt
                from bollywood
                where actor in ("A1", "A2")
                group by movie
                having count(actor) >= 2
                """)
res.display()
            

# COMMAND ----------

#We have sales table with Year Quater and sale colm. pivot sales data on quater and provide sum of sales in each quater

sales_data  = spark.createDataFrame([
    {"Year": "2021", "Quarter": "Q1", "sale": 100},
    {"Year": "2021", "Quarter": "Q1", "sale": 200},
    {"Year": "2021", "Quarter": "Q2", "sale": 300},
    {"Year": "2021", "Quarter": "Q2", "sale": 400},
    {"Year": "2023", "Quarter": "Q1", "sale": 100},
    {"Year": "2023", "Quarter": "Q1", "sale": 200},
    {"Year": "2023", "Quarter": "Q2", "sale": 300},
    {"Year": "2023", "Quarter": "Q2", "sale": 400}    
])

sales_data.createOrReplaceTempView("sales_data1")

res = sales_data.groupBy(col("Year")).pivot("Quarter").sum("sale")

res.display()

                    

# COMMAND ----------



# COMMAND ----------

#We have Employee table with E_Id, E_Name, M_Id. write pyspark or sql code to provide manager name in front of employee. 

Employees  = spark.createDataFrame([
    {"E_Id": 1, "E_Name": "A", "M_Id": ""},
    {"E_Id": 2, "E_Name": "B", "M_Id": 8},
    {"E_Id": 3, "E_Name": "C", "M_Id": 7},
    {"E_Id": 4, "E_Name": "D", "M_Id": 7},
    {"E_Id": 5, "E_Name": "E", "M_Id": 7},
    {"E_Id": 6, "E_Name": "F", "M_Id": 1},
    {"E_Id": 7, "E_Name": "G", "M_Id": 1},
    {"E_Id": 8, "E_Name": "H", "M_Id": 1}    
])

Employees.createOrReplaceTempView("Employees_temp")

res = spark.sql("""
                select a.* , b.E_Name as Manager_Name
                from Employees_temp a
                left join Employees_temp b 
                on a.M_Id = b.E_id
                order by a.E_id asc
                """)

Employees = Employees.alias("a").join(Employees.alias("b"), col("a.M_Id") == col("b.E_Id"), "left")\
            .withColumn("Manager_Name", col("b.E_Name"))\
                .drop(col("b.E_Id"), col("b.E_Name"), col("b.M_Id"))


Employees.display()

res.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select a.* , b.E_Name as manager_name
# MAGIC from Employees_temp a
# MAGIC left join Employees_temp b
# MAGIC on b.E_id = a.M_id
# MAGIC

# COMMAND ----------

# We have student table with studentid , subject and marks column. pivot subject column for each studentid

student_data = spark.sql("select * from students_2_csv")

student_data.groupBy(col("StudentID")).pivot("Subject").sum("Marks").display()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- We have student table with studentid , subject and marks column. pivot subject column with respective marks for each studentid
# MAGIC
# MAGIC select  StudentID,
# MAGIC max(case when Subject = "Math" then marks else null end) as Math,
# MAGIC max(case when Subject = "Eng" then marks else null end) as Eng,
# MAGIC max(case when Subject = "Phy" then marks else null end) as Phy
# MAGIC FROM students_2_csv
# MAGIC group by StudentID

# COMMAND ----------

# MAGIC %sql
# MAGIC --Quarter sale
# MAGIC
# MAGIC select Year,
# MAGIC sum(case when Quarter = 'Q1' then sale else 0 end) as q1,
# MAGIC sum(case when Quarter = 'Q2' then sale else 0 end) as q2,
# MAGIC sum(case when Quarter = 'Q3' then sale else 0 end) as q3,
# MAGIC sum(case when Quarter = 'Q4' then sale else 0 end) as q4
# MAGIC from company_sales_csv
# MAGIC group by Year
# MAGIC order by Year asc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from person_year limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- write sql query to select person attending event for continuos three year.
# MAGIC
# MAGIC -- sample data 
# MAGIC -- pid, year
# MAGIC -- 1, 2019
# MAGIC -- 1, 2020
# MAGIC -- 1, 2021
# MAGIC -- 2, 2022
# MAGIC -- 2, 2021
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC with cte as(
# MAGIC select pid, year,
# MAGIC lag(year, 1) over(partition by pid order by year asc) as pre_1,   
# MAGIC lag(year, 2) over(partition by pid order by year asc) as pre_2
# MAGIC from person_year
# MAGIC )
# MAGIC select * from cte
# MAGIC where year = pre_1 + 1 and year = pre_2 + 2
# MAGIC -- 2003
# MAGIC --204
# MAGIC --205 203  204
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- with cte as(
# MAGIC -- select pid, Year,
# MAGIC -- lag(year,1) over(partition by pid order by year) as prev_one_year,
# MAGIC -- lag(year,2) over(partition by pid order by year) as prev_two_year
# MAGIC -- from person_year
# MAGIC -- )
# MAGIC
# MAGIC -- select * 
# MAGIC -- from cte 
# MAGIC -- where Year = prev_one_year + 1 and year = prev_two_year + 2
# MAGIC

# COMMAND ----------

#write python code to sort list wo using sorting method

list1=[76, 23, 45, 12, 54, 9] 
print(list1)

for i in range(0, len(list1)):
    for j in range(i+1, len(list1)):
        if list1[i] >= list1[j]:
            list1[i], list1[j] = list1[j], list1[i]

print(list1)            
        

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with RECURSIVE empdata as (
# MAGIC
# MAGIC
# MAGIC
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- write query to find 2nd highest sales data in a year
# MAGIC -- sale_id, year, sale
# MAGIC
# MAGIC with cte as (
# MAGIC   select a.*,
# MAGIC   dense_rank() over(partition by a.year order by a.sale desc) as rank_value
# MAGIC   from company_sales a
# MAGIC )
# MAGIC
# MAGIC select * from cte
# MAGIC where rank_value = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select year,
# MAGIC sum(case when quarter = "Q1" then sale else 0 end ) as Q1,
# MAGIC sum(case when quarter = "Q2" then sale else 0 end ) as Q2,
# MAGIC sum(case when quarter = "Q3" then sale else 0 end ) as Q3,
# MAGIC sum(case when quarter = "Q4" then sale else 0 end ) as Q4
# MAGIC from sales
# MAGIC group by Year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select DaysToManufacture,
# MAGIC 0, 1, 2, 3, 4, 5
# MAGIC from (
# MAGIC   select * from production
# MAGIC ) as src
# MAGIC pivot(
# MAGIC   sum(AverageCost),
# MAGIC   for DaysToManufacture in (0, 1, 2, 3, 4, 5)
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from production
# MAGIC where DaysToManufacture in (4)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sales_agg limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC --write sql query to find previous month sales and sale difference between present and previous month. sale difference can be -ve
# MAGIC -- column -- year, month, sales
# MAGIC
# MAGIC select
# MAGIC year, month, sales,
# MAGIC lag(sales, 1) over(partition by year order by month asc)   as previous_sales,
# MAGIC sales - lag(sales, 1 ) over(partition by year order by month asc) as sales_diff
# MAGIC from sales_agg
# MAGIC
# MAGIC -- select year, month, sales,
# MAGIC -- sales - lag(sales,1) over(partition by year order by year, month asc)  as sale_diff
# MAGIC -- from sales_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- write sql to find the rolling sum on sales column 
# MAGIC -- column -- year, month, sales
# MAGIC
# MAGIC select 
# MAGIC year, month, sales,
# MAGIC sum(sales) over(partition by year order by month rows between unbounded preceding and current row)  as roll_sum
# MAGIC from sales_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  
# MAGIC sum(case when sales = 300 then 1 else 0 end) as sale_value,
# MAGIC sum(case when sales > 800 then 1 else 0 end) as sale_value_2,
# MAGIC sum(case when sales > 300 and sales < 800 then 1 else 0 end) as sale_value_3
# MAGIC from sales_agg

# COMMAND ----------



# df_product_sales = df_product_sales.withColumn("year", year(col("sale_date")))

# df_res = df_product.join(df_product_sales, df_product.product_id == df_product_sales.sale_product_id, "inner")\
#   .drop(df_product_sales.sale_product_id, df_product_sales.sale_date, df_product_sales.sales_id)

# df_res = df_res.withColumn("Total_sales", col("unitprice") * col("Quantity"))

# df_res = df_res.drop( col("unitprice") , col("Quantity"))

# df_res = df_res.groupBy(col("product_id"), col("year")).agg(max("Total_sales").alias("max_sale")).orderBy(col("max_sale").desc())

# df_res.display()


# COMMAND ----------

sales =  spark.sql("select * from company_sales")

sales.groupBy("year").pivot("quarter").sum("sale").display()

# COMMAND ----------


# -- write pysoark to find the rolling sum on sales column 
# -- column -- date, customer_id,  sales

sales =  spark.sql("select * from sales_data")

sales = sales.withColumn("month", month(col("date")))

window_spec = Window.partitionBy(col("customer_id")).orderBy(col("date").asc()).rowsBetween(Window.unboundedPreceding, 0)

sales = sales.withColumn("rolling", sum(col("sales_amount")).over(window_spec))

# window_spec = Window.partitionBy(col("customer_id")).orderBy(col("date").desc()).rowsBetween(Window.unboundedPreceding, 0)

# sales = sales.withColumn("rolling", sum(col("sales_amount")).over(window_spec))


sales.display()

# COMMAND ----------

#create spark dataframe from list and show max value of list

my_list = [("apple", 10), ("banana", 20), ("cherry", 30)]

df = spark.createDataFrame(my_list, schema=["fruit", "price"])

res = df.agg(max("price"), min("price"))

display(res)

# COMMAND ----------

#find the max value of sum for any three numbers from the list.
#solution 1
list1 = [13, 2, 4, 6, 7, 10, 12]
list1.sort()
res = sum(list1[-3:])
print(res)

#solution 2
list1 = [13, 2, 4, 6, 7, 10, 12]
list1.sort()
res = [sum(x) for x in list(zip(list1, list1[1:], list1[2:]))]
res = max(res)
print(res)

# COMMAND ----------


-------------------------------------------------------------------------------------
empname   empcity     salary  

group 


df=df.groupBy('Empcity','Salary').count().filter(col('count')>3)


A_ID        Movie_ID
A1          M1
A1          M2
A1          M3
A2          M1
A2          M1
A2          M6
A3          M3
A4          M1
A4          M2
 
find all A_IDs having atleast M1 and M2 Movie_IDs.
 
sample output:
A1
A4
has context menu


select A_ID from table where Movie_ID in('M1','M2') 
group by A_ID having count(distinct Movie_ID)=2
