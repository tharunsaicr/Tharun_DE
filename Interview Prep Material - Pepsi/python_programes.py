#Prime Number 

start = 1
end = 10

for num in range(start, end+1):
    if num > 1:
        for i in range(2, num):
            if num % i == 0:
                break
        else:
            print(num)

#Fibonacci series 

num1 , num2 = 0, 1

no = 10

print("Fibonacci series :")

for i in range(10):
    
    print(num1, end= ' ')
    res = num1 + num2
    
    num1 , num2 = num2, res
    
#fibo number 

def fibo(n):
    if n <= 1:
        return n
    else:
        return (fibo(n-1) + fibo(n-2))
        
print(fibo(10))

========

#prime number 


start = 1
end = 15

prime = [num for num in range(start, end)
if all (num % i != 0 for i in range(2, num))  ]

print(prime)


=========

sort_list

list1=[76, 23, 45, 12, 54, 9] 
print(list1)

for i in range(0, len(list1)):
    for j in range(i+1 , len(list1)):
        if list1[i] >= list1[j]:
            list1[i] , list1[j] = list1[j] , list1[i]
            
            
print(list1)


#count vowel in string

str1 = "testdata"

vowels = ['a','e','i','o','u']

res1 = dict()

res = list([ch for ch in str1 if ch in vowels])

print(res)

for ch in res:
    res1[ch] = res.count(ch)

print(res1)

#add 2 dictionary 

dict1 = {'a': 5, 'o': 2, 'i': 3}
dict2 = {'b': 5, 'c': 2, 'd': 3}

res = dict(**dict1, **dict2)

print(res)

# Write a python script to below problem 

# If you have 4 different teams and must schedule match between them and cannot be duplicated. 

# Example Country,  Ind,sl,Aus, Nz and output should be like ind vs sl, ind vs Aus,Ind vsNz likewise other teams and it should not be duplicated like ind vs ind 

country = ['ind','ban','sl','pak']

schedule = list()

length = len(country)

for i in range(0 , length):
    for j in range(i+1, length):
        match = country[i] + ' vs ' + country[j]
        schedule.append(match)
        
for match in schedule:
    print(match)
    
    
#Program to count no of occurance of element in list
Array = [1,1,1,5,5,5,2,2] 

res_dict = dict()

for ele in Array:
    res_dict[ele] = Array.count(ele)
    
print(res_dict)

# program to achive
# Input
# Dict= {'ETL': {'Azure': 'ADF' , 'Amazon': 'Glue'}, 
# 'NoSql': {'Azure': 'CosmosDB' , 'Amazon': 'DynamoDB'}, 
# 'Sql': {'Azure': 'AzureSQL' , 'Amazon': 'Amazon RDS'}}
#output : {'ETL-Azure': 'ADF', 'ETL-Amazon': 'Glue', 'NoSql-Azure': 'CosmosDB', 'NoSql-Amazon': 'DynamoDB', 'Sql-Azure': 'AzureSQL', 'Sql-Amazon': 'Amazon RDS'}

Dict= {'ETL': {'Azure': 'ADF' , 'Amazon': 'Glue'}, 
'NoSql': {'Azure': 'CosmosDB' , 'Amazon': 'DynamoDB'}, 
'Sql': {'Azure': 'AzureSQL' , 'Amazon': 'Amazon RDS'}}

res_dict = dict()

for process, work in Dict.items():
    for company, tool in work.items():
        key_value = process + '-' + company
        res_dict[key_value] = tool
        
print(res_dict)

==================

#A = [1,2,1,1,1,2,2,3,4,5,5,5,5], get the unique values from the given list without using built in function 
A = [1,2,1,1,1,2,2,3,4,5,5,5,5]

res = list()

for x in A:
    if x not in res:
        res.append(x)
        
print(res)

============

# Input Files 

# File1: 
# Name|Age 

# Suman, Kumar | 25 
# Rahul, Dravid | 50 

# File2: 
# Name|Age|Gender 

# Suresh, Kumar|45|Male 
# Brinda, Keshav|22|Female 

# Output: 
# Name|Age|Gender 

# Suman, Kumar | 25|NA 
# Rahul, Dravid | 50|NA 
# Suresh, Kumar|45|Male 
# Brinda, Keshav|22|Female 

#below code will read all files under folder and will mergeSchema
df = spark.read
  .option("header", "true")
  .option("mergeSchema", "true")
  .csv("path/*")

=============

# only provide repeated integer from the list
list1 = [1,2,3,3,4,5]

list1 = [1,2,3,3,4,5]


res = dict()
for x in list1:
    key = x
    value = list1.count(x)
    if value > 1:
        res[key] = value
    
print(res)

================

# Input Data
+---------------------------------+------------+
|location                         |product_name|
+---------------------------------+------------+
|Dallas, Chicago, Houston, Chicago|7 up        |
|Dallas, Houston                  |pepsi XL    |
+---------------------------------+------------+
Output : Out1
+------------+--------+
|product_name|     loc|
+------------+--------+
|        7 up|  Dallas|
|        7 up| Chicago|
|        7 up| Houston|
|        7 up| Chicago|
|    pepsi XL|  Dallas|
|    pepsi XL| Houston|
+------------+--------+

Ans :
df = spark.createDataFrame([
    {"product_name": "7 up", "location": "Dallas, Chicago, Houston, Chicago"},
    {"product_name": "pepsi XL", "location": "Dallas, Houston"}
])

df.show(truncate=False)

df1=df.select(col("product_name"), explode(split(col("location"),",")).alias("loc"))
df1.show()

# Output : Out2
convert df1 to below output
+--------+-------------+
|     loc|     prods_ws|
+--------+-------------+
|  Dallas|pepsi XL,7 up|
| Chicago|         7 up|
| Houston|pepsi XL,7 up|
+--------+-------------+

Ans : df1.groupBy("loc").agg(collect_set("product_name").alias("prods")).withColumn("prods_ws",concat_ws(",",col("prods"))).drop("prods").show()
=======================================
# output a list that contains only "a" and "b"
# i/o -> Unsorted_list = ["a","b","f","a","r","f","a","","b","a","f","g","b","r","a","t","h","q","b","","a","d","r"]
# o/p -> out_list = ["a","a","a","a","a","a","b","b","b","b"]

Unsorted_list = ["a","b","f","a","r","f","a","","b","a","f","g","b","r","a","t","h","q","b","","a","d","r"]
a = []
b = []
for i in Unsorted_list:
    if(i=="a"):
        a.append(i)
    elif(i=="b"):
        b.append(i)
c = a+b
print(c)

=========================================
Input: 

ID,Sem,Year,Marks 
1,1,2019,100 
1,2,2019,50 
1,1,2020,60 
1,2,2020,40 

Expected Output: 

ID,Year,tsum
1,2019,150 
1,2020,250 

tmarksDF=df.groupBy(id,year).agg(sum(marks).alias(tmarks)) 

resultDF=tma1rksDF.withColumn('cmarks',sum(tmarks) over(order by year)) 

===================================================
Sales Table 


sales id , sale date ,sale_product_id, unitprice,Quantity 
123, 	2023-01-01,1 , 2 , 50    
321,	2023-02-02 ,3,4, 100     
876,	2023-03-03,1,  2     500 
976,	2024-03-03,2, 4 ,  400  

procuct table  

product_id, Product name 
1 ,A 
2, B 
3, C 

Year wise highest sold product based on (unitprice*Quantity) and write in pyspark 

Year,productName 

2023,A 
2024,B 

joinDF=salesDF.join(prodDF,salesDF[sales_product_ID]=prodDF[product_ID],’inner’) 

tsumDF=joinDF.withColumn(‘tsum’,joinDF[unitprice]*joinDF[Quantity]).withcolumn(‘year’,year(col(sale_date))) 

aggSumDF=tsumDF.groupBy(‘year’,’product_id’).agg(sum(col(tsum)).alias(‘tSumAgg’) 

window=Window.partitionBY(‘year’).orderBy(‘tSumAgg’)) 

resultDF= aggSumDF.withColumn(‘rank’,dense_rank().over(window)).filetr(col(ank)==1) 

===============================================================
2nd way -------->>>>

from pyspark.sql.functions import col 
from pyspark.sql import Window
from pyspark.sql import functions as F

data = [(123,'2023',1,2,50),(321,'2023',3,4,100),(876,'2023',1,2,500),(976,'2024',2,4,400)]

data1 = [(1,'A'),(2,'B'),(3,'C')]

Sales= spark.createDataFrame(data , 'sale_id Integer,sale_date string,sale_product_id Integer,unitprice Integer , Quantity Integer')

Product = spark.createDataFrame(data1 , 'product_id Integer, product_name String')

w = Window.partitionBy(col('sale_date')).orderBy(col('Tota_sale').desc())

res = Sales.join(Product , Sales.sale_product_id == Product.product_id , 'inner')\
      .withColumn('Tota_sale' ,col('unitprice')*col('Quantity') )\
      .withColumn('rank' , F.dense_rank().over(w))\
      .filter(col('rank')==1)\
      .select(col('product_name'),col('Tota_sale'),col('sale_date'))

res.show()
=============================================================================================================================








Find the dataframe distinct count 

Ans - df.groupBy("InvoiceNo").agg(expr("countDistinct(Quantity)")) 

===============================================================
