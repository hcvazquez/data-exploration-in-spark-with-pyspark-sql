# Databricks notebook source
#import SparkSession
from pyspark.sql import SparkSession

# COMMAND ----------

#create spar session object
spark=SparkSession.builder.appName('data_processing').getOrCreate()

# COMMAND ----------

# Load csv Dataset 
df=spark.read.csv('FileStore/tables/sample_data.csv',inferSchema=True,header=True)

# COMMAND ----------

#columns of dataframe
df.columns

# COMMAND ----------

#check number of columns
len(df.columns)

# COMMAND ----------

#number of records in dataframe
df.count()

# COMMAND ----------

#shape of dataset
print((df.count(),len(df.columns)))

# COMMAND ----------

#printSchema
df.printSchema()

# COMMAND ----------

#fisrt few rows of dataframe
df.show(6)

# COMMAND ----------

#select only 2 columns
df.select('age','mobile').show(5)

# COMMAND ----------

try:
    df['age'].show(5)
except TypeError:
    print("This give us an error because return a Column not a Dataframe. The method select return a DataFrame :)")


# COMMAND ----------

#info about dataframe
df.describe().show()

# COMMAND ----------

from pyspark.sql.types import StringType,DoubleType,IntegerType

# COMMAND ----------

#with column return a new dataframe
#in order to add the new column to the dataframe, we will need to assign this to an exisiting or new dataframe.
#df= df.withColumn("age_after_10_yrs",(df["age"]+10))

df.withColumn("age_after_10_yrs",(df["age"]+10)).show(10,False)

# COMMAND ----------

df.withColumn('age_double',df['age'].cast(DoubleType())).show(10,False)

# COMMAND ----------

#with column
df.withColumn("age_after_10_yrs",(df['age'].cast(DoubleType())+10)).show(10,False)

# COMMAND ----------

#filter the records 
df.filter(df['mobile']=='Vivo').show()

# COMMAND ----------

#filter the records 
df.filter(df['mobile']=='Vivo').select('age','ratings','mobile').show()

# COMMAND ----------

type(df.filter(df['mobile']=='Vivo'))

# COMMAND ----------

#filter the multiple conditions
df.filter(df['mobile']=='Vivo').filter(df['experience'] >10).show()

# COMMAND ----------

#filter the multiple conditions
df.filter((df['mobile']=='Vivo')&(df['experience'] >10)).show()

# COMMAND ----------

#Distinct Values in a column
df.select('mobile').distinct().show()

# COMMAND ----------

type(df.select('mobile').distinct())

# COMMAND ----------

#distinct value count
df.select('mobile').distinct().count()

# COMMAND ----------

df.groupBy('mobile').count().show(5,False)

# COMMAND ----------

# Value counts
print(type(df.groupBy('mobile')))
print(type(df.groupBy('mobile').count())) #Crea un dataframe con mobile y count
df.groupBy('mobile').count().orderBy('count',ascending=False).show(5,False)

# COMMAND ----------

# Value counts
df.groupBy('mobile').mean().show(5,False)

# COMMAND ----------

df.groupBy('mobile').sum().show(5,False)

# COMMAND ----------

# Value counts
df.groupBy('mobile').max().show(5,False)

# COMMAND ----------

# Value counts
df.groupBy('mobile').min().show(5,False)

# COMMAND ----------

#Aggregation
df.groupBy('mobile').agg({'experience':'min'}).show(5,False)

# COMMAND ----------

# UDF
from pyspark.sql.functions import udf


# COMMAND ----------

#normal function 
def price_range(brand):
    if brand in ['Samsung','Apple']:
        return 'High Price'
    elif brand =='MI':
        return 'Mid Price'
    else:
        return 'Low Price'

# COMMAND ----------

#create udf using python function
brand_udf=udf(price_range,StringType())
#apply udf on dataframe
df.withColumn('price_range',brand_udf(df['mobile'])).show(10,False)

# COMMAND ----------

#using lambda function
age_udf = udf(lambda age: "young" if age <= 30 else "senior", StringType())
#apply udf on dataframe
df.withColumn("age_group", age_udf(df.age)).show(10,False)

# COMMAND ----------

#pandas udf
from pyspark.sql.functions import pandas_udf, PandasUDFType

# COMMAND ----------

#create python function
def remaining_yrs(age):
    yrs_left=100-age

    return yrs_left

# COMMAND ----------

#create udf using python function
length_udf = pandas_udf(remaining_yrs, IntegerType())
#apply pandas udf on dataframe
df.withColumn("yrs_left", length_udf(df['age'])).show(10,False)

# COMMAND ----------

#udf using two columns 
def prod(rating,exp):
    x=rating*exp
    return x

# COMMAND ----------

#create udf using python function
prod_udf = pandas_udf(prod, DoubleType())
#apply pandas udf on multiple columns of dataframe
df.withColumn("product", prod_udf(df['ratings'],df['experience'])).show(10,False)

# COMMAND ----------

#duplicate values
df.count()

# COMMAND ----------

#drop duplicate values
df=df.dropDuplicates()

# COMMAND ----------

#validate new count
df.count()

# COMMAND ----------

#drop column of dataframe
df_new=df.drop('mobile')

# COMMAND ----------

df_new.show(10)

# COMMAND ----------

# saving file (csv)

# COMMAND ----------

#current working directory
import os

os.getcwd()

# COMMAND ----------

#target directory 
write_uri = os.getcwd() + "\\df_csv"

# COMMAND ----------

#save the dataframe as single csv 
df.coalesce(1).write.format("csv").option("header","true").save(write_uri)

# COMMAND ----------

# parquet

# COMMAND ----------

#target location
parquet_uri = os.getcwd() + "\\df_parquet"

# COMMAND ----------

#save the data into parquet format 
df.write.format('parquet').save(parquet_uri)

# COMMAND ----------

df = spark.read.parquet(parquet_uri)
df

# COMMAND ----------


