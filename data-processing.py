# Databricks notebook source
#import SparkSession
from pyspark.sql import SparkSession


#create spar session object
spark=SparkSession.builder.appName('data_processing').getOrCreate()


# Load csv Dataset 
df=spark.read.csv('FileStore/tables/sample_data.csv',inferSchema=True,header=True)


#columns of dataframe
df.columns


#check number of columns
len(df.columns)


#number of records in dataframe
df.count()


#shape of dataset
print((df.count(),len(df.columns)))


#printSchema
df.printSchema()


#fisrt few rows of dataframe
df.show(6)


#select only 2 columns
df.select('age','mobile').show(5)


try:
    df['age'].show(5)
except TypeError:
    print("This give us an error because return a Column not a Dataframe. The method select return a DataFrame :)")



#info about dataframe
df.describe().show()


from pyspark.sql.types import StringType,DoubleType,IntegerType


#with column return a new dataframe
#in order to add the new column to the dataframe, we will need to assign this to an exisiting or new dataframe.
#df= df.withColumn("age_after_10_yrs",(df["age"]+10))

df.withColumn("age_after_10_yrs",(df["age"]+10)).show(10,False)


df.withColumn('age_double',df['age'].cast(DoubleType())).show(10,False)


#with column
df.withColumn("age_after_10_yrs",(df['age'].cast(DoubleType())+10)).show(10,False)


#filter the records 
df.filter(df['mobile']=='Vivo').show()


#filter the records 
df.filter(df['mobile']=='Vivo').select('age','ratings','mobile').show()


type(df.filter(df['mobile']=='Vivo'))


#filter the multiple conditions
df.filter(df['mobile']=='Vivo').filter(df['experience'] >10).show()


#filter the multiple conditions
df.filter((df['mobile']=='Vivo')&(df['experience'] >10)).show()


#Distinct Values in a column
df.select('mobile').distinct().show()


type(df.select('mobile').distinct())


#distinct value count
df.select('mobile').distinct().count()


df.groupBy('mobile').count().show(5,False)


# Value counts
print(type(df.groupBy('mobile')))
print(type(df.groupBy('mobile').count())) #Crea un dataframe con mobile y count
df.groupBy('mobile').count().orderBy('count',ascending=False).show(5,False)


# Value counts
df.groupBy('mobile').mean().show(5,False)


df.groupBy('mobile').sum().show(5,False)


# Value counts
df.groupBy('mobile').max().show(5,False)


# Value counts
df.groupBy('mobile').min().show(5,False)


#Aggregation
df.groupBy('mobile').agg({'experience':'min'}).show(5,False)


# UDF
from pyspark.sql.functions import udf



#normal function 
def price_range(brand):
    if brand in ['Samsung','Apple']:
        return 'High Price'
    elif brand =='MI':
        return 'Mid Price'
    else:
        return 'Low Price'


#create udf using python function
brand_udf=udf(price_range,StringType())
#apply udf on dataframe
df.withColumn('price_range',brand_udf(df['mobile'])).show(10,False)


#using lambda function
age_udf = udf(lambda age: "young" if age <= 30 else "senior", StringType())
#apply udf on dataframe
df.withColumn("age_group", age_udf(df.age)).show(10,False)


#pandas udf
from pyspark.sql.functions import pandas_udf, PandasUDFType


#create python function
def remaining_yrs(age):
    yrs_left=100-age

    return yrs_left


#create udf using python function
length_udf = pandas_udf(remaining_yrs, IntegerType())
#apply pandas udf on dataframe
df.withColumn("yrs_left", length_udf(df['age'])).show(10,False)


#udf using two columns 
def prod(rating,exp):
    x=rating*exp
    return x


#create udf using python function
prod_udf = pandas_udf(prod, DoubleType())
#apply pandas udf on multiple columns of dataframe
df.withColumn("product", prod_udf(df['ratings'],df['experience'])).show(10,False)


#duplicate values
df.count()


#drop duplicate values
df=df.dropDuplicates()


#validate new count
df.count()


#drop column of dataframe
df_new=df.drop('mobile')


df_new.show(10)


# saving file (csv)


#current working directory
import os

os.getcwd()


#target directory 
write_uri = os.getcwd() + "\\df_csv"


#save the dataframe as single csv 
df.coalesce(1).write.format("csv").option("header","true").save(write_uri)


# parquet


#target location
parquet_uri = os.getcwd() + "\\df_parquet"


#save the data into parquet format 
df.write.format('parquet').save(parquet_uri)


df = spark.read.parquet(parquet_uri)
df



