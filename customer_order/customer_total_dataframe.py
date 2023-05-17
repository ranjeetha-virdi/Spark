# -*- coding: utf-8 -*-
"""
Created on Wed May 17 12:09:59 2023

@author: kulpr
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("CustomerPurchase").getOrCreate()
schema = StructType([\
                     StructField("CustID", IntegerType(),True),\
                     StructField("ItemID", IntegerType(),True),\
                     StructField("Price", FloatType(),True)
                     ])
    
df=spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")

#debugging step to check the schema
df.printSchema()                  

custpurchase = df.select("CustID","Price")
custpurchasetotal = custpurchase.groupBy("CustID").sum("Price")

custpurchasetotal = custpurchasetotal.withColumn("Total_Price",func.round(func.col("sum(Price)"),2))\
                                                .select("CustID","Total_Price").sort("Total_Price")
                                                
results = custpurchasetotal.collect()

for result in results :
    print(str(result[0]) + str("\t {:.2f}€".format(result[1])))
    
spark.stop()
          
          
                                                
                                                


                                                 
