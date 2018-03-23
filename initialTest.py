#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 12 19:45:54 2018

@author: ilan
"""

## This code performs map reduce on seattle library dataset
#from pyspark.sql import SparkSession
#
#spark = SparkSession.builder.appName('seattle-map-reduce').getOrCreate()
#
## Read from HDFS
#df_load = spark.read.csv("hdfs://localhost:9000//user/ilan/Checkouts_By_Title_Data_Lens_2017.csv")

from pyspark import SparkContext

sc = SparkContext('local','seattleLibraryTest')
data = sc.textFile("hdfs://localhost:9000//user/ilan/Checkouts_By_Title_Data_Lens_2017.csv")

result = data.map(lambda x: x.split(','))\
    .map(lambda x: (x[0],1))\
    .reduceByKey(lambda x,y: x+y)
    
# result.take(10)

result.saveAsTextFile("hdfs://localhost:9000//user/ilan/sparkResults.csv")
