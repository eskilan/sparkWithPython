#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 12 20:04:33 2018

@author: ilan
"""

# This code performs map reduce on seattle library dataset
from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.appName('seattle-authors').getOrCreate()

# Read from HDFS

df1 = sparkSession.read.csv("hdfs://localhost:9000//user/ilan/Checkouts_By_Title_Data_Lens_2017.csv",header=True)
df2 = sparkSession.read.csv("./Library_Collection_Inventory.csv",header=True) # in local filesystem

# perform map reduce operation on df1 by grouping by BibNumber
countsByBib = df1.groupBy("BibNumber").count()

# create new dataframe RDD from df2 keeping BibNumber, and Author, then removing duplicates and dropping null values
df3 = df2.select(df2['BibNum'],df2['Author']).distinct().dropna()

# joining countsByBib and df3 on countsByBib.bibNumber==df3.BibNum
df4 = countsByBib.join(df3,countsByBib.BibNumber==df3.BibNum,how='inner')

# now we group by author
df5 = df4.select(df4['count'],df4['Author']).groupBy('Author').sum()

# order by descending count
df6 = df5.sort("sum(count)",ascending=False)

# show 
df6.show()

pDF=df6.toPandas()

sparkSession.stop()
