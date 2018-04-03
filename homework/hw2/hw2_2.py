# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from collections import Counter
import csv
import os
import re


def per_hour_popularity(line):
    values = [x for x in line.strip().split(",")]
    average = []
    for x in range(0,len(values),+3):
        tempAverage = values[x+1]+values[x+2]+values[x+3]
        average.append(tempAverage)
    return average

def per_day_popularity(line):
    values = [x for x in line.strip().split(",")]
    for x in range(1,len(values)):
        average = average+values[x]
    return average

# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw2"
sparkExecutorMemory="3g"
sparkDriverMemory="3g"
sparkCoreMax="4"
outputFile = "result.txt"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory).set("spark.driver.memory",sparkDriverMemory)
sc = SparkContext(conf=conf)

# input data.
with open ("/root/homework/dataset/hw2/Fackbook_Economy.csv",'r') as file:
    data = csv.reader(file,delimiter = ",")
    fackbook_Economy = list(data)

fackbook_Economy_RDD = sc.parallelize(fackbook_Economy)
per_hour_result = fackbook_Economy_RDD.map(per_hour_popularity)

print("per_hour_average :",per_hour_result.collect())
