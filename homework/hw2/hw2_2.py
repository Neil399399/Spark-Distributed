# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from collections import Counter
import csv
import os
import re
 

# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw1"
sparkExecutorMemory="3g"
sparkDriverMemory="3g"
sparkCoreMax="4"
outputFile = "result.txt"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory).set("spark.driver.memory",sparkDriverMemory)
sc = SparkContext(conf=conf)

# input data.
fackbook_Economy = sc.textFile("file:/homework/dataset/")