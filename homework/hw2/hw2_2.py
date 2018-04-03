# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from collections import Counter
import csv
import os
import re

# global values.
Dir = '/root/homework/dataset/hw2/'
resultDir = '/root/homework/result/hw2/'
newHeader_perHour = ['IDLink','1 hour','2 hour','3 hour','4 hour','5 hour','6 hour','7 hour','8 hour','9 hour','10 hour','11 hour','12 hour'
                    ,'13 hour','14 hour','15 hour','16 hour','17 hour','18 hour','19 hour','20 hour','21 hour','22 hour','23 hour','24 hour'
                    ,'25 hour','26 hour','27 hour','28 hour','29 hour','30 hour','31 hour','32 hour','33 hour','34 hour','35 hour','36 hour'
                    ,'37 hour','38 hour','39 hour','40 hour','41 hour','42 hour','43 hour','44 hour','45 hour','46 hour','47 hour','48 hour']

newHeader_perDay = ['IDLink','Day 1','Day 2']

def per_hour_popularity(line):
    average = []
    ID = line[0]
    average.append(ID)
    tempAverage=0
    for x in range(1,len(line)):
        if x%3==0:
            tempAverage=tempAverage+float(line[x])
            average.append(tempAverage)
            tempAverage=0
        else:
            tempAverage=tempAverage+float(line[x])
    return average

def per_day_popularity(line):
    average = []
    ID = line[0]
    average.append(ID)
    tempAverage=0
    for x in range(1,len(line)):
        if x%72==0:
            tempAverage=tempAverage+float(line[x])
            average.append(tempAverage)
            tempAverage=0
        else:
            tempAverage=tempAverage+float(line[x])
    return average

def Do(fileName):
    # input data.
    with open (Dir+fileName+".csv",'r',encoding = 'utf8') as file:
        data = csv.reader(file,delimiter = ",")
        dataset = list(data)

    # create RDD.
    data_RDD = sc.parallelize(dataset)
    # mapping.(per_hour and per day)
    header = data_RDD.first()
    per_hour_result = data_RDD.filter(lambda x: x!=header).map(per_hour_popularity).collect()
    per_day_result = data_RDD.filter(lambda x: x!=header).map(per_day_popularity).collect()

    # write in file (per_hour_result).
    file = open(resultDir+fileName+"_perHour.txt",'a')
    writer = csv.writer(file)
    writer.writerow(newHeader_perHour)
    for i in range(0,len(per_hour_result)):
        writer.writerow(per_hour_result[i])
    file.close()

    # write in file (per_day_result).
    file = open(resultDir+fileName+"_perDay.txt",'a')
    writer = csv.writer(file)
    writer.writerow(newHeader_perDay)
    for i in range(0,len(per_day_result)):
        writer.writerow(per_day_result[i])
    file.close()
    print(fileName,"Parser Success!")


# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw2"
sparkExecutorMemory="3g"
sparkDriverMemory="3g"
sparkCoreMax="4"
outputFile = "result2.txt"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory).set("spark.driver.memory",sparkDriverMemory)
sc = SparkContext(conf=conf)


fileList = ['Facebook_Economy','Facebook_Microsoft','Facebook_Obama','Facebook_Palestine'
            ,'GooglePlus_Economy','GooglePlus_Microsoft','GooglePlus_Obama','GooglePlus_Palestine'
            ,'LinkedIn_Economy','LinkedIn_Microsoft','LinkedIn_Obama','LinkedIn_Palestine']

# start.
for x in fileList:
    Do(x)
print("all success!")