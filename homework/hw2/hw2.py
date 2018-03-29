# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import csv
import os


def Parser(line):
    values = [x for x in line.split(",")]
    return values

def Normalization(x):
    max = x.max()
    min = x.min()
    result = x.map(lambda x: (x-min)/(max-min))
    return result

# Spark configure.
sparkMaster="spark://172.17.0.5:7077"
sparkAppName="hw1"
sparkExecutorMemory="3g"
sparkDriverMemory="3g"
sparkCoreMax="4"
outputFile = "result.txt"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory).set("spark.driver.memory",sparkDriverMemory)
sc = SparkContext(conf=conf)

# decode dataset.
with open ("/root/homework/dataset/hw2/News_Final.csv",'r',encoding = 'utf8') as file:
    data = csv.reader(file,delimiter = ",")
    dataset = list(data)
print("dataset long:",len(dataset))

# Input data.
dataset1 = sc.parallelize(dataset)
print("dataset(RDD) long:",dataset1.count())

# # remove header.
header = dataset1.first()
print("header",header)
subData1 = dataset1.filter(lambda x: x !=header)

# split.
topicObama = subData1.filter(lambda x: x[4]=='obama')
topicEconomy = subData1.filter(lambda x: x[4]=='economy')
topicMicrosoft = subData1.filter(lambda x: x[4]=='microsoft')
topicPalestine = subData1.filter(lambda x: x[4]=='palestine')

# topicPalestineList = topicPalestine.collect()
print("Topic Obama:",topicObama.first()
print("Topic Economy:",topicEconomy.count())
print("Topic Microsoft:",topicMicrosoft.count())
print("Topic Palestine:",topicPalestine.count())
# title = parserResult.map(lambda x: float(x[2])
