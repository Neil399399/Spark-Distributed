# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import csv
import os
import re
newDataset=[]
dict={}


def Parser(line):
    values = [x for x in line.split(",")]
    return values

def TF(line):
    dict={}
    titleWords = [x for x in re.split(' |, ',line[1])]
    # headlineWords = [x for x in line[2].split(" ")]
    for x in range(0,len(titleWords)):
        if titleWords[x] in dict:
            dict[titleWords[x]]=dict[titleWords[x]]+1
        else:
            dict[titleWords[x]]=1
    return dict

def Dict(dictionary):
    for (key, value) in dictionary.items():
        if key in dict:
            dict[key]=dict[key]+1
        else:
            dict[key]=1
    return dict
  

 

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

# # Input data.
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

#test
test = topicObama.map(TF)
test2 = test.take(2)
print(test2[0])
print(test2[1])
# for x in test2:
Dict(test2[0])
print(dict)




# print(dict.items())
# topicPalestineList = topicPalestine.collect()
# print("Topic Obama:",topicObama.first())
# print("Topic Economy:",topicEconomy.count())
# print("Topic Microsoft:",topicMicrosoft.count())
# print("Topic Palestine:",topicPalestine.count())
# title = parserResult.map(lambda x: float(x[2])

