# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from collections import Counter
import csv
import os
import re


def Parser(line):
    values = [x for x in line.split(",")]
    return values

# TF
def TFtitle(line):
    dict={}
    titleWords = [x for x in re.split(' |, |\u2019|\u2018|\u2014|\u2013|\u2026|\u201d|\u201c|:|(|)|\xe1|\xf1|...',line[1].strip())]
    for x in range(0,len(titleWords)):
        if titleWords[x] in dict:
            dict[titleWords[x]]=dict[titleWords[x]]+1
        else:
            dict[titleWords[x]]=1
    return dict

def TFheadline(line):
    dict={}
    headlineWords = [x for x in re.split(' |, |\u2019|\u2018|\u2014|\u2013|\u2026|\u201d|\u201c|:|(|)|\xe1|\xf1|...',line[2].strip())]
    for x in range(0,len(headlineWords)):
        if headlineWords[x] in dict:
            dict[headlineWords[x]]=dict[headlineWords[x]]+1
        else:
            dict[headlineWords[x]]=1
    return dict

def Dict(dictionary,newDictionary):
    for (key, value) in dictionary.items():
        if key in newDictionary:
            newDictionary[key]=newDictionary[key]+1
        else:
            newDictionary[key]=1
    return newDictionary
  

 

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

# split 4 topic.
topicObama = subData1.filter(lambda x: x[4]=='obama')
topicEconomy = subData1.filter(lambda x: x[4]=='economy')
topicMicrosoft = subData1.filter(lambda x: x[4]=='microsoft')
topicPalestine = subData1.filter(lambda x: x[4]=='palestine')

# 4 topic dictionary (title). 
topicObamaDicts_title = topicObama.map(TFtitle).take(topicObama.count())
topicEconomyDicts_title = topicEconomy.map(TFtitle).take(topicEconomy.count())
topicMicrosoftDicts_title = topicMicrosoft.map(TFtitle).take(topicMicrosoft.count())
topicPalestineDicts_title = topicPalestine.map(TFtitle).take(topicPalestine.count())

# define dictionarys.
topicObamaDict = {}
topicEconomyDict = {}
topicMicrosoftDict = {}
topicPalestineDict = {}

# merge all dict.
for x in range(0,len(topicObamaDicts_title)):
    Dict(topicObamaDicts_title[x],topicObamaDict)

for x in range(0,len(topicEconomyDicts_title)):
    Dict(topicEconomyDicts_title[x],topicEconomyDict)

for x in range(0,len(topicMicrosoftDicts_title)):
    Dict(topicMicrosoftDicts_title[x],topicMicrosoftDict)

for x in range(0,len(topicPalestineDicts_title)):
    Dict(topicPalestineDicts_title[x],topicPalestineDict)

# find top 3 words.
print(Counter(topicObamaDict).most_common(3))
print(Counter(topicEconomyDict).most_common(3))
print(Counter(topicMicrosoftDict).most_common(3))
print(Counter(topicPalestineDict).most_common(3))




# print(dict.items())
# topicPalestineList = topicPalestine.collect()
# print("Topic Obama:",topicObama.first())
# print("Topic Economy:",topicEconomy.count())
# print("Topic Microsoft:",topicMicrosoft.count())
# print("Topic Palestine:",topicPalestine.count())
# title = parserResult.map(lambda x: float(x[2])

