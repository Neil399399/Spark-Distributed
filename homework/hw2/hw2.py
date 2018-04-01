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

def TFpublishDate(line):
    dict={}
    dates = [x for x in re.split(' ',line[5].strip())]
    if dates[0] in dict:
        dict[dates[0]].append(line[0])
    else:
        dict[dates[0]]=[line[5]]
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

# # 4 topic dictionary (title). 
# topicObamaDicts_title = topicObama.map(TFtitle).take(topicObama.count())
# topicEconomyDicts_title = topicEconomy.map(TFtitle).take(topicEconomy.count())
# topicMicrosoftDicts_title = topicMicrosoft.map(TFtitle).take(topicMicrosoft.count())
# topicPalestineDicts_title = topicPalestine.map(TFtitle).take(topicPalestine.count())

# # 4 topic dictionary (headline). 
# topicObamaDicts_headline = topicObama.map(TFheadline).take(topicObama.count())
# topicEconomyDicts_headline = topicEconomy.map(TFheadline).take(topicEconomy.count())
# topicMicrosoftDicts_headline = topicMicrosoft.map(TFheadline).take(topicMicrosoft.count())
# topicPalestineDicts_headline = topicPalestine.map(TFheadline).take(topicPalestine.count())

# # per day dictionary (publish date). 
dates_dict = subData1.map(TFpublishDate).take(subData1.count())
dict = {}
for x in range(0,len(dates_dict)):
    Dict(dates_dict[x],dict)
print(dict)

# # define dictionarys.
# ## for title.
# topicObamaDict_title = {}
# topicEconomyDict_title = {}
# topicMicrosoftDict_title = {}
# topicPalestineDict_title = {}

# ## for headline.
# topicObamaDict_headline = {}
# topicEconomyDict_headline = {}
# topicMicrosoftDict_headline = {}
# topicPalestineDict_headline = {}

# # merge all dict.
# for x in range(0,len(topicObamaDicts_title)):
#     Dict(topicObamaDicts_title[x],topicObamaDict_title)

# for x in range(0,len(topicEconomyDicts_title)):
#     Dict(topicEconomyDicts_title[x],topicEconomyDict_title)

# for x in range(0,len(topicMicrosoftDicts_title)):
#     Dict(topicMicrosoftDicts_title[x],topicMicrosoftDict_title)

# for x in range(0,len(topicPalestineDicts_title)):
#     Dict(topicPalestineDicts_title[x],topicPalestineDict_title)

# for x in range(0,len(topicObamaDicts_headline)):
#     Dict(topicObamaDicts_headline[x],topicObamaDict_headline)

# for x in range(0,len(topicEconomyDicts_headline)):
#     Dict(topicEconomyDicts_headline[x],topicEconomyDict_headline)

# for x in range(0,len(topicMicrosoftDicts_headline)):
#     Dict(topicMicrosoftDicts_headline[x],topicMicrosoftDict_headline)

# for x in range(0,len(topicPalestineDicts_headline)):
#     Dict(topicPalestineDicts_headline[x],topicPalestineDict_headline)

# # find top 3 words.
# print("Topic obama top3 most frequent words in title :",Counter(topicObamaDict_title).most_common(3))
# print("Topic economy most frequent words in title :",Counter(topicEconomyDict_title).most_common(3))
# print("Topic microsoft most frequent words in title :",Counter(topicMicrosoftDict_title).most_common(3))
# print("Topic palestine most frequent words in title :",Counter(topicPalestineDict_title).most_common(3))

# print("Topic obama top3 most frequent words in headline :",Counter(topicObamaDict_headline).most_common(3))
# print("Topic economy most frequent words in headline :",Counter(topicEconomyDict_headline).most_common(3))
# print("Topic microsoft most frequent words in headline :",Counter(topicMicrosoftDict_headline).most_common(3))
# print("Topic palestine most frequent words in headline :",Counter(topicPalestineDict_headline).most_common(3))