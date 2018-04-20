# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from collections import Counter
import csv
import os
import re
import string


# TF
def TFtitle(line):
    dict={}
    titleWords = re.findall('[a-zA-z]+', str(line[2]))
    for x in range(0,len(titleWords)):
        if titleWords[x] in dict:
            dict[titleWords[x]]=dict[titleWords[x]]+1
        else:
            dict[titleWords[x]]=1
    return dict

def TFheadline(line):
    dict={}
    headlineWords = re.findall('[a-zA-z]+', str(line[2]))
    for x in range(0,len(headlineWords)):
        if headlineWords[x] in dict:
            dict[headlineWords[x]]=dict[headlineWords[x]]+1
        else:
            dict[headlineWords[x]]=1
    return dict

def TFpublishDate(line):
    dict={}
    dates = [x for x in re.split(' ',line[5].strip())]
    dict[dates[0]]=line[0]
    return dict

def Dict(dictionary,newDictionary):
    for (key, value) in dictionary.items():
        if key in newDictionary:
            newDictionary[key]=newDictionary[key]+1
        else:
            newDictionary[key]=1
    return newDictionary

# return Title
def Title(line):
    values = [x for x in line[1].split(' ')]
    return values

# return Headline
def Headline(line):
    values = [x for x in line[2].split(' ')]
    return values

# co-occurrence matrices
def Do(dictionary,topic,column,fileName):
    array = []
    header = []
    if column == 'title':
        Mi = Counter(dictionary).most_common(100)
        topic_list = topic.map(Title).take(topic.count())
    if column =='headline':
        Mi = Counter(dictionary).most_common(100)
        topic_list = topic.map(Headline).take(topic.count())

    for (key,value) in Mi:
        header.append(key)
        temparray = []
        for (key1,value1) in Mi: 
            count = 0
            for x in topic_list:
                if key !=key1:
                    if key in x and key1 in x:
                        count+=1
            temparray.append(count)
        array.append(temparray)
    
    # write in file
    file = open(resultDir+fileName+".txt",'a')
    writer = csv.writer(file)
    writer.writerow(header)
    for x in array:
        writer.writerow(x)
    file.close()


# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw2"
sparkExecutorMemory="3g"
sparkDriverMemory="3g"
sparkCoreMax="4"
resultDir = '/root/homework/result/hw2/'

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

# 4 topic dictionary (headline). 
topicObamaDicts_headline = topicObama.map(TFheadline).take(topicObama.count())
topicEconomyDicts_headline = topicEconomy.map(TFheadline).take(topicEconomy.count())
topicMicrosoftDicts_headline = topicMicrosoft.map(TFheadline).take(topicMicrosoft.count())
topicPalestineDicts_headline = topicPalestine.map(TFheadline).take(topicPalestine.count())

# define dictionarys.
## for title.
topicObamaDict_title = {}
topicEconomyDict_title = {}
topicMicrosoftDict_title = {}
topicPalestineDict_title = {}

## for headline.
topicObamaDict_headline = {}
topicEconomyDict_headline = {}
topicMicrosoftDict_headline = {}
topicPalestineDict_headline = {}

# merge all dict.
for x in range(0,len(topicObamaDicts_title)):
    Dict(topicObamaDicts_title[x],topicObamaDict_title)

for x in range(0,len(topicEconomyDicts_title)):
    Dict(topicEconomyDicts_title[x],topicEconomyDict_title)

for x in range(0,len(topicMicrosoftDicts_title)):
    Dict(topicMicrosoftDicts_title[x],topicMicrosoftDict_title)

for x in range(0,len(topicPalestineDicts_title)):
    Dict(topicPalestineDicts_title[x],topicPalestineDict_title)

for x in range(0,len(topicObamaDicts_headline)):
    Dict(topicObamaDicts_headline[x],topicObamaDict_headline)

for x in range(0,len(topicEconomyDicts_headline)):
    Dict(topicEconomyDicts_headline[x],topicEconomyDict_headline)

for x in range(0,len(topicMicrosoftDicts_headline)):
    Dict(topicMicrosoftDicts_headline[x],topicMicrosoftDict_headline)

for x in range(0,len(topicPalestineDicts_headline)):
    Dict(topicPalestineDicts_headline[x],topicPalestineDict_headline)

# start create co-occurrence matrices.
## Title
Do(topicObamaDict_title,topicObama,'title','topicObamaTitle_matix')
Do(topicEconomyDict_title,topicEconomy,'title','topicEconomyTitle_matix')
Do(topicMicrosoftDict_title,topicMicrosoft,'title','topicMicrosoftTitle_matix')
Do(topicPalestineDict_title,topicPalestine,'title','topicPalestineTitle_matix')
## Headline
Do(topicObamaDict_headline,topicObama,'headline','topicObamaHeadline_matix')
Do(topicEconomyDict_headline,topicEconomy,'headline','topicEconomyHeadline_matix')
Do(topicMicrosoftDict_headline,topicMicrosoft,'headline','topicMicrosoftHeadline_matix')
Do(topicPalestineDict_headline,topicPalestine,'headline','topicPalestineHeadline_matix')