# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from collections import Counter
import csv
import os
import re
import string

# global
resultDir = '/root/homework/result/hw2/'

def Parser(line):
    values = [x for x in line.split(",")]
    return values

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
  
def Find_top_frequent_word_per_day(originRDD,fields):
    # per day dictionary (publish date). 
    dates_dict = originRDD.map(TFpublishDate).take(originRDD.count())
    dict = {}  # {date: ID}
    date = {}
    perDay_TitleList= [] # list for write in file.

    for x in range(0,len(dates_dict)):
        for (key, value) in dates_dict[x].items():
            if key in dict:
                dict[key].append(value)
            else:
                dict[key]=[]
                dict[key].append(value)

    for (key, value) in dict.items():
        tempdict = {}
        topicObamaDicts_title = originRDD.filter(lambda x: key in x[5]).map(fields).collect()
        for x in range(0,len(topicObamaDicts_title)):
            Dict(topicObamaDicts_title[x],tempdict)
        date[key] = tempdict

    # make list.
    for key in date:
        temp =[]
        temp.append(key)
        temp.append(Counter(date[key]).most_common(1))
        perDay_TitleList.append(temp)
    return perDay_TitleList

def Find_top_frequent_word_per_topic(originRDD,fields):
    # define dictionarys.
    # split 4 topic.
    topicObama = originRDD.filter(lambda x: x[4]=='obama')
    topicEconomy = originRDD.filter(lambda x: x[4]=='economy')
    topicMicrosoft = originRDD.filter(lambda x: x[4]=='microsoft')
    topicPalestine = originRDD.filter(lambda x: x[4]=='palestine')

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
    temp =[]
    if fields=='Title':
        # merge all dict.
        for x in range(0,len(topicObamaDicts_title)):
            Dict(topicObamaDicts_title[x],topicObamaDict_title)

        for x in range(0,len(topicEconomyDicts_title)):
            Dict(topicEconomyDicts_title[x],topicEconomyDict_title)

        for x in range(0,len(topicMicrosoftDicts_title)):
            Dict(topicMicrosoftDicts_title[x],topicMicrosoftDict_title)

        for x in range(0,len(topicPalestineDicts_title)):
            Dict(topicPalestineDicts_title[x],topicPalestineDict_title)

        temp.append(Counter(topicObamaDict_title).most_common(1))
        temp.append(Counter(topicEconomyDict_title).most_common(1))
        temp.append(Counter(topicMicrosoftDict_title).most_common(1))
        temp.append(Counter(topicPalestineDict_title).most_common(1))
    else:
        for x in range(0,len(topicObamaDicts_headline)):
            Dict(topicObamaDicts_headline[x],topicObamaDict_headline)

        for x in range(0,len(topicEconomyDicts_headline)):
            Dict(topicEconomyDicts_headline[x],topicEconomyDict_headline)

        for x in range(0,len(topicMicrosoftDicts_headline)):
            Dict(topicMicrosoftDicts_headline[x],topicMicrosoftDict_headline)

        for x in range(0,len(topicPalestineDicts_headline)):
            Dict(topicPalestineDicts_headline[x],topicPalestineDict_headline)

        temp.append(Counter(topicObamaDict_headline).most_common(1))
        temp.append(Counter(topicEconomyDict_headline).most_common(1))
        temp.append(Counter(topicMicrosoftDict_headline).most_common(1))
        temp.append(Counter(topicPalestineDict_headline).most_common(1))
    
    return temp

def Writer(dir,filename,List):
    # write in file (per_day_result).
    file = open(dir+filename+".txt",'a')
    writer = csv.writer(file)
    for i in range(0,len(List)):
        writer.writerow(List[i])
    file.close()
    

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

# decode dataset.
with open ("/root/homework/dataset/hw2/News_Final.csv",'r',encoding = 'utf8') as file:
    data = csv.reader(file,delimiter = ",")
    dataset = list(data)
print("dataset long:",len(dataset))

# # Input data.
dataset1 = sc.parallelize(dataset)
print("dataset(RDD) long:",dataset1.count())

# remove header.
header = dataset1.first()
subData1 = dataset1.filter(lambda x: x !=header)

# per day
TitleResult = Find_top_frequent_word_per_day(subData1,TFtitle)
HeadlineResult = Find_top_frequent_word_per_day(subData1,TFheadline)

Writer(resultDir,'Title_perDay',TitleResult)
Writer(resultDir,'Headline_perDay',HeadlineResult)

# per topic 
Topic_Title_result = Find_top_frequent_word_per_topic(subData1,'Title')
Topic_Headline_result = Find_top_frequent_word_per_topic(subData1,'Headline')

# in total
total_result = []
## title
total_title = subData1.map(TFtitle).take(subData1.count())
totalDict_title = {}
for x in range(0,len(total_title)):
    Dict(total_title[x],totalDict_title)
total_result.append(Counter(totalDict_title).most_common(1))

## headline
total_headline = subData1.map(TFheadline).take(subData1.count())
totalDict_headline = {}
for x in range(0,len(total_headline)):
    Dict(total_headline[x],totalDict_headline)
total_result.append(Counter(totalDict_headline).most_common(1))

print("All Success!")
print("-------------------------------------------------------------")

print('In Total (title, headline):',total_result)
print('In topic (title):',Topic_Title_result)
print('In topic (headline):',Topic_Headline_result)




