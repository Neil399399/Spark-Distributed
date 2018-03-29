# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import csv


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

# Input data.
dataset = sc.textFile("file:/root/homework/dataset/hw2/News_Final.csv")
print("dataset long:",dataset.count())

# remove header.
header = dataset.first()
subData1 = dataset.filter(lambda x: x !=header)
print("subData1:",subData1.count())

# split.
topicObama = subData1.map(Parser).filter(lambda x: x[4]=='"obama"'| x[5]=='"obama"')
topicEconomy = subData1.map(Parser).filter(lambda x: x[4]=='"economy"')
topicMicrosoft = subData1.map(Parser).filter(lambda x: x[4]=='"microsoft"')
topicPalestine = subData1.map(Parser).filter(lambda x: x[4]=='"palestine"')

print("Topic Obama:",topicObama.count())
print("Topic Economy:",topicEconomy.count())
print("Topic Microsoft:",topicMicrosoft.count())
print("Topic Palestine:",topicPalestine.count())
# title = parserResult.map(lambda x: float(x[2])
