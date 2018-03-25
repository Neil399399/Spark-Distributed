# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf


def parser(line):
    values = [x for x in line.split(";")]
    return values


# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw1"
sparkExecutorMemory="2g"
sparkCoreMax="4"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory)
sc = SparkContext(conf=conf)

# Input data.
dataset = sc.textFile("file:/root/homework/dataset/hw1/household_power_consumption.txt")
print("dataset long:",dataset.count())
header = dataset.first()
subData1 = dataset.filter(lambda x: x !=header).filter(lambda x: x!="?")
print("subData long:",subData1.count())
# map.
parserResult = subData1.map(parser).map(lambda x: x[3]).collect()
print("check parser result:",parserResult)
print("Max global active power:")








