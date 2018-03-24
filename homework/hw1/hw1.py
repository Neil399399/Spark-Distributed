import findspar
findspark.init()
from pyspark import SparkContext, SparkConf

# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw1"
sparkExecutorMemory="2g"
sparkCoreMax="4"
# init.
# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory)
sc = SparkContext(conf=conf)

# Input data.
dataset=sc.textFile("file:/root/homework/dataset/hw1/household_power_consumption.txt")
print(dataset.count())




