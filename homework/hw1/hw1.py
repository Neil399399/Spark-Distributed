from pyspark import SparkContext, SparkConf

# Setting Spark conf.
conf = SparkConf().setMaster("spark://172.17.0.2:7077").setAppName("hw1")
sc = SparkContext(conf=conf)

# Input data.
dataset=sc.textFile("file:~homework/Spark-Distributed/homework/hw1/dataset/household_power_consumption.txt")
print(dataset.count())