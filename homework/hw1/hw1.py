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
subData1 = dataset.filter(lambda x: x !=header)
# map for gap.
parserResult = subData1.map(parser).filter(lambda x: x[2]!="?")
gap = parserResult.map(lambda x: float(x[2]))
max = gap.max()
min = gap.min()
# map for grp.
parserResult2 = subData1.map(parser).filter(lambda x: x[3]!="?")
grp = parserResult2.map(lambda x: float(x[3]))
# map for voltage.
parserResult3 = subData1.map(parser).filter(lambda x: x[4]!="?")
vol = parserResult3.map(lambda x: float(x[4]))
# map for global intensity.
parserResult4 = subData1.map(parser).filter(lambda x: x[5]!="?")
gi = parserResult4.map(lambda x: float(x[5]))

print("/------------ Question 1, 2 ---------------/")
print("Global active power:",gap.stats())
print("Global reactive power:",grp.stats())
print("Voltage:",vol.stats())
print("Global intensity:",gi.stats())

print("/------------ Question 3 ---------------/")
print("Global active power:",gapNormalization.collect())


