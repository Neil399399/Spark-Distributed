# init to find pyspark folder.
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import csv


def Parser(line):
    values = [x for x in line.split(";")]
    return values

def Normalization(x):
    max = x.max()
    min = x.min()
    result = x.map(lambda x: (x-min)/(max-min))
    return result

def Writer(filename,contant):
    file = open(filename,'a')
    writer = csv.writer(file)
    writer.writerows(contant)
    file.close()

# Spark configure.
sparkMaster="spark://172.17.0.5:7077"
sparkAppName="hw1"
sparkExecutorMemory="3g"
sparkCoreMax="4"
outputFile = "result.txt"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory)
sc = SparkContext(conf=conf)

# Input data.
dataset = sc.textFile("file:/root/homework/dataset/hw1/household_power_consumption.txt")
print("dataset long:",dataset.count())

# remove header.
header = dataset.first()
subData1 = dataset.filter(lambda x: x !=header)

# map for gap.
parserResult = subData1.map(Parser).filter(lambda x: x[2]!="?")
gap = parserResult.map(lambda x: float(x[2]))
gapN = Normalization(gap).collect()

# map for grp.
parserResult2 = subData1.map(Parser).filter(lambda x: x[3]!="?")
grp = parserResult2.map(lambda x: float(x[3]))
grpN = Normalization(grp).collect()

# map for voltage.
parserResult3 = subData1.map(Parser).filter(lambda x: x[4]!="?")
vol = parserResult3.map(lambda x: float(x[4]))
# volN = Normalization(vol)

# map for global intensity.
parserResult4 = subData1.map(Parser).filter(lambda x: x[5]!="?")
gi = parserResult4.map(lambda x: float(x[5]))
# giN = Normalization(gi)

# write in file.
for i in range(0,len(gapN)):
    newValues=[]
    newValues.append(gapN[i])
    newValues.append(grpN[i])
    Writer(outputFile,newValues)


    

print("/------------ Question 1, 2 ---------------/")
print("Global active power:",gap.stats())
print("Global reactive power:",grp.stats())
print("Voltage:",vol.stats())
print("Global intensity:",gi.stats())

print("/------------ Question 3 ---------------/")
# print("Global active power:",gapN.count())
# print("Global reactive power:",grpN.count())
# print("Voltage:",volN.count())
# print("Global intensity:",giN.count())


