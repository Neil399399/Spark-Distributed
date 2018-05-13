import findspark
findspark.init()
import csv
from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix



# Spark configure.
sparkMaster="spark://172.17.0.2:7077"
sparkAppName="hw2"
sparkExecutorMemory="3g"
sparkDriverMemory="3g"
sparkCoreMax="4"
sparkRpcMessageMaxSize="256"

# Setting Spark conf.
conf = SparkConf().setMaster(sparkMaster).setAppName(sparkAppName).set("spark.executor.memory",sparkExecutorMemory).set("spark.driver.memory",sparkDriverMemory)
sc = SparkContext(conf=conf)


# decode dataset.
with open ("TF-matrix.txt",'r',encoding = 'utf8') as file:
    data = csv.reader(file,delimiter = ",")
    temp = list(data)
print("temp long:",len(temp))


dataset = []
for x in temp:
    if len(x) != 0:
        dataset.append(x)
        
print("dataset long:",len(dataset))

subdataset = dataset[0:15000]
# # Input data.
dataset1 = sc.parallelize(subdataset)
print("dataset long:",dataset1.count())

mat = RowMatrix(dataset1)
# # Compute the top 5 singular values and corresponding singular vectors.
svd = mat.computeSVD(5, computeU=True)
U = svd.U
s = svd.s
V = svd.V

print(U)