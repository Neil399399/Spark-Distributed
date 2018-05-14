# init to find pyspark folder.
import csv
import logging
import os
import time
from utilities.spark_context_handler import SparkContextHandler
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.linalg import Matrices

def write_row(dir,filename,data):
    file = open(dir+filename, 'a')
    writer = csv.writer(file)
    for vector in data:
        writer.writerow(vector)
    file.close()

def write(dir,filename,data):
    file = open(dir+filename, 'a')
    file.write(data)
    file.close()

# Spark configure.
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
logger = logging.getLogger("pyspark")
SparkContextHandler._master_ip = "10.14.24.101"
sc = SparkContextHandler.get_spark_sc()

if __name__ == '__main__':
    # decode dataset.
    print("Start input data ...")
    start_time = time.time()
    with open ("TF-matrix.txt",'r',encoding = 'utf8') as file:
        data = csv.reader(file,delimiter = ",")
        temp = list(data)

    dataset = []
    for x in temp:
        if len(x) != 0:
            dataset.append(x)

    dataset2 = []
    for x in temp[0]:
        dataset2.append(x)

    end_time1 = time.time()
    print("Done.")
    print('Running time:', end_time1 - start_time)

    # Implement matrix mulitplication.
    matrix1 = sc.parallelize(dataset)
    ## Used RowMatrix make first matrix. (33150,10051)
    mat1 = RowMatrix(matrix1)
    ## Used Matrice make second matrix. (10051,1)
    dm2 = Matrices.dense(10051,1,dataset2)
    mat3 = mat1.multiply(dm2)

    # Save result.
    print("Save results ...")
    output_dir = "/home/spark/Documents/spark_web/CPS2017/"
    write_row(output_dir,"matrix(33150x1).txt",mat3.rows.collect())
    end_time3 = time.time()
    print("Done.")
