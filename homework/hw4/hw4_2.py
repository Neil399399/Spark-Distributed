# init to find pyspark folder.
import csv
import logging
import os
import time
from utilities.spark_context_handler import SparkContextHandler
from pyspark.mllib.linalg.distributed import RowMatrix

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

    end_time1 = time.time()
    print("Done.")
    print('Running time:', end_time1 - start_time)

    dataset1 = sc.parallelize(dataset)
    mat = RowMatrix(dataset1)

    # Save result.
    print("Save results ...")

    output_dir = "/home/spark/Documents/spark_web/CPS2017/"
    # write_row(output_dir,"SVD_U_Matrix.txt",U_matrix)
    end_time3 = time.time()
    print("Done.")



