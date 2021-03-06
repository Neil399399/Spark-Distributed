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

    # SVD.
    print("Start do SVD ...")
    dataset1 = sc.parallelize(dataset)
    mat = RowMatrix(dataset1)
    # Compute the top 5 singular values and corresponding singular vectors.
    svd = mat.computeSVD(5, computeU=True)
    U = svd.U
    s = svd.s
    V = svd.V
    end_time2 = time.time()
    print("Done.")
    print('Running time:', end_time2 - end_time1)

    # Save result.
    print("Save results ...")
    U_matrix = U.rows.collect()

    output_dir = ""
    # write_row(output_dir,"SVD_U_Matrix.txt",U_matrix)
    write(output_dir,'SVD_S_Matrix.txt',str(s))
    write(output_dir,'SVD_V_Matrix.txt',str(V))
    end_time3 = time.time()
    print("Done.")
    print('Running time:', end_time3 - end_time2)



