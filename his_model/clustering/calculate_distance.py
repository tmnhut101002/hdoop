from pyspark.sql import SparkSession
import numpy as np
import os
from math import sqrt
import logging

# def create_path(filename):
#     current_directory = os.path.dirname(os.path.abspath(__file__))
#     return os.path.join(current_directory, filename)

def extractCentroids(line):            
        centroid_id, centroid_value = line.strip().split('\t')
        centroid_value = centroid_value.strip().split('|')
        centroid_value = [el.strip().split(';') for el in centroid_value]
        centroid_coordinate = np.array(centroid_value, dtype='f')[:, 1].reshape(1, -1)
        return (centroid_id, list(centroid_coordinate))

def extractUserItemMatrix(line):
    user, values = line.strip().split('\t')
    coordinate = values.strip().split('|')
    coordinate = [el.strip().split(';') for el in coordinate]
    coordinate = np.array(coordinate, dtype='f')[:, 1].reshape(1, -1)
    return (user, list(coordinate))

def euclidean_distance(array1, array2):
    if len(array1) != len(array2):
        raise ValueError("Two arrays must have the same length")
    
    squared_diff_sum = 0
    for i in range(len(array1)):
        squared_diff_sum += (array1[i] - array2[i]) ** 2
    squared_diff_sum = sum(squared_diff_sum)
    return sqrt(squared_diff_sum)            

def calDistance (userItemMatrixFile, centroidsFile, outputFile):
    logging.basicConfig(level=logging.ERROR)
    spark = SparkSession.builder.appName("calDistance").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Đọc ma trận User x Item, Trọng tâm
    UserItemRDD = spark.sparkContext.textFile(userItemMatrixFile).map(lambda x: extractUserItemMatrix(x))
    CentroidsRDD = spark.sparkContext.textFile(centroidsFile).map(lambda x: extractCentroids(x))
    CentroidsList = CentroidsRDD.collect()
    
    # Tính khoảng cách
    preDistance = UserItemRDD.map(lambda x : (x, CentroidsList))
    preDistance1 = preDistance.map(lambda x: (x[0][0],euclidean_distance(x[0][1],x[1][0][1])))
    
    # Ghi kết quả vào HDFS
    preDistance1.toDF(["user","distance"]).write.mode("overwrite").options(header='False', delimiter='\t').csv(outputFile)

    spark.stop()
    
if __name__ == "__main__":
    
    host = 'localhost'
    port = '3306'
    user = 'root'
    password = '1234'
    
    mysql_url = f"jdbc:mysql://{host}:{port}/ecommerce?useSSL=false"
    mysql_properties = {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    print('Calculate Distance (D)...')
    userItemMatrixFile = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    centroidsFile = "hdfs://localhost:9000/HM_clustering/NewCentroids"
    outputFile = "hdfs://localhost:9000/HM_clustering/Distance"
    calDistance(userItemMatrixFile, centroidsFile, outputFile)

