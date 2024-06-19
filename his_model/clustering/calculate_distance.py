from pyspark.sql import SparkSession
import numpy as np
import os
from scipy.spatial.distance import cdist
from math import sqrt

def create_path(filename):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_directory, filename)

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
    spark = SparkSession.builder.appName("calDistance").getOrCreate()

    UserItemRDD = spark.sparkContext.textFile(userItemMatrixFile).map(lambda x: extractUserItemMatrix(x))
    CentroidsRDD = spark.sparkContext.textFile(centroidsFile).map(lambda x: extractCentroids(x))
    CentroidsList = CentroidsRDD.collect()

    preDistance = UserItemRDD.map(lambda x : (x, CentroidsList))
    preDistance1 = preDistance.map(lambda x: (x[0][0],euclidean_distance(x[0][1],x[1][0][1])))
    preDistance1.toDF(["user","distance"]).write.mode("overwrite").options(header='False', delimiter='\t').csv(outputFile)

    spark.stop()

if  __name__ == "__main__":
    userItemMatrixFile = "hdfs://localhost:9000/HM_clustering/UserItemMatrix"
    centroidsFile = "hdfs://localhost:9000/HM_clustering/NewCentroids"
    outputFile = "hdfs://localhost:9000/HM_clustering/Distance"
    calDistance(userItemMatrixFile, centroidsFile, outputFile)
