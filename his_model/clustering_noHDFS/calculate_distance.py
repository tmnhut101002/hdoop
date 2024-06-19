from pyspark.sql import SparkSession
import numpy as np
import os
from math import sqrt

def extractCentroids(centroid_id, centroid_value):            
    # centroid_id, centroid_value = line.strip().split('\t')
    centroid_value = centroid_value.strip().split('|')
    centroid_value = [el.strip().split(';') for el in centroid_value]
    centroid_coordinate = np.array(centroid_value, dtype='f')[:, 1].reshape(1, -1)
    return (centroid_id, list(centroid_coordinate))

def extractUserItemMatrix(user, values):
    # user, values = line.strip().split('\t')
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
    
def calDistance (userItemMatrix_df, newCentroid_df):
    spark = SparkSession.builder.appName("calDistance").getOrCreate()

    UserItemRDD = spark.createDataFrame(userItemMatrix_df).rdd.map(lambda x: extractUserItemMatrix(x['_1'],x['_2']))
    CentroidsRDD = spark.createDataFrame(newCentroid_df).rdd.map(lambda x: extractCentroids(x['User'],x['UIRateData']))
    CentroidsList = CentroidsRDD.collect()
    # File centroid chi co 1 trong tam duy nhat
    
    preDistance = UserItemRDD.map(lambda x : (x, CentroidsList)) #([user, (rate)],[centroid, (rate)])
    
    preDistance1 = preDistance.map(lambda x: (x[0][0],euclidean_distance(x[0][1],x[1][0][1])))
    result = preDistance1.toDF(["User","Distance"])
    result = result.toPandas()

    spark.stop()
    return result
    
if  __name__ == "__main__":
    spark = SparkSession.builder.appName("calDistance").getOrCreate()
    userItemMatrixFile = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    centroidsFile = "hdfs://localhost:9000/Clustering/NewCentroids.csv"
    outputFile = "hdfs://localhost:9000/Clustering/Distance.csv"
    calDistance (spark, userItemMatrixFile, centroidsFile, outputFile)
    
    spark.stop()
