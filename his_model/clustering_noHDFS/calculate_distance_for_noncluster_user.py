from pyspark.sql import SparkSession
import numpy as np
import os
#from scipy.spatial.distance import cdist
from math import sqrt

def extractCentroids(centroid_id, centroid_value):            
        # centroid_id, centroid_value = line.strip().split('\t')
        centroid_value = centroid_value.strip().split('|')
        centroid_value = [el.strip().split(';') for el in centroid_value]
        centroid_coordinate = np.array(centroid_value, dtype='f')[:, 1].reshape(1, -1)
        return (centroid_id, list(centroid_coordinate))

def extractUserItemMatrix(user, values):
    #user, values = line.strip().split('\t')
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

def euclidean_distance_advance(user, user_rating_matrix):
    result = []
    for el in user_rating_matrix:
        result.append((user[0],(el[0],euclidean_distance(user[1], el[1]))))
    return result

def min_distance(x):
    min = x[0][1][1]
    centroid = x[0][1][0]
    for el in x:
        if el[1][1] < min: min, centroid = el[1][1], el[1][0]
    return (centroid,min)

def devideNonClusterUser (userItemMatrix_df, centroids_df):
    spark = SparkSession.builder.appName("calDistance").getOrCreate()

    userItemData = spark.createDataFrame(userItemMatrix_df)
    UserItemRDD_tab = userItemData.rdd
    UserItemRDD = userItemData.rdd.map(lambda x: extractUserItemMatrix(x[0],x[1]))

    CentroidsRDD = spark.createDataFrame(centroids_df).rdd.map(lambda x: extractCentroids(x[0],x[1]))
    CentroidsList = CentroidsRDD.collect()
    # File centroid chi co nhieu trong tam
    
    preDistance = UserItemRDD.map(lambda x : (x, CentroidsList)) #([user, (rate)],[centroid, (rate)]) nhieu dong

    #Tinh khoang cach
    preDistance = preDistance.map(lambda x: (x[0][0],euclidean_distance_advance(x[0],x[1])))

    #Chia user vao cac cum
    preDistance = preDistance.map(lambda x: (x[0],min_distance(x[1])))
    assignCentroidToUser = preDistance.map(lambda x: (x[0],(str(x[1][0]) + ';' + str(x[1][1])))).toDF(["userId","centroidMin_Distan"])
    assignCentroidToUser = assignCentroidToUser.toPandas()

    #Gan nhan theo dinh dang
    userItemLabel = UserItemRDD_tab.join(preDistance).map(lambda x : (x[0],(str(x[1][0])+'&'+str(x[1][1][0]))))
    userItemLabel = userItemLabel.toDF(["user","label"])
    userItemLabel = userItemLabel.toPandas()

    spark.stop()
    return userItemLabel, assignCentroidToUser
    
if  __name__ == "__main__":
    spark = SparkSession.builder.appName("calDistance").getOrCreate()
    userItemMatrixFile = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    centroidsFile = "hdfs://localhost:9000/Clustering/Centroids.csv"
    outputFile = "hdfs://localhost:9000/Clustering/ToClusterUser.csv"
    user_in_cluster_path = "hdfs://localhost:9000/Clustering/UserItemMatrixLabel.csv"
    devideNonClusterUser (spark, userItemMatrixFile, centroidsFile, outputFile, user_in_cluster_path)
    
    spark.stop()
