from pyspark.sql import SparkSession
import numpy as np
import os
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

def devideNonClusterUser (userItemMatrixFile, centroidsFile, user_in_cluster_path, centroidsList = None):
    spark = SparkSession.builder.appName("calDistance").getOrCreate()
    # Input
    userItemData = spark.sparkContext.textFile(userItemMatrixFile)
    UserItemRDD_tab = userItemData.map(lambda x: x.split('\t'))
    UserItemRDD = userItemData.map(lambda x: extractUserItemMatrix(x))
    
    if centroidsList is None:
        CentroidsRDD = spark.sparkContext.textFile(centroidsFile).map(lambda x: extractCentroids(x))
        centroidsList = CentroidsRDD.collect()
    
    # Input centroid có nhiều trọng tâm
    preDistance = UserItemRDD.map(lambda x : (x, centroidsList)) #([user, (rate)],[centroid, (rate)])

    #Tính khoảng cách
    preDistance1 = preDistance.map(lambda x: (x[0][0],euclidean_distance_advance(x[0],x[1])))

    # Chia user vào cụm
    preDistance1 = preDistance1.map(lambda x: (x[0],min_distance(x[1])))

    # Cập nhật kết quả vào HDFS
    userItemLabel = UserItemRDD_tab.join(preDistance1).map(lambda x : (x[0],(str(x[1][0])+'&'+str(x[1][1][0]))))
    userItemLabel.toDF(["user","rating&label"]).write.mode("overwrite").options(header='False', delimiter = '\t').csv(user_in_cluster_path)
    
    labelItemRating = UserItemRDD_tab.join(preDistance1).map(lambda x : (str(x[1][1][0]),str(x[1][0])))
    labelItemRating = labelItemRating.toDF(["label","itemRating"])
    labelItemRating = labelItemRating.toPandas()

    spark.stop()
    return labelItemRating

if  __name__ == "__main__":
    userItemMatrixFile = "hdfs://localhost:9000/HM_clustering/UserItemMatrixCluster"
    centroidsFile = "hdfs://localhost:9000/HM_clustering/Centroids"
    outputFile = "hdfs://localhost:9000/HM_clustering/ToClusterUser"
    user_in_cluster_path = "hdfs://localhost:9000/HM_clustering/UserItemMatrixLabel"
    labelItemRating = devideNonClusterUser (userItemMatrixFile, centroidsFile, user_in_cluster_path)