from pyspark.sql import SparkSession
import numpy as np

def extractUserItemMatrix(line):
    user, values = line.strip().split('\t')
    values, label = values.strip().split('&')
    coordinate = values.strip().split('|')
    coordinate = [el.strip().split(';') for el in coordinate]
    coordinate = np.array(coordinate, dtype='f')[:, 1].reshape(1, -1)
    return (label, list(coordinate))

def sum2Array(arr1, arr2):
    result = [x+y for x,y in zip(arr1,arr2)]
    return result
        

def updateCentroids(user_item_label_matrix, output):
    spark = SparkSession.builder.appName('NewCentroids').getOrCreate()

    userItemLabelRDD = spark.sparkContext.textFile(user_item_label_matrix)
    labelUser = userItemLabelRDD.map(lambda x: extractUserItemMatrix(x))
    a = labelUser.collect()
    b = labelUser.reduceByKey(lambda x,y: (x + y)).collect()
    c= labelUser.reduceByKey(lambda x,y: (x + y)).map(lambda x : (x[0],len(x[1]))).collect()
    d = labelUser.reduceByKey(lambda x,y: sum2Array(x,y)).collect()
    e = 1

if __name__ == "__main__":
    user_item_label_matrix = './output/user_item_matrix_label.txt'
    updateCentroids(user_item_label_matrix,0)
