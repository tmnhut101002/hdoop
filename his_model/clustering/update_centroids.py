from pyspark.sql import SparkSession
import numpy as np

def extractLabel2UpdateCentroid(centroid_id, centroid_value):
    centroid_value = centroid_value.strip().split('|')
    centroid_value = [el.strip().split(';') for el in centroid_value]
    centroid_coordinate = np.array(centroid_value, dtype='f')[:, 1].reshape(1, -1)
    return (centroid_id, np.array(centroid_coordinate))

def updateCentroid (labelItemRating_df):
    spark = SparkSession.builder.appName('updateCentroids').getOrCreate()

    label_rdd = spark.createDataFrame(labelItemRating_df)

    countKey = label_rdd.rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+y)

    label_rdd = label_rdd.rdd.map(lambda x : extractLabel2UpdateCentroid(x[0],x[1])).reduceByKey(lambda x,y: x+y)

    label_rdd = label_rdd.join(countKey)

    label_rdd = label_rdd.mapValues(lambda x: x[0]/x[1])
    # label_rdd = label_rdd.mapValues(lambda x : np.array(x[1]))
    label_keys = label_rdd.keys().collect()
    label_values = label_rdd.values().collect()
    label = label_rdd.collect()
    spark.stop()
    return label_keys, np.array(label_values), label
#aa = updateCentroid(labelItemRating)