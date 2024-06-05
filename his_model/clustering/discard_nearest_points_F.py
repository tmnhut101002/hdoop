from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, split, explode, concat_ws
import os

def discard_nearest_points(spark0, input_path_F, input_path_nearest_points, output_path):

    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()
    # Read the M_nearest_points data
    nearest_points_data = spark.read.text(input_path_nearest_points).rdd.map(lambda r: r[0])
    nearest_points_rdd = nearest_points_data.map(lambda line: tuple(map(float, line.split('\t'))))

    # Read the F data
    F_data = spark.read.text(input_path_F).rdd.map(lambda r: r[0])
    F_rdd = F_data.map(lambda line: tuple(map(int, line.split('\t'))))

    # Convert RDDs to DataFrames
    F_df = spark.createDataFrame(F_rdd, ["user", "value_F"])
    nearest_points_df = spark.createDataFrame(nearest_points_rdd, ["user", "nearest_points_value"])

    # Perform an anti-join to filter out users in M_nearest_points from user-item matrix
    result_df = F_df.join(nearest_points_df, on="user", how="left_anti")

    # Collect the results
    result_df = result_df.groupBy("user").agg(collect_list("value_F").alias("result_value")).rdd.map(lambda x: (x['user'], x["result_value"][0])).toDF(['user','value'])


    result_df.write.mode('overwrite').options(header = "False", delimiter = '\t').csv(output_path)

    spark.stop()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()

    input_path_matrix = "hdfs:///Clustering_mysql/Importance"
    input_path_nearest_points = "hdfs:///Clustering_mysql/M_NearestPoints"
    output_path = "hdfs:///Clustering_mysql/Importance"

    discard_nearest_points(spark, input_path_matrix, input_path_nearest_points, output_path)

    spark.stop()
