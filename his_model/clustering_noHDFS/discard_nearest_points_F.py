from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

def discard_nearest_points(importance_df, mNearestPoint_df):

    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()

    # Convert RDDs to DataFrames
    F_df = spark.createDataFrame(importance_df, ["user", "value_F"])
    nearest_points_df = spark.createDataFrame(mNearestPoint_df, ["user", "nearest_points_value"])

    # Perform an anti-join to filter out users in M_nearest_points from user-item matrix
    result_df = F_df.join(nearest_points_df, on="user", how="left_anti")

    # Collect the results
    result_df = result_df.groupBy("user").agg(collect_list("value_F").alias("result_value")).rdd
    # result_list = result_df.collect()
    result_df = result_df.map(lambda x: (x['user'], x["result_value"][0])).toDF()
    result_df = result_df.toPandas()

    spark.stop()
    return result_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()
    
    input_path_matrix = "hdfs://localhost:9000/Clustering/Importance.csv"
    input_path_nearest_points = "hdfs://localhost:9000/Clustering/M_NearestPoints.csv"
    output_path = "hdfs://localhost:9000/Clustering/Importance.csv"
    
    discard_nearest_points(spark, input_path_matrix, input_path_nearest_points, output_path)
    
    spark.stop()
