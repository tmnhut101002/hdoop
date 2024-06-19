from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, collect_list, split, explode, concat_ws

def discard_nearest_points(uiMatrix_df, nearestPoints_df):
    conf = SparkConf() \
        .setAppName("DiscardNearestPoints") \
        .set("spark.executor.memory", "5g") \
        .set("spark.driver.memory", "5g") \
        .set("spark.executor.cores", "2") \
        .set("spark.driver.cores", "2") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .set("spark.kryoserializer.buffer.max", "512m") \
        .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
        .set("spark.network.timeout", "3601s") \
        .set("spark.executor.heartbeatInterval", "3600s") \
        .setMaster("local[2]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    num_partitions = 50

    matrix_df = spark.createDataFrame(uiMatrix_df, ["user", "matrix_value"]).repartition(num_partitions)
    nearest_points_df = spark.createDataFrame(nearestPoints_df, ["user", "nearest_points_value"]).repartition(num_partitions)
    
    matrix_df.cache()
    nearest_points_df.cache()
    #get centroid
    centroid_id = nearest_points_df.filter(col("nearest_points_value") == 0).select("user").collect()[0].user

    # Split the matrix_value column into an array of values
    matrix_df = matrix_df.withColumn("matrix_values", split(col("matrix_value"), "\\|"))
    matrix_df = matrix_df.select("user", explode("matrix_values").alias("matrix_value"))

    # Join matrix_df with nearest_points_df
    result_df0 = matrix_df.join(nearest_points_df, on="user")

    # Aggregate data to reduce its size
    result_discard_df = result_df0.groupBy("user").agg(concat_ws("|", collect_list("matrix_value")).alias("result_value"))
    
    # Add label to users in the cluster
    user_in_cluster_with_label_rdd = result_discard_df.rdd.map(lambda row: (row['user'], row['result_value'] + '&' + str(centroid_id)))
    user_in_cluster_with_label_df = user_in_cluster_with_label_rdd.toDF(["user", "label"])

    # Filter out users in nearest_points_df from matrix_df
    result_df = matrix_df.join(nearest_points_df, on="user", how="left_anti")

    # Aggregate the remaining matrix values
    result_df = result_df.groupBy("user").agg(concat_ws("|", collect_list("matrix_value")).alias("values"))

    # Convert results to Pandas DataFrames
    user_in_cluster_with_label_df = user_in_cluster_with_label_df.toPandas()
    result_df = result_df.select(col('user').alias('_1'), col('values').alias('_2')).toPandas()
    
    matrix_df.unpersist()
    nearest_points_df.unpersist()
    result_df0.unpersist()
    result_discard_df.unpersist()
        
    spark.stop()
    return user_in_cluster_with_label_df, result_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()
    
    input_path_matrix = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    input_path_nearest_points = "hdfs://localhost:9000/Clustering/M_NearestPoints.csv"
    output_path = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    user_in_cluster_path = "hdfs://localhost:9000/Clustering/UserItemMatrixLabel.csv"
    
    discard_nearest_points(spark, input_path_matrix, input_path_nearest_points, output_path, user_in_cluster_path)
    
    spark.stop()
