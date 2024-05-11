from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, split, explode, concat_ws
import os

def discard_nearest_points(spark0, input_path_matrix, input_path_nearest_points, UImatrix_output_path, user_in_cluster_output_path):
    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()
    
    # Read the user-item matrix data
    matrix_data = spark.read.text(input_path_matrix).rdd.map(lambda r: r[0])
    matrix_rdd = matrix_data.map(lambda line: tuple(line.split('\t')))

    # Read the M_nearest_points data
    nearest_points_data = spark.read.text(input_path_nearest_points).rdd.map(lambda r: r[0])
    nearest_points_rdd = nearest_points_data.map(lambda line: tuple(map(float, line.split('\t'))))

    # Convert RDDs to DataFrames
    matrix_df = spark.createDataFrame(matrix_rdd, ["user", "matrix_value"])
    nearest_points_df = spark.createDataFrame(nearest_points_rdd, ["user", "nearest_points_value"])
   
   #get centroid
    centroid_id = nearest_points_df.filter((nearest_points_df.nearest_points_value == 0)).select("user").rdd.collect() #centroid id

    # Split the matrix_value column into an array of values
    matrix_df = matrix_df.withColumn("matrix_values", split(col("matrix_value"), "\\|"))
    

    # Explode the array into separate rows
    matrix_df = matrix_df.select("user", explode("matrix_values").alias("matrix_value"))

    
    #add label to user in cluster
    result_df0 = matrix_df.join(nearest_points_df, on= "user")
    result_discard_df = result_df0.groupBy("user").agg(concat_ws("|", collect_list("matrix_value")).alias("result_value"))
    user_in_cluster_with_label_rdd= result_discard_df.rdd.mapValues(lambda x : x+'&'+str(int(centroid_id[0].user)))
    user_in_cluster_with_label_df = user_in_cluster_with_label_rdd.toDF(["user","values"])

                               
    # Perform an anti-join to filter out users in M_nearest_points from user-item matrix
    result_df = matrix_df.join(nearest_points_df, on="user", how="left_anti")
    
    # Group by user and concatenate the values into the expected format
    # result_df = result_df.groupBy("user").agg(collect_list("matrix_value").alias("values"))
    
    # Convert the aggregated values back to a delimited string
    result_df = result_df.groupBy("user").agg(concat_ws("|", collect_list("matrix_value")).alias("result_value"))


    # Collect the results
    result_rdd = result_df.rdd.map(lambda row: (row['user'], row['result_value']))


    # Write the results to the local file
    user_in_cluster_with_label_df.write.mode("append").options(header="False", delimiter = '\t').csv(user_in_cluster_output_path)
    result_df.write.mode("overwrite").options(header="False", delimiter = '\t').csv(UImatrix_output_path)

    # with open(UImatrix_output_path, "w") as out_file:
    #     for user, result_value in result_rdd.collect():
    #         result_value = result_value.split("&")[0]
    #         out_file.write(f"{user}\t{result_value}\n")  
    # out_file.close()

    # with open(user_in_cluster_output_path, "a") as out_file:
    #     for user, result_value in user_in_cluster_with_label_rdd.collect():
    #         result_value = result_value
    #         out_file.write(f"{user}\t{result_value}\n")  
    # out_file.close()
    spark.stop()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()
    
    input_path_matrix = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    input_path_nearest_points = "hdfs://localhost:9000/Clustering/M_NearestPoints.csv"
    output_path = "hdfs://localhost:9000/Clustering/UserItemMatrix.csv"
    user_in_cluster_path = "hdfs://localhost:9000/Clustering/UserItemMatrixLabel.csv"
    
    discard_nearest_points(spark, input_path_matrix, input_path_nearest_points, output_path, user_in_cluster_path)
    
    spark.stop()
