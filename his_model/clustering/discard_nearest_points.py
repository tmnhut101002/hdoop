from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, split, explode, concat_ws

def discard_nearest_points(input_path_matrix, input_path_nearest_points, UImatrix_output_path, user_in_cluster_output_path):
    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()
    # Đọc User x Item
    matrix_data = spark.read.text(input_path_matrix).rdd.map(lambda r: r[0])
    matrix_rdd = matrix_data.map(lambda line: tuple(line.split('\t')))

    # Đọc M_nearest_points
    nearest_points_data = spark.read.text(input_path_nearest_points).rdd.map(lambda r: r[0])
    nearest_points_rdd = nearest_points_data.map(lambda line: tuple(map(float, line.split('\t'))))

    # Chuyển RDDs thành DataFrames
    matrix_df = spark.createDataFrame(matrix_rdd, ["user", "matrix_value"])
    nearest_points_df = spark.createDataFrame(nearest_points_rdd, ["user", "nearest_points_value"])

    # Tách cột matrix_value thành một mảng các giá trị
    matrix_df = matrix_df.withColumn("matrix_values", split(col("matrix_value"), "\\|"))

    # Phân tách mảng thành các hàng riêng biệt
    matrix_df = matrix_df.select("user", explode("matrix_values").alias("matrix_value"))

    # Tạo df kết quả và ghi vào HDFS
    result_df = matrix_df.join(nearest_points_df, on="user", how="left_anti")
    result_df = result_df.groupBy("user").agg(concat_ws("|", collect_list("matrix_value")).alias("result_value"))
    result_df.write.mode("overwrite").options(header="False", delimiter = '\t').csv(UImatrix_output_path)
    spark.stop()
