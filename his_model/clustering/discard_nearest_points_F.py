from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

def discard_nearest_points(input_path_F, input_path_nearest_points, output_path):

    spark = SparkSession.builder.appName("DiscardNearestPoints").getOrCreate()
    # Đọc User x Item
    nearest_points_data = spark.read.text(input_path_nearest_points).rdd.map(lambda r: r[0])
    nearest_points_rdd = nearest_points_data.map(lambda line: line.split('\t'))

    # Đọc F
    F_data = spark.read.text(input_path_F).rdd.map(lambda r: r[0])
    F_rdd = F_data.map(lambda line: line.split('\t'))

    # Chuyển RDDs thành DataFrames
    F_df = spark.createDataFrame(F_rdd, ["user", "value_F"])
    nearest_points_df = spark.createDataFrame(nearest_points_rdd, ["user", "nearest_points_value"])

    # Tạo df kết quả và ghi vào HDFS
    result_df = F_df.join(nearest_points_df, on="user", how="left_anti")
    result_df = result_df.groupBy("user").agg(collect_list("value_F").alias("result_value")).rdd.map(lambda x: (x['user'], x["result_value"][0])).toDF(['user','value'])
    result_df.write.mode('overwrite').options(header = "False", delimiter = '\t').csv(output_path)
    spark.stop()
