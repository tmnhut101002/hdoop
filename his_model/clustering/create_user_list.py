from pyspark.sql import SparkSession

def createUserList(spark, mysql_url, mysql_properties, output_file, table_name):
    
    # Lấy dữ liệu rating
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)

    # Lấy dữ liệu từ DataFrame và chuyển đổi thành định dạng key-value
    users = df.rdd.map(lambda row: (row.user_id, None))

    # Loại bỏ các giá trị trùng lặp
    unique_users = users.distinct().map(lambda x: (x[0], 1))

    # Ghi kết quả vào HDFS
    result_data = unique_users.toDF(["user", "1"]).select(["user"])
    result_data.write.mode('overwrite').options(header='False').csv(output_file)

    spark.stop()

