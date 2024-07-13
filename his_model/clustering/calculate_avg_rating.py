from pyspark.sql import SparkSession

def calculateAvgRating(spark, mysql_url, mysql_properties, output_file, table_name):
    
    # Lấy dữ liệu rating
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)

    # Lấy dữ liệu từ DataFrame và chuyển đổi thành định dạng key-value
    ratings = df.rdd.map(lambda row: (row.user_id, row.rating))

    # Tính rating trung bình
    user_totals = ratings.aggregateByKey((0, 0), lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
    avg_ratings = user_totals.map(lambda x: (x[0], x[1][0] / float(x[1][1])))
    
    # Ghi kết quả vào HDFS
    result_data = avg_ratings.toDF(["User", "Average Rating"])
    result_data.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_file)
    
    spark.stop()
