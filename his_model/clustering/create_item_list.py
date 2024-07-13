from pyspark.sql import SparkSession

def createItemList(spark, mysql_url, mysql_properties, output_file, table_name):
    # Lấy dữ liệu rating
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)

    # Chuyển đổi dữ liệu thành định dạng key-value
    items = df.rdd.map(lambda row: (row.item_id, None))

    # Loại bỏ các giá trị trùng lặp
    unique_items = items.distinct().map(lambda x: (x[0], 1))

    # Ghi kết quả vào HDFS
    result_data = unique_items.toDF(["item","1"]).select(["item"])
    result_data.write.mode('overwrite').options(header='False').csv(output_file)
    
    spark.stop()
