from pyspark.sql import SparkSession

def createItemList(spark, mysql_url, mysql_properties, output_file, table_name):
    
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)

    # Chuyển đổi dữ liệu thành định dạng key-value
    items = df.rdd.map(lambda row: (row.item_id, None))

    # Loại bỏ các giá trị trùng lặp
    unique_items = items.distinct().map(lambda x: (x[0], 1))

    # Ghi kết quả vào tệp items.txt
    result_data = unique_items.toDF(["item","1"]).select(["item"])

    result_data.write.mode('overwrite').options(header='False').csv(output_file)

if __name__ == '__main__':
    mysql_url = "jdbc:mysql://localhost:3306/ML100?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name = 'TrainingData'

    spark = SparkSession.builder \
        .appName("ItemList") \
        .getOrCreate()
    output_file = "hdfs://localhost:9000/HM_clustering/Item"
    createItemList(spark, mysql_url, mysql_properties, output_file, table_name)
