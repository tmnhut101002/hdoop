from pyspark.sql import SparkSession

def createUserList(spark, mysql_url, mysql_properties, output_file, table_name):
    
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)

    # Lấy dữ liệu từ DataFrame và chuyển đổi thành định dạng key-value
    users = df.rdd.map(lambda row: (row.user_id, None))

    # Loại bỏ các giá trị trùng lặp
    unique_users = users.distinct().map(lambda x: (x[0], 1))

    # Ghi kết quả vào tệp users.csv
    result_data = unique_users.toDF(["user", "1"]).select(["user"])
    result_data.write.mode('overwrite').options(header='False').csv(output_file)

    spark.stop()

if __name__ == '__main__':
    mysql_url = "jdbc:mysql://localhost:3306/ML100?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name = 'TrainingData'

    spark = SparkSession.builder \
        .appName("UserList") \
        .getOrCreate()
    output_file = "hdfs://localhost:9000/HM_clustering/User"
    createUserList(spark, mysql_url, mysql_properties, output_file, table_name)

