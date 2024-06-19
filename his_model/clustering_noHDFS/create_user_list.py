from pyspark.sql import SparkSession

def extract(line):
    user = line.strip().split('\t')[0].strip().split(';')[0]
    return user, None

def createUserList(table_name, mysql_url, mysql_properties):
    # Đọc dữ liệu từ tệp đầu vào
    spark = SparkSession.builder \
        .appName("UserList") \
        .getOrCreate()
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)

    # Lấy dữ liệu từ DataFrame và chuyển đổi thành định dạng key-value
    users = df.rdd.map(lambda row: (row.user_id, None))

    # Loại bỏ các giá trị trùng lặp
    unique_users = users.distinct().map(lambda x: (x[0], 1))

    # Ghi kết quả vào tệp users.csv
    result_data = unique_users.toDF(["user","1"]).select(["user"])
    result_data = result_data.toPandas()

    spark.stop()
    return result_data

if __name__ == '__main__':
    # Tạo một phiên Spark
    spark = SparkSession.builder.appName("UserList").getOrCreate()
    input_file = "../input_file.txt"
    output_file = "hdfs://localhost:9000/Clustering/User.csv"

    uList = createUserList(input_file)
    # Dừng phiên Spark
    spark.stop()
