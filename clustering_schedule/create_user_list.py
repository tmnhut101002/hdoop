from pyspark.sql import SparkSession

def extract(line):
    user = line.strip().split('\t')[0].strip().split(';')[0]
    return user, None

def createUserList(spark0, input_file, output_file):
    # Đọc dữ liệu từ tệp đầu vào
    spark = SparkSession.builder.appName("CreateUserList").getOrCreate()
   
    lines = spark.sparkContext.textFile(input_file)

    # Chuyển đổi dữ liệu thành định dạng key-value
    users = lines.map(extract)

    # Loại bỏ các giá trị trùng lặp
    unique_users = users.distinct().map(lambda x: (x[0], 1))


    # Ghi kết quả vào tệp users.txt
    result_data = unique_users.toDF(["user","1"]).select(["user"])

    result_data.write.mode('overwrite').options(header='False').csv(output_file)
    spark.stop()

    # with open(output_file, "w") as output_file:
    #     for user in result_data:
    #         output_file.write(f"{user[0]}\n")

if __name__ == '__main__':
    # Tạo một phiên Spark
    spark = SparkSession.builder.appName("UserList").getOrCreate()
    input_file = "file:///home/hdoop/input_file.txt"
    output_file = "hdfs://localhost:9000/airflow_schedule/User"

    createUserList(spark,input_file,output_file)
    # Dừng phiên Spark
    spark.stop()
