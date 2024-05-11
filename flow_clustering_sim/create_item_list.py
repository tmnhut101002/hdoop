from pyspark.sql import SparkSession

def extract(line):
    item = line.strip().split('\t')[0].strip().split(';')[1]
    return item, None

def createItemList(spark0, input_file, output_file):
    spark = SparkSession.builder.appName("CreateUserList").getOrCreate()
    # Đọc dữ liệu từ tệp đầu vào
   
    lines = spark.sparkContext.textFile(input_file)

    # Chuyển đổi dữ liệu thành định dạng key-value
    items = lines.map(extract)

    # Loại bỏ các giá trị trùng lặp
    unique_items = items.distinct().map(lambda x: (x[0], 1))


    # Ghi kết quả vào tệp items.txt
    result_data = unique_items.toDF(["item","1"]).select(["item"])

    result_data.write.mode('overwrite').options(header='False').csv(output_file)
    spark.stop()

    # with open(output_file, "w") as output_file:
    #     for item in result_data:
    #         output_file.write(f"{item[0]}\n")

if __name__ == '__main__':
    # Tạo một phiên Spark
    spark = SparkSession.builder.appName("ItemList").getOrCreate()
    input_file = "../input_file.txt"
    output_file = "hdfs://localhost:9000/Clustering/Item.csv"

    createItemList(spark,input_file,output_file)
    # Dừng phiên Spark
    spark.stop()
