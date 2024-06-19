from pyspark.sql import SparkSession

def extract(line):
    item = line.strip().split('\t')[0].strip().split(';')[1]
    return item, None

def createItemList(table_name, mysql_url, mysql_properties):
    spark = SparkSession.builder \
        .appName("ItemList") \
        .getOrCreate()
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    
    # Chuyển đổi dữ liệu thành định dạng key-value
    items = df.rdd.map(lambda row: (row.item_id, None))

    # Loại bỏ các giá trị trùng lặp
    unique_items = items.distinct().map(lambda x: (x[0], 1))


    # Ghi kết quả vào tệp items.txt
    result_data = unique_items.toDF(["item","1"]).select(["item"])
    result_data = [(row.item) for row in result_data.collect()]

    spark.stop()
    return result_data

if __name__ == '__main__':
    # Tạo một phiên Spark
    spark = SparkSession.builder.appName("ItemList").getOrCreate()
    input_file = "../input_file.txt"

    iList = createItemList(input_file)
    # Dừng phiên Spark
    spark.stop()
