from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import shutil

def splitClusterInfo(spark, mysql_url, mysql_properties, table_name):
    # Đọc bảng huấn luyện
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    input_file_rdd = df.rdd.map(lambda row: (row.user_id, row.item_id, row.rating, row.timestamp))
    cols = ["user_id","item_id", "rating", "timestamp"]
    input_file = input_file_rdd.toDF(cols)

    # Đọc kết quả cụm
    label_rdd = spark.sparkContext.textFile('hdfs:///HM_clustering/Label').map(lambda x: x.strip().split('\t')).map(lambda x: (str(x[0]),x[1]))
    labels = label_rdd.toDF(["user_", "label"])

    joined_df = labels.join(input_file, input_file.user_id == labels.user_).select(
                        input_file.user_id,
                        input_file.item_id,
                        input_file.rating,
                        input_file.timestamp,
                        labels.label)

    # Tính rating trung bình
    avg_rating_df = joined_df.groupBy("user_id", "label").agg(F.avg("rating").alias("avg_rating"))
    
    output_dir = '../mfps/temp_preSIM'
    def split_and_save(df, file_prefix):
        pandas_df = df.toPandas()
        grouped = pandas_df.groupby('label')
        for label, group in grouped:
            filename = os.path.join(output_dir, f"{file_prefix}_{label}.txt")
            with open(filename, 'w') as file:
                for _, row in group.iterrows():
                    if file_prefix == 'input':
                        user_item = f"{row['user_id']};{row['item_id']}"
                        rating_timestamp = f"{row['rating']};{row['timestamp']}"
                        file.write(f"{user_item}\t{rating_timestamp}\n")
                    elif file_prefix == 'avg':
                        user = f"{row['user_id']}"
                        avg_rating = f"{row['avg_rating']}"
                        file.write(f"{user}\t{avg_rating}\n")
    
    # Xóa output cụm cũ
    clear_directory(output_dir)
    
    # Lưu file output đã tách cụm
    split_and_save(joined_df, 'input')
    split_and_save(avg_rating_df, 'avg')
    
    spark.stop()

# Xóa dữ liệu cũ đã tách 
def clear_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)
    
if __name__ == "__main__":
    
    host = 'mysql-ecommerce-nhut0789541410-f8ba.e.aivencloud.com'
    port = '27163'
    user = 'avnadmin'
    password = 'AVNS_SQHY8Ivz7J5kp9ElUF2'
    
    mysql_url = f"jdbc:mysql://{host}:{port}/ecommerce?useSSL=false"
    mysql_properties = {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name = 'TrainingData'
    spark = SparkSession.builder \
        .appName("SplitFile") \
        .getOrCreate()
    splitClusterInfo(spark, mysql_url, mysql_properties, table_name)