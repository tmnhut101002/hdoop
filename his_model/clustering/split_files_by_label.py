from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import shutil

def insertDataToDB(spark, mysql_url, mysql_properties, table_name):
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    input_file_rdd = df.rdd.map(lambda row: (row.user_id, row.item_id, row.rating, row.timestamp))
    cols = ["user_id","item_id", "rating", "timestamp"]
    input_file = input_file_rdd.toDF(cols)

    label_rdd = spark.sparkContext.textFile('hdfs:///HM_clustering/Label').map(lambda x: x.strip().split('\t')).map(lambda x: (str(x[0]),x[1]))
    labels = label_rdd.toDF(["user_", "label"])

    joined_df = labels.join(input_file, input_file.user_id == labels.user_).select(
                        input_file.user_id,
                        input_file.item_id,
                        input_file.rating,
                        input_file.timestamp,
                        labels.label)
    
    joined_df.write.jdbc(
        url=mysql_url,
        table="TrainingData_Cluster",
        mode="overwrite",
        properties=mysql_properties
    )
    
    avg_rating_df = joined_df.groupBy("user_id", "label").agg(F.avg("rating").alias("avg_rating"))
    
    avg_rating_df.write.jdbc(
        url=mysql_url,
        table="AVGRating",
        mode="overwrite",
        properties=mysql_properties
    )
    spark.stop()
    
def splitInput(mysql_url, mysql_properties):
    spark = SparkSession.builder.appName("SplitInput").getOrCreate()
    table_name = "TrainingData_Cluster"
    df_input = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    df_input = df_input.toPandas()
    
    grouped = df_input.groupby('label')

    for label, group in grouped:
        filename = f"../mfps_v2/temp_preSIM/input_{label}.txt"
        
        with open(filename, 'w') as file:
            for index, row in group.iterrows():
                user_item = f"{row['user_id']};{row['item_id']}"
                rating_timestamp = f"{row['rating']};{row['timestamp']}"
                file.write(f"{user_item}\t{rating_timestamp}\n")  
    spark.stop()

def splitAVG(mysql_url, mysql_properties):
    spark = SparkSession.builder.appName("SplitAVG").getOrCreate()
    table_name = "AVGRating"
    df_avg = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    df_avg = df_avg.toPandas()
    
    grouped = df_avg.groupby('label')
    for label, group in grouped:
        filename = f"../mfps_v2/temp_preSIM/avg_{label}.txt"
        
        with open(filename, 'w') as file:
            for index, row in group.iterrows():
                user = f"{row['user_id']}"
                avg_rating = f"{row['avg_rating']}"
                file.write(f"{user}\t{avg_rating}\n")
                
    spark.stop() 

def clear_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)
    
if __name__ == "__main__":
    mysql_url = "jdbc:mysql://localhost:3306/ecommerce?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    table_name = "TrainingData"
    spark = SparkSession.builder \
        .appName("SplitFile") \
        .getOrCreate()
    insertDataToDB(spark, mysql_url, mysql_properties, table_name)
    output_directory = "../mfps_v2/temp_preSIM"
    clear_directory(output_directory)
    splitInput(mysql_url, mysql_properties)
    splitAVG(mysql_url, mysql_properties)
    
