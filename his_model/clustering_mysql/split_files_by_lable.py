import pandas as pd
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import column as col

def split_files_by_label_to_hdfs(spark, mysql_url, mysql_properties):
    # Connect to HDFS
    client = InsecureClient("http://localhost:9870", user="hdoop")
    spark = SparkSession.builder.appName("clusteringForRS").getOrCreate()
    table_name = 'MovieLen100k_training'

    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    input_file_rdd = df.rdd.map(lambda row: (row.user_id, row.item_id, row.rating, row.timestamp))
    cols = ["user_id","item_id", "rating", "timestamp"]
    input_file = input_file_rdd.toDF(cols)

    label_rdd = spark.sparkContext.textFile('hdfs:///Clustering_mysql/Label').map(lambda x: x.strip().split('\t')).map(lambda x: (str(x[0]),x[1]))
    labels = label_rdd.toDF(["user_", "label"])

    joined_df = labels.join(input_file, input_file.user_id == labels.user_)

    avg_ratings_rdd = spark.sparkContext.textFile('hdfs:///Clustering_mysql/AverageRating').map(lambda x: x.strip().split('\t')).map(lambda x: (str(x[0]),x[1]))
    avg_ratings = avg_ratings_rdd.toDF(["user", "avg_rating"])

    with client.write(f"/new_data/clustering/avg_ratings_full.txt", encoding="utf-8", overwrite= True) as writer:
        avg_ratings.toPandas().to_csv(writer, sep="\t", index=False, header=False)
        
    avg_ratings = avg_ratings.join(labels, avg_ratings.user ==  labels.user_)

    centroids_rdd = spark.sparkContext.textFile('hdfs:///Clustering_mysql/Centroids').map(lambda x: x.strip().split('\t')[0]).map(lambda x: (str(x), 1))
    centroids = centroids_rdd.toDF(['0','1'])
    centroids_l = centroids.select(['0']).collect()
    for index, value in enumerate(centroids_l):
        input_file_i = joined_df.filter(joined_df.label == value[0]).toPandas()

        input_file_i = input_file_i.drop(columns = ["user_", "label"])
        input_file_i['user_item'] = input_file_i['user_id'] + ';' + input_file_i['item_id']
        input_file_i['rating_timestamp'] = input_file_i['rating'].astype(str) + ';' + input_file_i['timestamp'].astype(str)
        input_file_i = input_file_i.drop(columns = ["user_id", "item_id", 'rating', 'timestamp'])
        column_names = ['user_item', 'rating_timestamp']
        input_file_i = input_file_i.reindex(columns=column_names)

        # Export input file to HDFS
        # with client.write(f"/new_data/clustering/input_file_{index}.txt", encoding="utf-8", overwrite= True) as writer:
        #     input_file_i.to_csv(writer, sep="\t", index=False, header=False)

        

        # avg_ratings_i = avg_ratings.filter(col("label") == value[0])
        # avg_ratings_i = avg_ratings.filter(col("label") == value[0]).toPandas()
        # avg_ratings_i = avg_ratings_i.drop(columns = ["user_", "label"])

        # # Export average ratings to HDFS
        # with client.write(f"/new_data/clustering/avg_ratings_{index}.txt", encoding="utf-8", overwrite= True) as writer:
        #     avg_ratings_i.to_csv(writer, sep="\t", index=False, header=False)
    

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SplitFile") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()

    mysql_url = "jdbc:mysql://localhost:3306/ML100?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    split_files_by_label_to_hdfs(spark, mysql_url, mysql_properties)
