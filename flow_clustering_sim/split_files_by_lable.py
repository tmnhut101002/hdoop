import pandas as pd
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import column as col

def split_files_by_label_to_hdfs(input_file_path):
    # Connect to HDFS
    client = InsecureClient("http://localhost:9870", user="hdoop")
    spark = SparkSession.builder.appName("clusteringForRS").getOrCreate()
    # Read the input file from HDFS
    input_rdd = spark.sparkContext.textFile(input_file_path).map(lambda x: x.strip().split('\t'))
    input_rdd = input_rdd.map(lambda x: (x[0].strip().split(';'),x[1])).map(lambda x: (str(x[0][0]),x[0][1],x[1]))
    inputList = input_rdd.collect()
    input_file = input_rdd.toDF(["user", "item", "value"])

    label_rdd = spark.sparkContext.textFile('hdfs://localhost:9000/Clustering/Label.csv').map(lambda x: x.strip().split('\t')).map(lambda x: (str(x[0]),x[1]))

    labels = label_rdd.toDF(["user_", "label"])

    joined_df = labels.join(input_file, input_file.user == labels.user_)

    avg_ratings_rdd = spark.sparkContext.textFile('hdfs://localhost:9000/Clustering/AverageRating.csv').map(lambda x: x.strip().split('\t')).map(lambda x: (str(x[0]),x[1]))
    avg_ratings = avg_ratings_rdd.toDF(["user", "avg_rating"])
    avg_ratings = avg_ratings.join(labels, avg_ratings.user ==  labels.user_)

    centroids_rdd = spark.sparkContext.textFile('hdfs://localhost:9000/Clustering/Centroids.csv').map(lambda x: x.strip().split('\t')[0]).map(lambda x: (str(x), 1))
    centroids = centroids_rdd.toDF(['0','1'])
    centroids_l = centroids.select(['0']).collect()
    for index, value in enumerate(centroids_l):
        k = joined_df.collect()
        
        a = joined_df.collect()
        input_file_ = joined_df.filter(joined_df.label == value[0]).collect()

        input_file_i = joined_df.filter(joined_df.label == value[0]).toPandas()
        
        input_file_i = input_file_i.drop(columns = ["user_", "label"])
        input_file_i['user_item'] = input_file_i['user'] + ';' + input_file_i['item']
        input_file_i = input_file_i.drop(columns = ["user", "item"])
        column_names = ['user_item', 'value']
        input_file_i = input_file_i.reindex(columns=column_names)
        
        # Export input file to HDFS
        with client.write(f"/new_data/clustering/input_file_{index}.txt", encoding="utf-8", overwrite= True) as writer:
            input_file_i.to_csv(writer, sep="\t", index=False, header=False)

        a = avg_ratings.collect()
        avg_ratings_i = avg_ratings.filter(col("label") == value[0])
        k = avg_ratings_i.collect()
        
        avg_ratings_i = avg_ratings.filter(col("label") == value[0]).toPandas()
        avg_ratings_i = avg_ratings_i.drop(columns = ["user_", "label"])

        # Export average ratings to HDFS
        with client.write(f"/new_data/clustering/avg_ratings_{index}.txt", encoding="utf-8", overwrite= True) as writer:
            avg_ratings_i.to_csv(writer, sep="\t", index=False, header=False)

if __name__ == "__main__":
    input_file_path = "file:///home/hdoop/input_file_1k.txt"
    split_files_by_label_to_hdfs(input_file_path)
