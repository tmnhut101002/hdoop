from pyspark.sql import SparkSession


def extract(key, value):
    # key, value = line.strip().split('\t')
    item_rate, centroid = value.strip().split('&')
    return (key, centroid)

def simpleLabel(uLabel_df, output_user_label):
    spark = SparkSession.builder.appName('label').getOrCreate()

    user_item_label_rdd = spark.createDataFrame(uLabel_df).rdd.map(lambda x: extract(x[0],x[1]))

    with open(output_user_label, "w") as output:
        for user, centroid in user_item_label_rdd.collect():
            output.write(f'{user}\t{centroid}\n')
    
    output.close()
    spark.stop()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('label').getOrCreate()
    user_in_cluster_path = "hdfs://localhost:9000/Clustering/UserItemMatrixLabel.csv"
    user_label = "hdfs://localhost:9000/Clustering/Label.csv"
    simpleLabel(spark, user_in_cluster_path, user_label)
    spark.stop()

