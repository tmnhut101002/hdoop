from pyspark.sql import SparkSession

def extract(line):
    key, value = line.strip().split('\t')
    item_rate, centroid = value.strip().split('&')
    return (key, centroid)

def simpleLabel(user_in_cluster_path, output_user_label):
    spark = SparkSession.builder.appName('label').getOrCreate()
    # Làm gọn label
    user_item_label_rdd = spark.sparkContext.textFile(user_in_cluster_path).map(lambda x: extract(x))
    user_item_label_rdd.toDF(["userId", "centroid"])\
    .write.mode("overwrite").options(header='False', delimiter = '\t').csv(output_user_label)
    spark.stop()

