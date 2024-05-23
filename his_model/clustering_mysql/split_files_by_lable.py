from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def insertDataToDB(spark, mysql_url, mysql_properties):
    # spark = SparkSession.builder.appName("clusteringForRS").getOrCreate()
    table_name = 'TrainingData'

    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    input_file_rdd = df.rdd.map(lambda row: (row.user_id, row.item_id, row.rating, row.review_date))
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
        table="TrainingData_1",
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

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SplitFile") \
        .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
        .getOrCreate()

    mysql_url = "jdbc:mysql://localhost:3306/ecommerce?useSSL=false"
    mysql_properties = {
        "user": "root",
        "password": "Password@123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    insertDataToDB(spark, mysql_url, mysql_properties)
