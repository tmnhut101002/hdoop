from pyspark.sql import SparkSession

def importance_mapper(df):
    user = df[0]
    rating = df[1]
    return user, rating

def importance_reducer(user, ratings):
    ratings_list = list(ratings)
    numbers_of_rating = len(ratings_list)
    types_of_rating = len(set(ratings_list))
    user_importance = numbers_of_rating + types_of_rating

    return user, f'{user_importance}'

def createImportance(table_name, mysql_url, mysql_properties):
    spark = SparkSession.builder \
        .appName("CalImportance") \
        .getOrCreate()
    df = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    # Read input data
    input_rdd = df.rdd.map(lambda row: (row.user_id, row.rating))

    # Map and reduce
    result = input_rdd.map(importance_mapper) \
                    .groupByKey() \
                    .map(lambda x: importance_reducer(x[0], x[1]))

    result = result.toDF()
    result = result.toPandas()

    spark.stop()
    return result


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Importance").getOrCreate()

    input_path = "../input_file.txt" 
    output_path = "hdfs://localhost:9000/Clustering/Importance.csv"

    createImportance(spark,input_path,output_path)
    

    spark.stop()
