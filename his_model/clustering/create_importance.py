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

def createImportance(spark, mysql_url, mysql_properties, output_path, table_name):
    # Lấy dữ liệu rating
    input_data = spark.read.jdbc(mysql_url, table_name, properties=mysql_properties)
    input_rdd = input_data.rdd.map(lambda row: (row.user_id, row.rating))
    
    # Tính độ quan trọng
    result = input_rdd.map(importance_mapper) \
                    .groupByKey() \
                    .map(lambda x: importance_reducer(x[0], x[1]))
    
    # Ghi kết quả vào HDFS
    output = result.toDF()
    output.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_path)
    spark.stop()
