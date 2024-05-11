from pyspark.sql import SparkSession

def importance_mapper(row):
    key, value = row.strip().split('\t')
    user, _ = key.strip().split(';')
    rating, _ = value.strip().split(';')

    return user, rating

def importance_reducer(user, ratings):
    ratings_list = list(ratings)
    numbers_of_rating = len(ratings_list)
    types_of_rating = len(set(ratings_list))
    user_importance = numbers_of_rating + types_of_rating

    return user, f'{user_importance}'

def createImportance(spark1, input_path, output_path):
    spark = SparkSession.builder.appName("Importance").getOrCreate()
    # Read input data
    input_data = spark.sparkContext.textFile(input_path)

    # Map and reduce
    result = input_data.map(importance_mapper) \
                       .groupByKey() \
                       .map(lambda x: importance_reducer(x[0], x[1]))

    # Print the result to console (for testing purposes)
    output = result.toDF()

    output.write.mode('overwrite').options(header='False', delimiter='\t').csv(output_path)

    # with open(output_path, "w") as output_file:
    #     for user, importance in output:
    #         output_file.write(f"{user}\t{importance}\n")
    
    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Importance").getOrCreate()

    input_path = "../input_file.txt" 
    output_path = "hdfs://localhost:9000/Clustering/Importance.csv"

    createImportance(spark,input_path,output_path)
    

    spark.stop()
