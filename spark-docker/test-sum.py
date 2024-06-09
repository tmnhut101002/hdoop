from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SumFrom1To10").getOrCreate()

numbers = spark.sparkContext.parallelize(range(1, 11))
sum_of_numbers = numbers.sum()
print(f"The sum of numbers from 1 to 10 is: {sum_of_numbers}")
spark.stop()