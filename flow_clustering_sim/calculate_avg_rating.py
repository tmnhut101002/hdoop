# from pyspark.sql import SparkSession

# def calculate_avg_rating(line):
#     key, value = line.strip().split('\t')
#     user, _ = key.strip().split(';')
#     rating, _ = value.strip().split(';')
#     return user, int(rating)

# if __name__ == '__main__':
#     # Tạo một phiên Spark
#     spark = SparkSession.builder.appName("AvgRating").getOrCreate()

#     # Đọc dữ liệu từ tệp đầu vào
#     input_file = "../input_file.txt"
#     lines = spark.sparkContext.textFile(input_file)

#     # Chuyển đổi dữ liệu thành định dạng key-value
#     ratings = lines.map(calculate_avg_rating)

#     # Tính toán tổng điểm và số lượng đánh giá cho mỗi người dùng
#     user_totals = ratings.aggregateByKey((0, 0), lambda acc, value: (acc[0] + value, acc[1] + 1),
#                                          lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))

#     # Tính toán trung bình và xuất kết quả
#     avg_ratings = user_totals.map(lambda x: (x[0], x[1][0] / float(x[1][1])))
#     result_data = avg_ratings.collect()
    
#     with open('./output/avg_ratings.txt', "w") as output_file:
#         for user, avg_rating in result_data:
#             output_file.write(f"{user}\t{avg_rating}\n")
    
#     # Dừng phiên Spark
#     spark.stop()

from pyspark.sql import SparkSession

class AvgRatingCalculator:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.spark = SparkSession.builder.appName("AvgRating").getOrCreate()

    @staticmethod
    def calculate_avg_rating(line):
        key, value = line.strip().split('\t')
        user, _ = key.strip().split(';')
        rating, _ = value.strip().split(';')
        return user, int(rating)

    def run(self):
        # Read data from the input file
        lines = self.spark.sparkContext.textFile(self.input_file)

        # Convert data to key-value format
        ratings = lines.map(self.calculate_avg_rating)

        # Calculate total score and the number of ratings for each user
        user_totals = ratings.aggregateByKey((0, 0), self.aggregate_function, self.combine_function)

        # Calculate average and output the result
        avg_ratings = user_totals.map(lambda x: (x[0], x[1][0] / float(x[1][1])))
        
        result_data = avg_ratings.toDF(["User", "Average Rating"])

        result_data.write.mode('overwrite').options(header='False', delimiter='\t').csv(self.output_file)

        #avg_ratings.saveAsTextFile('hdfs://localhost:9000/'+"Clustering"+"/RF_model.txt")

        # with open(self.output_file, "w") as output_file:
        #     for user, avg_rating in result_data:
        #         output_file.write(f"{user}\t{avg_rating}\n")

        # Stop the Spark session
        self.spark.stop()

    @staticmethod
    def aggregate_function(acc, value):
        return acc[0] + value, acc[1] + 1

    @staticmethod
    def combine_function(acc1, acc2):
        return acc1[0] + acc2[0], acc1[1] + acc2[1]

if __name__ == '__main__':
    input_file_path = "../input_file.txt"
    output_file_path = "hdfs://localhost:9000/Clustering/AverageRating.csv"

    avg_rating_calculator = AvgRatingCalculator(input_file_path, output_file_path)
    avg_rating_calculator.run()

