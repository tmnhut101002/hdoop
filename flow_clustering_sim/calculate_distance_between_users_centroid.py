from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import numpy as np
from scipy.spatial.distance import cdist

class DistanceBetweenUsersCentroidSpark:
    def __init__(self, input_path, centroid_path, return_centroid_id=False):
        self.input_path = input_path
        self.centroid_path = centroid_path
        self.return_centroid_id = return_centroid_id

    def get_init_centroid(self, filename):
        centroid_ids = []
        centroids = []

        with open(filename, 'r') as file:
            for line in file:
                centroid_id, centroid_value = line.strip().split('\t')
                centroid_ids.append(centroid_id)

                centroid_value = centroid_value.strip().split('|')
                centroid_value = [el.strip().split(';') for el in centroid_value]
                centroid_coordinate = np.array(centroid_value, dtype='f')[:, 1]
                centroids.append(centroid_coordinate)

        centroids = np.vstack(centroids)
        return centroid_ids, centroids

    def distance_between_users_centroid(self, row):
        user, value = row.split('\t')  # Assuming input format is user \t user_items_rating&centroid_id
        value, centroid_id = value.split('&')
        value = value.strip()
        coordinate = value.split('|')
        coordinate = [el.strip().split(';') for el in coordinate]
        coordinate = np.array(coordinate, dtype='f')[:, 1].reshape(1, -1)
        centroids = self.centroids
        centroid_ids = self.centroid_ids

        distances = cdist(centroids, coordinate)  # Corrected the order of arguments in cdist
        min_euclidean_distance_index = np.argmin(distances)
        selected_centroid_id = centroid_ids[min_euclidean_distance_index]
        min_euclidean_distance = distances[min_euclidean_distance_index][0]

        if self.return_centroid_id:
            return f'{user}\t{min_euclidean_distance}'
        else:
            return f'{user}\t{min_euclidean_distance}'

    def run(self):
        conf = SparkConf().setAppName("DistanceBetweenUsersCentroid")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc)

        self.centroid_ids, self.centroids = self.get_init_centroid(self.centroid_path)

        input_data = spark.read.text(self.input_path).rdd
        result = input_data.map(lambda x: x[0]).map(self.distance_between_users_centroid)

        # result.saveAsTextFile('./output_spark')

        result_data = result.collect()
        
        with open("./output/distance.txt","w") as out_file:
            for result in result_data:
                out_file.write(f"{result}\n")

        spark.stop()
        sc.stop()

if __name__ == '__main__':
    spark_app = DistanceBetweenUsersCentroidSpark('./output/user_item_matrix.txt', './output/centroids.txt', return_centroid_id=True)
    spark_app.run()
