import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import pandas as pd


class rating_details(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(rating_details, self).configure_args()
        self.add_file_arg("--avg-rating-path", help="Path to average rating file")

    def rating_details_mapper(self, _, line):
        key, value = line.strip().split("\t")
        keys = key.strip().split(";")

        for index, _ in enumerate(keys):
            yield f"{keys[index]};{keys[1 - index]}", value

    def create_avg_rating_df(self, filename):
        df = pd.read_csv(
            filename, sep="\t", names=["user", "avg_rating"], dtype="float64"
        )
        df.set_index("user", inplace=True)
        return df

    def rating_details_reducer_init(self):
        avg_rating_path = self.options.avg_rating_path
        self.avg_rating_df = self.create_avg_rating_df(avg_rating_path)

    def rating_details_reducer(self, users, values):
        avg_rating_df = self.avg_rating_df
        user1, user2 = users.strip().split(";")
        threshold1 = float(avg_rating_df.loc[float(user1), "avg_rating"])
        threshold2 = float(avg_rating_df.loc[float(user2), "avg_rating"])

        count = 0
        for value in values:
            ratings, _ = value.strip().split("|")
            rating1, rating2 = ratings.strip().split(";")
            rating1 = float(rating1)
            rating2 = float(rating2)

            if (rating1 < threshold1 and rating2 < threshold2) or (
                rating1 > threshold1 and rating2 > threshold2
            ):
                count = count + 1

        yield users, f"{count};rd"

    def steps(self):
        return [
            MRStep(
                mapper=self.rating_details_mapper,
                reducer_init=self.rating_details_reducer_init,
                reducer=self.rating_details_reducer,
            )
        ]


if __name__ == "__main__":
    rating_details().run()
