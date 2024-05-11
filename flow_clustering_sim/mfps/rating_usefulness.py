import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class rating_usefulness(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(rating_usefulness, self).configure_args()
        self.add_file_arg("--rating-commodity-path")

    def count_items_mapper(self, _, line):
        key, value = line.strip().split("\t")
        user, _ = key.strip().split(";")
        rating, _ = value.strip().split(";")
        yield user, rating

    def count_items_reducer(self, user, ratings):
        ratings = list(ratings)
        items_counted = len(ratings)
        yield user, f"{items_counted}"

    def read_file(self, filename):
        arr = []
        with open(filename, "r") as file:
            for line in file:
                arr.append(line.strip().split("\t"))
        return arr

    def rating_usefulness_mapper_init(self):
        rating_commodity_path = self.options.rating_commodity_path
        self.rating_commodity = self.read_file(rating_commodity_path)

    def rating_usefulness_mapper(self, user, items_counted):
        for key, value in self.rating_commodity:
            u1, u2 = key.strip().split(";")
            commodity = value.strip().split(";")[0]

            if user == u1 or user == u2:
                yield f"{u1};{u2}", f"{commodity};{user};{items_counted}"

    def rating_usefulness_reducer(self, key, values):
        values = list(values)
        values = [value.strip().split(";") for value in values]

        commodity, u1, items_counted_1 = values[0]
        _, u2, items_counted_2 = values[1]

        yield f"{u1};{u2}", f"{int(items_counted_2) - int(commodity)};ru"
        yield f"{u2};{u1}", f"{int(items_counted_1) - int(commodity)};ru"

    def steps(self):
        return [
            MRStep(mapper=self.count_items_mapper, reducer=self.count_items_reducer),
            MRStep(
                mapper_init=self.rating_usefulness_mapper_init,
                mapper=self.rating_usefulness_mapper,
                reducer=self.rating_usefulness_reducer,
            ),
        ]


if __name__ == "__main__":
    rating_usefulness().run()
