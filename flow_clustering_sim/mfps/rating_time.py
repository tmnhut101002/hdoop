import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
from math import e


class rating_time(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def rating_time_mapper(self, _, line):
        users, value = line.strip().split('\t')
        time = value.strip().split('|')[1]
        yield users, time

    def rating_time_reducer(self, users, values):
        values = list(values)
        values = [value.strip().split(';') for value in values]

        alpha = 10**-6
        sum = 0

        for line in values:
            user1_time, user2_time = line[:2]
            user1_time = float(user1_time)
            user2_time = float(user2_time)
            sum += pow(e, -alpha * abs(user1_time - user2_time))

        yield users, f'{sum};rt'

    def steps(self):
        return [
            MRStep(mapper=self.rating_time_mapper,
                   reducer=self.rating_time_reducer),
        ]


if __name__ == '__main__':
    rating_time().run()
