import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
from itertools import combinations
from hdfs import InsecureClient

class create_combinations(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def create_combinations_mapper(self, _, line):
        key, value = line.strip().split('\t')
        user, item = key.strip().split(';')
        rating, time = value.strip().split(';')
        yield item, f'{user};{rating};{time}'

    def create_combinations_reducer(self, item, group):
        group = list(group)
        group = [i.strip().split(';') for i in group]

        comb = combinations(group, 2)
        for u, v in comb:
            if (int(u[0]) < int(v[0])):
                yield f'{u[0]};{v[0]}', f'{u[1]};{v[1]}|{u[2]};{v[2]}'
            else:
                yield f'{v[0]};{u[0]}', f'{v[1]};{u[1]}|{v[2]};{u[2]}'

    def steps(self):
        return [
            MRStep(mapper=self.create_combinations_mapper,
                   reducer=self.create_combinations_reducer),
        ]

if __name__ == '__main__':
    create_combinations.run()
