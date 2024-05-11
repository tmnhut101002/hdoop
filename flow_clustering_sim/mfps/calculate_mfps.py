import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class CalculateMFPS(MRJob):
    OUTPUT_PROTOCOL = TextProtocol
    INTERNAL_PROTOCOL = TextProtocol

    def mfps_mapper(self, _, line):
        key, value = line.strip().split('\t')
        u1, u2 = key.strip().split(';')

        if value.strip().split(';')[-1] == 'rc' or value.strip().split(';')[-1] == 'rt':
            yield f'{u2};{u1}', value
        yield key, value

    def mfps_reducer(self, key, values):
        values = list(values)
        values = [value.strip().split(';') for value in values]
        mfps = 1

        for line in values:
            sim = float(line[0])
            flag = line[-1]
            if (sim == 0):
                if flag == 'rc':
                    mfps = 0
                    break
                continue
            mfps += 1 / sim
        if mfps:
            mfps = 1 / mfps

        yield key, f'{mfps}'

    def steps(self):
        return [
            MRStep(mapper=self.mfps_mapper, reducer=self.mfps_reducer),
        ]


if __name__ == '__main__':
    CalculateMFPS().run()
