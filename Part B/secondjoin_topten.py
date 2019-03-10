from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class repl_wiki_secondjoin(MRJob):

    transaction_table_second = {}

    def mapper_join_init(self):
        with open("firstjoin.csv") as f:
            for line in f:
                try:
                    fields = line.split(",")
                    if len(fields)==4:
                        tx_hash = str(fields[1])
                        vout = float(fields[2])
                        self.transaction_table_second[tx_hash] = vout
                except:
                    pass

    def mapper_repl_join(self, _, line):
        fields = line.split(',')
        try:
            if len(fields)==4:
                hash = str(fields[0])
                n = float(fields[2])
                publicKey = str(fields[3])
                value = float(fields[1])

            if hash in self.transaction_table_second:
                if self.transaction_table_second[hash]==n:
                    yield(publicKey, value)
        except:
            pass

    def reducer_sum(self, key, value):
        yield(key, sum(value))

    def mapper_pair(self, key, sum):
        yield('a', (key, sum))

    def reducer_sort(self, key, sum):

        sorted_values = sorted(sum, reverse = True, key = lambda x:x[1])

        for rank in range(10):
            yield(rank+1,'{}-{}'.format(sorted_values[rank][0], sorted_values[rank][1]))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                            mapper=self.mapper_repl_join, reducer=self.reducer_sum),
                            MRStep(mapper = self.mapper_pair, reducer=self.reducer_sort)]

if __name__ == '__main__':
    repl_wiki_secondjoin.run()
