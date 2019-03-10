from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class repl_wiki_join(MRJob):

    transaction_table = {}

    def mapper_join_init(self):
        with open("wikifilter.csv") as f:
            for line in f:
                try:
                    fields = line.split(",")
                    if len(fields)==4:
                        hash = str(fields[0])
                        btc_amount = float(fields[1])
                        self.transaction_table[hash] = btc_amount
                except:
                    pass

    def mapper_repl_join(self, _, line):
        try:
            fields = line.split(',')
            if len(fields)==3:
                txid=str(fields[0])
                tx_hash=str(fields[1])
                vout=int(fields[2])

                if txid in self.transaction_table:
                    btc_amount = self.transaction_table[txid]

                    #yield(txid, (tx_hash, vout, btc_amount))
                    yield('null', '{},{},{},{}'.format(txid, tx_hash, vout, btc_amount))
        except:
            pass

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                            mapper=self.mapper_repl_join)]

if __name__ == '__main__':
    repl_wiki_join.run()
