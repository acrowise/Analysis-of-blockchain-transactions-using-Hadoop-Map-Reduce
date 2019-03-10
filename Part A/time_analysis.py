
from mrjob.job import MRJob
import time
import re

class time_analysis(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 5 :
                time_epoch = int(fields[2])
                transactions_in = int(fields[3])
                transactions_out = int(fields[4])
                total_transactions = transactions_in + transactions_out
                month_year = time.strftime("%m-%y",time.gmtime(time_epoch)) #returns day of the month
                yield (month_year, total_transactions)
        except:
            pass
            #do nothing

    def combiner(self, key, value):
        yield (key, sum(value))

    def reducer(self, key, value):
        yield (key, sum(value))

if __name__ == '__main__':
    time_analysis.run()
