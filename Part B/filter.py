import re
from mrjob.job import MRJob

class wikileaks_filter(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            # if len(fields)==4:
            wikileaks_wallet = fields[3]
            donor_hash = fields[0]
            btc_amount = float(fields[1])
            transaction_id = int(fields[2])

            if wikileaks_wallet == "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}":
                print(f"{donor_hash}, {btc_amount}, {transaction_id}, {wikileaks_wallet}")
        except:
            pass
if __name__ == '__main__':
    wikileaks_filter.run()
