# python3 reduce.py --jobconf mapreduce.job.reduces=1 data.csv
# python3 scrapper.py --jobconf mapreduce.job.reduces=1 test.txt

from mrjob.job import MRJob

class MRJob_reduce(MRJob):
  

    def mapper_init(self):
        self.threshold = 200
        self.count = 0

    def mapper(self, _, line):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        indiv_list = line.split("|")
        c_id = indiv_list[0]
        trans_amount = float(indiv_list[14])
        if trans_amount >= self.threshold:
            self.count += 1
            print(self.count)
            yield c_id, trans_amount

    def combiner(self, c_id, trans_amount):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        print(trans_amount)

        sum_transactions = sum(trans_amount)
        yield c_id, sum_transactions

    def reducer(self, c_id, trans_amount):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        sum_transactions = sum(trans_amount)
        yield c_id, sum_transactions


if __name__ == '__main__':
    MRJob_reduce.run()