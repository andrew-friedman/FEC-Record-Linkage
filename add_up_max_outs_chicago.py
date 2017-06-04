# python3 reduce.py --jobconf mapreduce.job.reduces=1 data.csv
# python3 scrapper.py --jobconf mapreduce.job.reduces=1 test.txt

from mrjob.job import MRJob

def get_chicago_zip_codes():
    zip_codes = set(['60601', '60602', '60603', '60604', '60605',
        '60606', '60607', '60608', '60609', '60610', '60611',
        '60612', '60613', '60614', '60615', '60616', '60617',
        '60618', '60619', '60620', '60621', '60622', '60623',
        '60624', '60625', '60626', '60628', '60629', '60630',
        '60631', '60632', '60633', '60634', '60636', '60637',
        '60638', '60639', '60640', '60641', '60642', '60643',
        '60644', '60645', '60646', '60647', '60649', '60651',
        '60652', '60653', '60654', '60655', '60656', '60657',
        '60659', '60660', '60661', '60664', '60666', '60668',
        '60669', '60670', '60673', '60674', '60675', '60677',
        '60678', '60680', '60681', '60682', '60684', '60685',
        '60686', '60687', '60688', '60689', '60690', '60691',
        '60693', '60694', '60695', '60696', '60697', '60699',
        '60701'])
    return zip_codes


class MRJob_reduce(MRJob):
  

    def mapper_init(self):
        self.threshold = 2700.00
        self.chicago_zip_codes = get_chicago_zip_codes()


    def mapper(self, _, line):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        indiv_list = line.split("|")
        indiv_Zip = indiv_list[10][0:5]
        trans_amount = float(indiv_list[14])

        if indiv_Zip in self.chicago_zip_codes and trans_amount >= self.threshold:
            yield indiv_Zip, trans_amount

    def combiner(self, indiv_Zip, trans_amount):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        sum_transactions = sum(trans_amount)
        
        yield indiv_Zip, sum_transactions

    def reducer(self, indiv_Zip, trans_amount):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        sum_transactions = sum(trans_amount)
        yield indiv_Zip, sum_transactions


if __name__ == '__main__':
    MRJob_reduce.run()