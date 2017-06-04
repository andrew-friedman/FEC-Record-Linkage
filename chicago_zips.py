
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w]+")

class MRChicagoZips(MRJob):

    def mapper_init(self):
        '''
        Sets a variable with list of Chicago zip codes
        '''
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
        self.ZIPS =  zip_codes
        
    def mapper(self, _, line):
        l = line.split('|')
        try:
            zipcode = l[10][:5]
        except:
            zipcode = "Invalid Zip"
        if zipcode in self.ZIPS:
            occupation = l[12]
            amount = l[14]
            for word in WORD_RE.findall(occupation):
                try:
                    key = zipcode + ' ' + word
                    amt = float(amount)
                    yield key, amt
                except:
                    pass       
    
    def combiner(self, key, amount):
        yield key, sum(amount)

    def reducer(self, key, amount):
        yield key, sum(amount)

if __name__ == '__main__':
    MRChicagoZips.run()

