
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w]+")

class MRChicagoPercentages(MRJob):
        
    def mapper(self, _, line):
        l = line.split('|')
        zipcode = l[0]
        occupation = l[1].split(',')[0]
        amount = l[2]
        for word in WORD_RE.findall(occupation):
            key = zipcode + ' ' + word
            amt = float(amount)
            yield key, (amt, 1)    
    
    def combiner(self, key, amount):
        x = 0
        y = 0
        for amt in amount:
            x += amt[0]
            y += amt[1]
        yield key, (x, y)

    def reducer(self, key, amount):
        x = 0
        y = 0
        for amt in amount:
            x += amt[0]
            y += amt[1]
        yield key, x/y

if __name__ == '__main__':
    MRChicagoPercentages.run()

