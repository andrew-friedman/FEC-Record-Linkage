
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w]+")

class MRChicagoAverages(MRJob):
        
    def mapper(self, _, line):
        """
        yields:
          key: a string, zip code and word
          value: a tuple, of a float, amount and a interger, count
        """
        l = line.split('|')
        zipcode = l[0]
        occupation = l[1].split(',')[0]
        amount = l[2]
        for word in WORD_RE.findall(occupation):
            key = zipcode + ' ' + word
            amt = float(amount)
            yield key, (amt, 1)    
    
    def combiner(self, key, amount):
        """
        yields:
          key: a string, zip code and word
          value: a tuple, of a float, amount and a interger, count
        """
        x = 0
        y = 0
        for amt in amount:
            x += amt[0]
            y += amt[1]
        yield key, (x, y)

    def reducer(self, key, amount):
        """
        yields:
          key: a string, zip code and word
          value: a float, the average
        """
        x = 0
        y = 0
        for amt in amount:
            x += amt[0]
            y += amt[1]
        yield key, x/y

if __name__ == '__main__':
    MRChicagoAverages.run()

