from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")


class MRJob_occupations(MRJob):


    def mapper(self, _, line):
        """
        yields:
          key: a string, one word of occupation
          value: a float, donation amount
        """
        indiv_list = line.split("|")
        occupation = indiv_list[12]
        trans_amount = float(indiv_list[14])
        for one_word in WORD_RE.findall(occupation):
            occupation_words = set()
            if one_word not in occupation_words:
                occupation_words.add(one_word) 
                yield one_word, trans_amount

    def combiner(self, one_word, amount):
        """
        yields:
          key: a string, one word of occupation
          value: a float, donation amount
        """
        sum_amount = sum(amount)

        yield one_word, sum_amount

    def reducer(self, one_word, amount):
        """
        yields:
          key: a string, one word of occupation
          value: a float, donation amount
        """
        sum_amount = sum(amount)

        yield one_word, sum_amount


if __name__ == '__main__':
    MRJob_occupations.run()