from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")


class MRJob_occupations(MRJob):


    def mapper(self, _, line):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        indiv_list = line.split("|")
        occupation = indiv_list[12]
        for one_word in WORD_RE.findall(occupation):
            occupation_words = set()
            if one_word not in occupation_words:
                occupation_words.add(one_word) 
                yield one_word, 1

    def combiner(self, one_word, count):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        sum_count = sum(count)

        yield one_word, sum_count

    def reducer(self, one_word, count):
        """
        yields:
          key: a string, candidate or PAC ID
          value: a float, donation amount
        """
        sum_count = sum(count)

        yield one_word, sum_count


if __name__ == '__main__':
    MRJob_occupations.run()