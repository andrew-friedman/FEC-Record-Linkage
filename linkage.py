from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations_with_replacement, takewhile, starmap
from jellyfish import jaro_winkler
from util import BinaryTree, line_to_dict, calc_dist, ZIP_TO_CBSA
import re

#################################
##      Tuning Parameters      ##
#################################
THRESHOLD = 0.88
W = 0.002 # zip penalty weight
#################################
#################################
WORD_RE = re.compile(r"[\w]+\.*")

class MRMatch(MRJob):

	def steps(self):
        return[
            MRStep(mapper = self.block_on_area,
                reducer = self.match),
            MRStep(reducer_init = self.make_id_generator,
            	reducer = self.generate_ids)
        ]

	def block_on_area(self, _, line):
		'''
		We use CBSA code if available and otherwise zip code as key 
		to effectively block on geographical area while, to the best
		of our ability, accounting for movement within a region.
		'''
		self.increment_counter('Counts', 'Donations', 1)
		record = line_to_dict(line)
		zipcode = record["ZIP_CODE"]
		cbsa = ZIP_TO_CBSA[zipcode]
		if cbsa != "cbsa99999":
			yield cbsa, line
		else:
			yield zipcode, line

	def m_score(record1, record2):
		'''
		Calculates the "match score" between the individuals
		in two records
		'''
		r1 = line_to_dict(record1)
		r2 = line_to_dict(record2)
		r1_words = takewhile(lambda w:  not (len(w) > 1 and w[-1] != "."), 
			WORD_RE.findall(r1["NAME"]))
		r2_words = takewhile(lambda w:  not (len(w) > 1 and w[-1] != "."), 
			WORD_RE.findall(r2["NAME"]))
		ljw = 0
		l = 0
		for word1 in r1_words:
			w1 = word1.strip(".").lower()
			sm = starmap(lambda w2: (jaro_winkler(w1, w2.strip(".").lower()), 
				(w1, w2.strip(".").lower())), r2_words)
			m = max(sm)
			jw = m[0]
			li = len(m[1][0]) + len(m[1][1])
			ljw += li*jw 
			l += li 
		scjw = ljw/l 
		zip_penalty = W*calc_dist(r1["ZIP_CODE"][:5], r2["ZIP_CODE"][:5])
		return scjw - zip_penalty

	def match(self, code, lines):
		matched = BinaryTree() # records which entries 
							#have already been matched
		# try matching unmatched lines
		pairs = combinations_with_replacement(lines) # all pairs
		unmatched = takewhile(lambda p: not matched.contains(p[1]), pairs)
		for pair in unmatched:
			if p[0] == p[1]:
				m = THRESHOLD
				self.increment_counter('Counts', 'Matches', -1)
			else:
				try:
					m = m_score(pair[0], pair[1])
				except Exception as e:
					self.increment_counter('Errors', e, 1)
					m = 0
			if m >= THRESHOLD:
				self.increment_counter('Counts', 'Matches', 1)
				matched.add(pair[1])
				yield hash(p[0]), hash(p[1])

	def make_id_generator():
		self.next_id = 0

	def generate_ids(self, key, hashvals):
		self.increment_counter('Counts', 'Individuals', 1)
		for h in hashvals:
			yield h, self.next_id
		self.next_id += 1

if __name__ == '__main__':
    MRMatch.run()


    