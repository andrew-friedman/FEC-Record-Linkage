from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import tee
from jellyfish import jaro_winkler
from util import BinaryTree, get_indices_of_keys, get_CBSAs
import re

INDEX = get_indices_of_keys()
ZIP_TO_CBSA = get_CBSAs()
WORD_RE = re.compile(r"[\w]+\.*")

class MRMatch(MRJob):

	def line_to_dict(line):
		fields = line.split('|')
		record = {}
		for key in INDEX:
			record[key] = fields[INDEX[key]]
		return record

	def mapper(self, _, line):
		'''
		We use CBSA code if available and otherwise zip code as key 
		to effectively block on geographical region while, to the best
		of our ability, accounting for movement within a region.
		'''
		record = self.line_to_dict(line)
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
		r1_name = r1["NAME"]
		r2_name = r2["NAME"]
			for word1 in WORD_RE.findall(r1_name):
				# filter out Mr., Mrs., etc.
				if not (len(word1) > 1 and word[-1] != "."):
					word1 = word1.strip(".")



	def reducer_init(self):
		matched = BinaryTree() # records which entries 
							#have already been matched

	def reducer(self, code, lines):
		for line in lines:
			if not matched.contains(line): 
				L = tee(lines)
				lines = L[0]
				for l in L[1]:
					m = m_score(line, l)















