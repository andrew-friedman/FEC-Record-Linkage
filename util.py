import re
import csv
import math

def get_indices_of_keys(filename = "data/indiv_header_file.csv"):
	'''
	Makes a field to key dictionary from the header file for the
	individual contribution dataset.
	'''
	f = open(filename)
	s = f.read()
	f.close()
	s = s.strip()
	keys = s.split(',')
	index = {}
	for i in range(len(keys)):
		index[keys[i]] = i
	return index

INDEX = get_indices_of_keys()
def line_to_dict(line):
	fields = line.split('|')
	record = {}
	for key in INDEX:
		record[key] = fields[INDEX[key]].strip()
	return record

def get_CBSAs(filename = "data/ZIP_CBSA_032017.csv"):
	'''
	Creates dictionary that maps zip codes to core based statistical 
	areas, which are better to block by because they're more likely
	to capture an individual changing address.

	More info on CBSAs:
		https://www.census.gov/geo/reference/gtc/gtc_cbsa.html

	Source for data:
		https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data
	(converted file format from downloaded .xslx)

	Concatonate "cbsa" to the beginning of CBSA code so as to avoid
	confusion with identical zipcodes
	'''
	zip_to_cbsa = {}
	with open(filename) as f:
		reader = csv.DictReader(f)
		for row in reader:
			zip_to_cbsa[row["ZIP"]] = "cbsa" + row["CBSA"]
	return zip_to_cbsa

ZIP_TO_CBSA = get_CBSAs()

def get_coordinates(filename = "data/zipcode/zipcode.csv"):
	zip_to_coord = {}
	with open(filename) as f:
		reader = csv.DictReader(f)
		for row in reader:
			zipcode = row["zip"]
			while(len(zipcode) < 5):
				zipcode = "0" + zipcode
			zip_to_coord[zipcode] = (row["latitude"], row["longitude"])
	return zip_to_coord

ZIP_TO_COORD = get_coordinates()
def calc_dist(zip1, zip2):
	x1 = float(ZIP_TO_COORD[zip1][0])
	x2 = float(ZIP_TO_COORD[zip2][0])
	y1 = float(ZIP_TO_COORD[zip1][1])
	y2 = float(ZIP_TO_COORD[zip2][1])
	return math.sqrt(pow(x1 + x2, 2) + pow(y1 + y2, 2))


class BinaryTree:
	'''
	An object that can keep track of and quickly check whether 
	and an object has been previously added.
	'''

	def __init__(self):
		self.val = None
		self.g = None # g is always greater than val
		self.l = None # l is always less than val

	def add(self, val):
		'''
		Adds val to tree if not already added
		'''
		val = hash(val)
		if self.val is None:
			self.val = val
		else:
			if val > self.val:
				if self.g is None:
					self.g = BinaryTree()
				self.g.add(val)
			elif val < self.val:
				if self.l is None:
					self.l = BinaryTree()
				self.l.add(val)

	def contains(self, val):
		'''
		Return True if val is in tree, False otherwise
		'''
		val = hash(val)
		if self.val is not None:
			if val == self.val:
				return True
			elif val > self.val:
				if self.g is not None:
					return self.g.contains(val)
				else:
					return False
			elif val < self.val:
				if self.l is not None:
					return self.l.contains(val)
				else:
					return False
		else:
			return False

