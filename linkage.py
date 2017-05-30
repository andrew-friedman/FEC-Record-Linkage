from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations_with_replacement
from jellyfish import jaro_winkler
import re
import csv

#################################
##      Tuning Parameters      ##
#################################
THRESHOLD = 0.9
#################################
#################################
WORD_RE = re.compile(r"[\w]+")

INDEX = {'AMNDT_IND': 1,
 'CITY': 8,
 'CMTE_ID': 0,
 'EMPLOYER': 11,
 'ENTITY_TP': 6,
 'FILE_NUM': 17,
 'IMAGE_NUM': 4,
 'MEMO_CD': 18,
 'MEMO_TEXT': 19,
 'NAME': 7,
 'OCCUPATION': 12,
 'OTHER_ID': 15,
 'RPT_TP': 2,
 'STATE': 9,
 'SUB_ID': 20,
 'TRANSACTION_AMT': 14,
 'TRANSACTION_DT': 13,
 'TRANSACTION_PGI': 3,
 'TRANSACTION_TP': 5,
 'TRAN_ID': 16,
 'ZIP_CODE': 10}

def get_CBSAs(filename = "ZIP_CBSA_032017.csv"):
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



class MRMatch(MRJob):

    def steps(self):
        return[
            MRStep(mapper_init = self.cbsa_init,
                mapper = self.block_on_area,
                reducer = self.match),
            MRStep(reducer_init = self.make_id_generator,
                reducer = self.generate_ids)
        ]

    def cbsa_init(self):
        self.ZIP_TO_CBSA = get_CBSAs()

    def line_to_dict(self, line):
        fields = line.split('|')
        record = {}
        for key in INDEX:
            record[key] = fields[INDEX[key]].strip()
        return record

    def block_on_area(self, _, line):
        '''
        We use CBSA code if available and otherwise zip code as key 
        to effectively block on geographical area while, to the best
        of our ability, accounting for movement within a region.
        '''
        self.increment_counter('Counts', 'Donations', 1)
        record = self.line_to_dict(line)
        zipcode = record["ZIP_CODE"][:5]
        try:
            cbsa = ZIP_TO_CBSA[zipcode]
        except: # zipcodes get retired and whatnot
            self.increment_counter('Errors', 'Zip code to CBSA', 1)
            cbsa = "cbsa99999"
        if cbsa != "cbsa99999":
            yield cbsa, line
        else:
            yield zipcode, line

    def m_score(self, record1, record2):
        '''
        Calculates the "match score" between the individuals
        in two records

        Instead of the JW similarity score of the entire name string,
        we break the string down to words and match the words by 
        which are most likely to be the same (first name, first name)
        (last name, last name) pair. We do this because there are 
        inconsistencies in the order names are written in. Then, we 
        reconstruct the JW score of the full name and return.
        '''
        r1 = self.line_to_dict(record1)
        r2 = self.line_to_dict(record2)
        ljw = 0
        l = 0
        for word1 in WORD_RE.findall(r1["NAME"]):
            w1 = word1.lower()
            jw_pairs = []
            for word2 in WORD_RE.findall(r2["NAME"]):
                w2 = word2.lower()
                jw = jaro_winkler(w1, w2)
                jw_pairs.append((jw, w2))
            m = max(jw_pairs)
            jw = m[0]
            li = len(w1) + len(m[1])
            ljw += li*jw 
            l += li 
        scjw = ljw/l 
        return scjw 

    def match(self, code, lines):
        self.increment_counter('Counts', 'Zip codes', 1)
        matched = BinaryTree() # records which entries 
                            #have already been matched
        # try matching unmatched lines against every subsequent line
        pairs = combinations_with_replacement(lines, 2) # all pairs
        for p in pairs:
            if not matched.contains(p[0]) and not matched.contains(p[1]):
                if p[0] == p[1]:
                    m = THRESHOLD
                    self.increment_counter('Counts', 'Self Matched', 1)
                else:
                    try:
                        m = self.m_score(p[0], p[1])
                    except Exception as e:
                        self.increment_counter('Errors', e, 1)
                        m = 0
                if m >= THRESHOLD:
                    if p[0] != p[1]:
                        matched.add(p[1])
                        self.increment_counter('Counts', 'Matches', 1)
                    yield hash(p[0]), hash(p[1])

    def make_id_generator(self):
        self.next_id = 0

    def generate_ids(self, key, hashvals):
        self.increment_counter('Counts', 'Individuals', 1)
        for h in hashvals:
            yield h, self.next_id
        self.next_id += 1

if __name__ == '__main__':
    MRMatch.run()


