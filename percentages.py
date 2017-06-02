from mrjob.job import MRJob

class MRJPercentages(MRJob):
    '''
    Finds what percentage of the money spent on behalf of each 
    campaign is spent by the candidate's principal campaign committee
    '''

    def get_designations(self, filename = 'cm.txt'):
        f = open(filename)
        d = {}
        for line in f:
            l = line.strip()
            l = l.split('|')
            CMTE_ID = l[0]
            CMTE_DSGN = l[8]
            primary = CMTE_DSGN == 'P' # true if principal comm
            d[CMTE_ID] = primary
        return d

    def mapper_init(self):
        self.DESIGNATIONS = self.get_designations()
        
    def mapper(self, _, line):
        l = line.split('|')
        CMTE_ID = l[0]
        TRANSACTION_AMT = abs(float(l[14]))
        self.increment_counter('Transaction amt', TRANSACTION_AMT, 1)
        assert TRANSACTION_AMT != 0
        CAND_ID = l[16]
        if self.DESIGNATIONS[CMTE_ID]: # is principal comm
            yield CAND_ID, (TRANSACTION_AMT, 0)
        else:
            yield CAND_ID, (0, TRANSACTION_AMT)
    
    def combiner(self, CAND_ID, amt):
        p = 0
        n = 0
        for a in amt:
            p += a[0]
            n += a[1]
        yield CAND_ID, (p, n)

    def reducer(self, CAND_ID, amt):
        p = 0
        n = 0
        for a in amt:
            p += a[0]
            n += a[1]
        assert p!= 0 or n != 0
        yield CAND_ID, p/(p + n) 

if __name__ == '__main__':
    MRJPercentages.run()
