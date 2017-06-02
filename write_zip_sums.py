import sqlite3
import re

f = open('zipresults.txt')
conn = sqlite3.connect("donations.db")
db = conn.cursor()
c.execute('''CREATE TABLE chicago_zips
             (zip text, job_word text, donated real)''')
REGEX = re.compile(r"[\S]+")
for line in f:
	fields = REGEX.findall(line)
	fields = [f.strip('\"') for f in fields]
	fields[2] = float(fields[2])
	fields = tuple(fields)
	c.execute('INSERT INTO chicago_zips VALUES (?,?,?)', fields)
conn.commit()
conn.close()

