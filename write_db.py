import sqlite3

def write_results(resultsFile, db, tableName, colName1, colName2):
	query = "CREATE TABLE " + tableName + " (" + colName1 + 
			', ' + colName2 + ")"
	db.execute(query)
	query = "INSERT INTO " + tableName + "VALUES (?,?)"
	f = open(resultsFile)
	for line in f:
		line = line.strip()
		l = line.split('\t')
		t = (l[0], l[1].strip('\"'))
		db.execute(query, t)
	f.close()
	db.commit()