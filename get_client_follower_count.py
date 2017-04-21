import sqlite3
import sys

input_filename = sys.argv[1]

conn = sqlite3.connect(input_filename)

cursor = conn.cursor()
for r in cursor.execute("SELECT * FROM twitter_graph"):
    print str(r[0]) + ' ' + str(len(r[1].strip().split()))

conn.close()
