
import codecs
import csv
import sqlite3

conn = sqlite3.connect("database/NCES.db")

c = conn.cursor()

sql = "SELECT * FROM final WHERE State = 'WA'"

c.execute(sql)

field_names = [i[0] for i in c.description]
results = c.fetchall()

with codecs.open("output/NCESSchoolsWA.txt", 'w', 'utf8') as f:
    writer = csv.writer(f, delimiter="\t")

    writer.writerow(field_names)
    for row in results:
        writer.writerow(row)
