import sqlite3

conn = sqlite3.connect(r'Database/empresas.db')
cursor = conn.cursor()

cursor.execute("DELETE FROM empresas")
conn.commit()

#cursor.execute("SELECT * FROM empresas")

#for row in cursor.fetchall():
#    print(row)

conn.close()