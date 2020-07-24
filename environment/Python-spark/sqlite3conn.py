import sqlite3
import csv

# con = sqlite3.connect(":memory:")
con = sqlite3.connect("person.db")
cur = con.cursor()

cur.execute('DROP TABLE IF EXISTS FactCurrencyRate')
cur.execute('CREATE TABLE IF NOT EXISTS FactCurrencyRate( \
	  CurrencyKey integer, \
	  DateKey integer, \
	  AverageRate integer, \
	  EndOfDayRate integer, \
	  Datex TIMESTAMP)')
con.commit()

	# Load the CSV file into CSV reader
csvfile = open('//home/hpinto/Desktop/mySpark-data/FactCurrencyRate.csv', 'rb')
xreader = csv.reader(csvfile, delimiter='|', quotechar='|')
print(xreader)