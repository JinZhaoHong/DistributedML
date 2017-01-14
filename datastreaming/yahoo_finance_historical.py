import sys
import csv
from cassandra.cluster import Cluster
import json

# default value
symbol = 'MSFT'

# This will attempt to connection to a Cassandra instance on the local machine (127.0.0.1)
# http://datastax.github.io/python-driver/getting_started.html
cluster = Cluster()
session = cluster.connect()

KEYSPACE = "tickerkeyspace"
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """ % KEYSPACE)

session.set_keyspace(KEYSPACE)


def process(inputFile):
	inputArray = []
	inputArray
	for line in inputFile:
		inputArray.append(line)
	i = 0
	inputArray.reverse() # start from the most distant data

	formatDate(inputArray)

	# These functions must be called in order
	simpleMovingAverage(inputArray, 5)
	simpleMovingAverage(inputArray, 10)
	simpleMovingAverage(inputArray, 20)
	simpleMovingAverage(inputArray, 30)
	simpleMovingAverage(inputArray, 60)
	expoentialMovingAverage(inputArray, window = 12)
	expoentialMovingAverage(inputArray, window = 26)
	MACD(inputArray)
	signalLine(inputArray)
	histogram(inputArray)

	createTable()

	for line in inputArray:
		# create a new table for the data to be displayed.
		insert = """INSERT INTO %s (Symbol, Date, Date_Formatted, Open, Close, High, Low, Volume, SMA5, SMA10, SMA20, SMA30, SMA60, EMA12, EMA26, MACD, MACD_EMA9, MACDhistogram)""" %symbol
		session.execute(insert + 
			"""
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
			""", (symbol, line['Date'], line['Date_Formatted'], float(line['Open']), float(line['Close']), float(line['High']), float(line['Low']), float(line['Volume']), float(line['SMA5']), float(line['SMA10']), float(line['SMA20']), float(line['SMA30']), float(line['SMA60']), float(line['EMA12']), float(line['EMA26']), float(line['MACD']), float(line['MACD_EMA9']), float(line['MACDhistogram']))
		)

	#saveAsCsv()


def simpleMovingAverage(inputArray, window = 5):
	if len(inputArray) < 1:
		print "Not enough data!"
		return
	i = 0
	while i < len(inputArray):
		line = inputArray[i]
		average = 0
		if i + 1 < window: # edge case, we don't have enough data
			total = 0.0
			for j in range(i + 1):
				subLine = inputArray[i - j]
				total += float(subLine['Close'])
			average = total / (i + 1) 
		else:
			total = 0.0
			for j in range(window):
				subLine = inputArray[i - j]
				total += float(subLine['Close'])
			average = total / window
		line['SMA' + str(window)] = average
		i += 1



def expoentialMovingAverage(inputArray, name = 'EMA', param = 'Close', window = 12):
	# http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_averages
	if len(inputArray) < 1:
		print "Not enough data!"
		return
	i = 2 
	alpha = 2.0 / (window + 1)
	# Base Case
	inputArray[0][name + str(window)] = inputArray[0][param]
	while i - 1 < len(inputArray):
		emaPrev = float(inputArray[i - 2][name + str(window)])
		curr = float(inputArray[i - 1][param])
		inputArray[i - 1][name + str(window)] = (curr - emaPrev) * alpha + emaPrev
		i += 1


def MACD(inputArray):
	# http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_average_convergence_divergence_macd
	# 12 day EMA - 26 day EMA
	if len(inputArray) < 1:
		print "Not enough data!"
		return
	for line in inputArray:
		line['MACD'] = float(line['EMA12']) - float(line['EMA26'])


def signalLine(inputArray):
	expoentialMovingAverage(inputArray, name = 'MACD_EMA', param = 'MACD', window = 9)


def histogram(inputArray):
	if len(inputArray) < 1:
		print "Not enough data!"
		return
	for line in inputArray:
		line['MACDhistogram'] = float(line['MACD']) - float(line['MACD_EMA9'])


def formatDate(inputArray):
	lib = {'01':"Jun", '02':"Feb", '03':"Mar", '04':"Apr", '05':"May", '06':"Jun", '07':"Jul", '08':"Aug", '09':"Sep", '10':"Oct", '11':"Nov", '12':"Dec"}
	for line in inputArray:
		splitted = line['Date'].split("-")
	 	line['Date_Formatted']= splitted[2] + "-" + lib[splitted[1]] + "-" + splitted[0][2:] # example: "01-Jun-97"


def saveAsCsv(file):
	pass



def createTable():
	# http://stackoverflow.com/questions/35708118/where-and-order-by-clauses-in-cassandra-cql
    message = """CREATE TABLE IF NOT EXISTS %s (
                Symbol varchar,
                Date timestamp,
                Date_Formatted varchar, 
                Open float,
                Close float,
                High float,
                Low float,
                Volume float,
                SMA5 float,
                SMA10 float,
                SMA20 float,
                SMA30 float,
                SMA60 float,
                EMA12 float,
                EMA26 float,
                MACD float,
                MACD_EMA9 float,
                MACDhistogram float,
                PRIMARY KEY (Symbol, Date)
                ) 
                WITH CLUSTERING ORDER BY (Date DESC);""" % symbol
    session.execute(message)



if __name__ == "__main__":
	if len(sys.argv) > 2:
		print "This program is used to insert the historical stock data into the database"
		print "Usage: python yahoo_finance_historical.py [Stock Symbol]"
	elif len(sys.argv) == 2:
		symbol = str(sys.argv[1])
	else:
		inputFile = None
		try:
			inputFile = csv.DictReader(open("./data/" + symbol + ".csv"))
		except:
			print "No such file exists. Please download from Yahoo! Finance"
			exit()
		process(inputFile)




