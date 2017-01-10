import sys

symbol = "MSFT"


if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "This program is used to insert the historical stock data into the database"
		print "Usage: python yahoo_finance_historical.py [Stock Symbol]"
	else:
		symbol = str(sys.argv[1])

