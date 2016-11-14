from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SparkSession

import os.path

from cassandra.cluster import Cluster

"""
Get data streams from Apache Kafka, using Spark Streaming to process and generate dataframes,
and store dataframes to Apache Cassandra
"""


conf = SparkConf().setAppName("Ticker Quote Streaming Processor")#.set("spark.cassandra.connection.host", "127.0.0.1")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)


# This will attempt to connection to a Cassandra instance on the local machine (127.0.0.1)
cluster = Cluster()
session = cluster.connect()

KEYSPACE = "tickerkeyspace"
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """ % KEYSPACE)

session.set_keyspace(KEYSPACE)


def creatTickerTable():
    # shold have 83 columns. In the original JSON from Yahoo!, There is a column symbol and a column Symbol, which is repetative
    message = """CREATE TABLE IF NOT EXISTS ticker (
                created varchar PRIMARY KEY,
                symbol varchar,
                Ask varchar,
                AverageDailyVolume varchar,
                Bid varchar,
                AskRealtime varchar,
                BidRealtime varchar,
                BookValue varchar,
                Change_PercentChange varchar,
                Change varchar,
                Commission varchar,
                Currency varchar,
                ChangeRealtime varchar,
                AfterHoursChangeRealtime varchar,
                DividendShare varchar,
                LastTradeDate varchar,
                TradeDate varchar,
                EarningsShare varchar,
                ErrorIndicationreturnedforsymbolchangedinvalid varchar,
                EPSEstimateCurrentYear varchar,
                EPSEstimateNextYear varchar,
                EPSEstimateNextQuarter varchar,
                DaysLow varchar,
                DaysHigh varchar,
                YearLow varchar,
                YearHigh varchar,
                HoldingsGainPercent varchar,
                AnnualizedGain varchar,
                HoldingsGain varchar,
                HoldingsGainPercentRealtime varchar,
                HoldingsGainRealtime varchar,
                MoreInfo varchar,
                OrderBookRealtime varchar,
                MarketCapitalization varchar,
                MarketCapRealtime varchar,
                EBITDA varchar,
                ChangeFromYearLow varchar,
                PercentChangeFromYearLow varchar,
                LastTradeRealtimeWithTime varchar,
                ChangePercentRealtime varchar,
                ChangeFromYearHigh varchar,
                PercebtChangeFromYearHigh varchar,
                LastTradeWithTime varchar,
                LastTradePriceOnly varchar,
                HighLimit varchar,
                LowLimit varchar,
                DaysRange varchar,
                DaysRangeRealtime varchar,
                FiftydayMovingAverage varchar,
                TwoHundreddayMovingAverage varchar,
                ChangeFromTwoHundreddayMovingAverage varchar,
                PercentChangeFromTwoHundreddayMovingAverage varchar,
                ChangeFromFiftydayMovingAverage varchar,
                PercentChangeFromFiftydayMovingAverage varchar,
                Name varchar,
                Notes varchar,
                Open varchar,
                PreviousClose varchar,
                PricePaid varchar,
                ChangeinPercent varchar,
                PriceSales varchar,
                PriceBook varchar,
                ExDividendDate varchar,
                PERatio varchar,
                DividendPayDate varchar,
                PERatioRealtime varchar,
                PEGRatio varchar,
                PriceEPSEstimateCurrentYear varchar,
                PriceEPSEstimateNextYear varchar, 
                SharesOwned varchar,
                ShortRatio varchar,
                LastTradeTime varchar,
                TickerTrend varchar,
                OneyrTargetPrice varchar,
                Volume varchar,
                HoldingsValue varchar,
                HoldingsValueRealtime varchar,
                YearRange varchar,
                DaysValueChange varchar,
                DaysValueChangeRealtime varchar,
                StockExchange varchar,
                DividendYield varchar,
                PercentChange varchar
                )"""
    session.execute(message)



def getSparkSessionInstance(sparkConf):
    """
    Referenve: spark streaming examples sql_newwork_wordcount.py
    """
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def streamProcess(topic, brokers):
    # read streaming data directly from kafka
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # get the json object of the stock data
    lines = directKafkaStream.map(lambda x: x[1])


    def process(rdd):

        spark = None
        QuoteDataFrame = None

        try: # Must use try and catch, otherwise will get exceptions
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(conf)

            tickerQuoteDataFrame = spark.read.json(rdd)

            # Creates a temporary view using the DataFrame.
            tickerQuoteDataFrame.createOrReplaceTempView("ticker_table")

            # Please print out and read schema before doing query
            QuoteDataFrame = spark.sql("select query.created, query.results.quote.* from ticker_table")
            QuoteDataFrame.printSchema()

        except:
            pass

        # Move this statement outside for debuggin purposes. Otherwise no exceptions will ever be thrown
        if QuoteDataFrame != None:

            insert_statment = session.prepare('INSERT INTO ticker JSON ?')
            session.execute(insert_statment, QuoteDataFrame.toJSON().collect()) 


    lines.foreachRDD(process)

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate




if __name__ == "__main__":
    creatTickerTable()
    streamProcess("quotes", "localhost:9092")




