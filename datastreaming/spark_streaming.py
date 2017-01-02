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
# http://datastax.github.io/python-driver/getting_started.html
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



def getAccumulators(sparkContext):
    if ('counter' not in globals()):
        globals()['counter'] = sparkContext.accumulator(1.0)
    if ('askMovingAverage' not in globals()):
        globals()['askMovingAverage'] = sparkContext.accumulator(0.0)
    if ('bidMovingAverage' not in globals()):
        globals()['bidMovingAverage'] = sparkContext.accumulator(0.0)
    if ('askExponentialMovingAverage' not in globals()):
        globals()['askExponentialMovingAverage'] = sparkContext.accumulator(0.0)
    if ('bidExponentialMovingAverage' not in globals()):
        globals()['bidExponentialMovingAverage'] = sparkContext.accumulator(0.0)
    return (globals()['counter'], globals()['askMovingAverage'], globals()['bidMovingAverage'], globals()['askExponentialMovingAverage'], globals()['bidExponentialMovingAverage'])



def process(rdd):

    spark = None
    quoteDataFrame = None

    counter, askMovingAverage, bidMovingAverage, askExponentialMovingAverage, bidExponentialMovingAverage = getAccumulators(sc)

    try: # Must use try and catch, otherwise will get exceptions
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(conf)

        # Schema is automatically inferred from the rdd. Convert rdd into dataframe for sql process
        tickerQuoteDataFrame = spark.read.json(rdd)

        # Creates a temporary view using the DataFrame.
        tickerQuoteDataFrame.createOrReplaceTempView("ticker_table")

        # Please print out and read schema before doing query
        quoteDataFrame = spark.sql("select query.created, query.results.quote.* from ticker_table")
        #quoteDataFrame.printSchema()

    except:
        pass

    # Move this statement outside for debugging purposes. Otherwise no exceptions will ever be thrown
    if quoteDataFrame != None:

        # http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support
        insert_statment = session.prepare('INSERT INTO ticker JSON ?')
        # convert the dataframe back to an RDD[String] that contains the JSON records
        # http://stackoverflow.com/questions/31473215/how-to-convert-dataframe-to-json
        session.execute(insert_statment, quoteDataFrame.toJSON().collect()) 


        # Some data analytics: calculate the expoential moving average and moving average
        # get the askPrice and bidPrice directly from each piece of data in Dataframe
        askPrice = float(spark.sql("select query.results.quote.Ask from ticker_table").rdd.collect()[0]['Ask'])
        bidPrice = float(spark.sql("select query.results.quote.Bid from ticker_table").rdd.collect()[0]['Bid'])


        alpha = float(2) / (counter.value + 1)

        if askExponentialMovingAverage.value == 0:
            askExponentialMovingAverage.add(askPrice)
        else:
            askExponentialMovingAverage.add(alpha * (askPrice - askExponentialMovingAverage.value))

        if bidExponentialMovingAverage.value == 0:
            bidExponentialMovingAverage.add(bidPrice)
        else:
            bidExponentialMovingAverage.add(alpha * (bidPrice - bidExponentialMovingAverage.value))

        globals()['askMovingAverage'] = sc.accumulator(float(askMovingAverage.value * (counter.value - 1) + askPrice) / counter.value)

        globals()['bidMovingAverage'] = sc.accumulator(float(bidMovingAverage.value * (counter.value - 1) + bidPrice) / counter.value)


        print ("counter: " + str(counter.value), "askMovingAverage: " + str(askMovingAverage.value), "bidMovingAverage: " + str(bidMovingAverage.value), "askExponentialMovingAverage: " + str(askExponentialMovingAverage.value), "bidExponentialMovingAverage: " + str(bidExponentialMovingAverage.value))

        counter.add(1)


def streamProcess(topic, brokers):

    # read streaming data directly from kafka
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # get the json object of the stock data
    lines = directKafkaStream.map(lambda x: x[1])

    lines.foreachRDD(process)

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate




if __name__ == "__main__":
    creatTickerTable()
    streamProcess("quotes", "localhost:9092")




