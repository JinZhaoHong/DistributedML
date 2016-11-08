from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SparkSession

import os.path


conf = SparkConf().setAppName("Ticker Quote Streaming Processor")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)


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

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(conf)

            tickerQuoteDataFrame = spark.read.json(rdd)

            # Creates a temporary view using the DataFrame.
            tickerQuoteDataFrame.createOrReplaceTempView("ticker")

            #tickerQuoteDataFrame.printSchema()

            # Please print out and read schema before doing query
            QuoteDataFrame = spark.sql("select query.results.quote.* from ticker")
            QuoteDataFrame.show()

        except:
        	pass

    lines.foreachRDD(process)

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate




if __name__ == "__main__":
	streamProcess("quotes", "localhost:9092")




