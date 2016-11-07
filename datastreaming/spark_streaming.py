from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


conf = SparkConf().setAppName("Ticker Data Streaming Processor")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 30)


def streamProcess(topic, brokers):
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
	lines = directKafkaStream.map(lambda x: x[1])
	words = lines.flatMap(lambda line: line.split(" "))
	pairs = words.map(lambda word: (word, 1))
	wordCounts = pairs.reduceByKey(lambda x, y: x + y)


	wordCounts.pprint()


	ssc.start()             # Start the computation
	ssc.awaitTermination()  # Wait for the computation to terminate




if __name__ == "__main__":
	streamProcess("quotes", "localhost:9092")