import urllib2
import urllib
import json
import time

from kafka import KafkaProducer

# From: https://developer.yahoo.com/yql/#python
# From: https://developer.yahoo.com/yql/console/?q=select%20*%20from%20weather.forecast%20where%20woeid%3D2502265&env=store://datatables.org/alltableswithkeys#h=select+*+from+yahoo.finance.quotes+where+symbol+in+(%22YHOO%22%2C%22AAPL%22%2C%22GOOG%22%2C%22MSFT%22)

class YahooFinanceAPI:
    def __init__(self):
        self.baseurl = "https://query.yahooapis.com/v1/public/yql?"
    
    def get(self):
        yql_query = "select * from yahoo.finance.quotes where symbol = 'MSFT'"
        yql_url = self.baseurl + urllib.urlencode({'q':yql_query}) + "&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback="
        result = urllib2.urlopen(yql_url).read()
        data = json.loads(result)
        #return data['query']['results']['quote']
        return data

# https://github.com/dpkp/kafka-python
class Producer:
    def __init__(self, topic, server):
        self.producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic

    def send(self, message):
        self.producer.send(self.topic, message)


        
        
if __name__ == "__main__":
    api = YahooFinanceAPI()
    producer = Producer("quotes", 'localhost:9092')
    
    while True:
        print "Getting and Sending quotes info to localhost:9092"
        quote = api.get()
        producer.send(quote)
        time.sleep(30)


