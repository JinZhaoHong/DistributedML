# DistributedML
Distributed Machine Learning for Stock Price Prediction

## Technology Stack
Apache Kafka

Apache Spark

Apache Cassandra

Apache Hadoop

Apache Mesos

Apache Zookeeper

Node.js

## Work Flow
1. Stock data is collected from Yahoo! Finance Using the YQL Query Language. https://developer.yahoo.com/yql/#python= (If having enough fund, a Bloomberg terminal API would be a better choice). All tiker quotes come in as JSON streams, and the streams are handled by Apache Kafka.

2. Spark Streaming handles Kafka inputs, convert the JSON objects into Spark Dataframes, and store these Dataframes into Apache Cassandra / HDFS

3. The current plan is to store this stock data both in the Hadoop Distributed File System(HDFS)and Apache Cassandra so that it is scalable to multiple machines. How to set up HDFS can be found here: http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SingleCluster.html

4. A data preprossing program in Java will select features and clean data from the raw files in HDFS. Some of the features are inspired based on these two research papers:
  
  (1) Machine Learning Techniques for Stock Prediction, Vatsal H. Shah
  http://www.vatsals.com/Essays/MachineLearningTechniquesforStockPrediction.pdf
  
  (2) Machine Learning in Stock Price Trend Forecasting, Yuqing Dai, Yuning Zhang
  http://cs229.stanford.edu/proj2013/DaiZhang-MachineLearningInStockPriceTrendForecasting.pdf
  
  Here is a list of features used in this program:
  For a particular stock: Open, High, Low, Close, Five Day Moving Average, Ten Day Moving Average, Exponential Moving Average, Rate of Change Five Day, Rate of Change Ten Day. Also, the Associated S&P500 ETF, Dow Jones, Nasdaq, 10-year bond, and S&P500 VIX's Close, Five Day Moving Average, Ten Day Moving Average, Rate of Change Five Day, Rate of Change Ten Day.
  
  The result will be stored in the HDFS as well.

5. Server. The Server will be written in Node JS(Try out new technologies). The Machine Learning Algorithm will be written in Python(Using SKLearn or Tensorflow). The Server will run Machine Learning on the backend and read the input datafile from HDFS(cold data) and store the trained model in a database(hot data). This scheme ensures that we can frequently update the model or retrive the model for the prediction.

6. Front End(Clint Side). Will be hosted on the Github. 

7. Work flow chart.
![Alt text](https://github.com/JinZhaoHong/DistributedML/blob/master/img/workflow.png?raw=true "Optional Title")

## Machine Learning
1. The machine learning algorithm requires(recommends)python anaconda distribution, which can be downloaded and installed here: https://www.continuum.io/downloads

