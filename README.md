# DistributedML
Distributed Machine Learning for Stock Price Prediction

## Work Flow
1. Stock data is collected from Yahoo! Finance (Currently manually downloading the data, but in the future should implement an automated system. And if having enough fund, a Bloomberg terminal API would be a better choice). All stock data are in csv format. For example, a typical file looks like: Date, Opening, High, Low, Close, Adjusted Close

2. The current plan is to store this stock data into the Hadoop Distributed File System(HDFS) so that it is scalable to multiple machines. How to set up HDFS can be found here: http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SingleCluster.html

3. A data preprossing program in Java will select features and clean data from the initial raw Yahoo! Finance csv files. Some of the features are inspired based on these two research papers:
  
  (1) Machine Learning Techniques for Stock Prediction, Vatsal H. Shah
  http://www.vatsals.com/Essays/MachineLearningTechniquesforStockPrediction.pdf
  
  (2) Machine Learning in Stock Price Trend Forecasting, Yuqing Dai, Yuning Zhang
  http://cs229.stanford.edu/proj2013/DaiZhang-MachineLearningInStockPriceTrendForecasting.pdf
  
  Here is a list of features used in this program:
  For a particular stock: Open, High, Low, Close, Five Day Moving Average, Ten Day Moving Average, Exponential Moving Average, Rate of Change Five Day, Rate of Change Ten Day. Also, the Associated S&P500 ETF, Dow Jones, Nasdaq, 10-year bond, and S&P500 VIX's Close, Five Day Moving Average, Ten Day Moving Average, Rate of Change Five Day, Rate of Change Ten Day.
  
  The result will be stored in the HDFS as well.

4. Server. The Server will be written in Node JS(Try out new technologies). The Machine Learning Algorithm will be written in Python(Using SKLearn or Tensorflow). The Server will run Machine Learning on the backend and read the input datafile from HDFS(cold data) and store the trained model in a database(hot data). This scheme ensures that we can frequently update the model or retrive the model for the prediction.

5. Front End(Clint Side). Will be hosted on the Github. 

6. Work flow chart.
![Alt text](https://github.com/JinZhaoHong/DistributedML/blob/master/img/Screen%20Shot%202016-10-27%20at%2011.56.56%20AM.png?raw=true "Optional Title")

## Machine Learning

