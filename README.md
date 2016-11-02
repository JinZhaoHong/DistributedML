# DistributedML
Distributed Machine Learning for Stock Price Prediction

## Work Flow
1. Stock data is collected from Yahoo! Finance (Currently manually downloading the data, but in the future should implement an automated system. And if having enough fund, a Bloomberg terminal API would be a better choice). All stock data are in csv format. For example, a typical file looks like: Date, Opening, High, Low, Close, Adjusted Close

2. The current plan is to store this stock data into the Hadoop Distributed File System(HDFS) so that it is scalable to multiple machines. How to set up HDFS can be found here: http://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SingleCluster.html

3. A data preprossing 




![Alt text](https://github.com/JinZhaoHong/DistributedML/blob/master/img/Screen%20Shot%202016-10-27%20at%2011.56.56%20AM.png?raw=true "Optional Title")
