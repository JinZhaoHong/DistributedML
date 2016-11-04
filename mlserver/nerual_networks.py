import numpy as np
import sklearn.metrics as metrics
import matplotlib.pyplot as plt
import pdb
from pyspark import SparkContext, SparkConf



conf = SparkConf().setAppName("nerual networks for stock price prediction")
sc = SparkContext(conf=conf)


def load_dataset():
	stock_file = sc.textFile("hdfs://localhost:9000/user/zhjin/mlinput/data/msft_combined.csv")
	return stock_file

if __name__ == "__main__":
	stock_file = load_dataset()
	print stock_file.collect()