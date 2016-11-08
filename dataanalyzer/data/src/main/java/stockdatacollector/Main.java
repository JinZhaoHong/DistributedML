package stockdatacollector;

import java.io.FileNotFoundException;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Scanner;


import dataFrame.DataPoint;
import dataFrame.Stock;

/**
 * 
 * @author zhjin
 *
 */
public class Main {
	
	public static Stock stock;
	
	public static String hdfs;
	public static String root;
	public static String stockfile;
	public static String stockName;
	public static String ticker;
	public static String[] indexList;
	public static String destinationfile;

	
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("[Usage] java -classpath name.jar stockdatacollector.Main config");
			return;
		}
		readConfiguration(args);
		loadStockData();
		loadIndexData();
		stock.setParameters();
		writeToFile("/" + destinationfile);
	}
	
	public static void readConfiguration(String[] args) {
		File file = new File(args[0]);
		try {
			Scanner c = new Scanner(file);
			hdfs = c.nextLine().split(",")[1];
			root = c.nextLine().split(",")[1];
			stockfile = c.nextLine().split(",")[1];
			stockName = c.nextLine().split(",")[1];
			ticker = c.nextLine().split(",")[1];
			indexList = c.nextLine().split(",")[1].split(" ");
			destinationfile = c.nextLine().split(",")[1];
			
			System.out.println(hdfs);
			System.out.println(root);
			System.out.println(stockfile);
			System.out.println(stockName);
			System.out.println(ticker);
			System.out.println(indexList[0]);
			System.out.println(destinationfile);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	/**
	 * This file I/O supports only HDFS, not the regular local file system
	 */
	public static void loadStockData() {
		stock = new Stock(stockName, ticker);
		
		Path path = new Path(root + "/" + stockfile); //for HDFS use 
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", hdfs);
			// see this post on stack overflow http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
		    conf.set("fs.hdfs.impl", 
		            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		        );
		    conf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName()
		        );
			FileSystem fs = FileSystem.get(conf);
			BufferedReader c = new BufferedReader(new InputStreamReader(fs.open(path)));
			// skip the title line and the first line(because it doesn't have a label
			c.readLine();
			c.readLine();
			String line = c.readLine();
			while (line != null) {
				stock.addStockEntry(line);
				line = c.readLine();
			}
			c.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * This file I/O supports only HDFS, not the regular local file system
	 */
	public static void loadIndexData() {
		for (String index : indexList) {
			
			try {
				
				Path path = new Path(root + "/" + index);
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", hdfs);
				// see this post on stack overflow http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
			    conf.set("fs.hdfs.impl", 
			            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
			        );
			    conf.set("fs.file.impl",
			            org.apache.hadoop.fs.LocalFileSystem.class.getName()
			        );
				FileSystem fs = FileSystem.get(conf);
				BufferedReader c = new BufferedReader(new InputStreamReader(fs.open(path)));
				
				String line = c.readLine();
				
				while (line != null) {
					stock.addIndexEntry(line, index);
					line = c.readLine();
				}
				
				c.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void writeToFile(String file) {
		try {
			Path path = new Path(root + file);
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", hdfs);
			// see this post on stack overflow http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
		    conf.set("fs.hdfs.impl", 
		            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		        );
		    conf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName()
		        );
			FileSystem fs = FileSystem.get(conf);
			BufferedWriter writer =new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
			
			String header = "Date,Open,high,low,close,volume,movingAverageFiveDay,"
					+ "movingAverageTenDay,exponentialMovingAverage,rateOfChangeFiveDay,"
					+ "rateOfChangeTenDay,spyClose,spyMovingAverageFiveDay,"
					+ "spyMovingAverageTenDay,spyRateOfChangeFiveDay,spyRateOfChangeTenDay,"
					+ "djiClose,djiMovingAverageFiveDay,djiMovingAverageTenDay,"
					+ "djiRateOfChangeFiveDay,djiRateOfChangeTenDay,ixicClose,"
					+ "ixicMovingAverageFiveDay,ixicMovingAverageTenDay,ixicRateOfChangeFiveDay,"
					+ "ixicRateOfChangeTenDay,tnxClose,tnxMovingAverageFiveDay,tnxMovingAverageTenDay,"
					+ "tnxRateOfChangeFiveDay,tnxRateOfChangeTenDay,vixClose,"
					+ "vixMovingAverageFiveDay,vixMovingAverageTenDay,vixRateOfChangeFiveDay,"
					+ "vixRateOfChangeTenDay,label \n";
			writer.write(header);
			for (String date : stock.dateList) {
				DataPoint data = stock.dataMap.get(date);
				String line = data.date + "," + data.open + "," + data.high + "," + data.low + "," + data.close + "," 
				+ data.volume + "," + data.movingAverageFiveDay + "," + data.movingAverageTenDay + "," 
				+ data.exponentialMovingAverage + "," + data.rateOfChangeFiveDay + "," + data.rateOfChangeTenDay + "," 
				+ data.spyClose + "," + data.spyMovingAverageFiveDay + "," + data.spyMovingAverageTenDay + "," 
				+ data.spyRateOfChangeFiveDay + "," + data.spyRateOfChangeTenDay + "," + data.djiClose + "," 
				+ data.djiMovingAverageFiveDay + "," + data.djiMovingAverageTenDay + "," + data.djiRateOfChangeFiveDay + "," 
				+ data.djiRateOfChangeTenDay + "," + data.ixicClose + "," + data.ixicMovingAverageFiveDay + "," 
				+ data.ixicMovingAverageTenDay + "," + data.ixicRateOfChangeFiveDay + "," + data.ixicRateOfChangeTenDay + "," 
				+ data.tnxClose + "," + data.tnxMovingAverageFiveDay + "," + data.tnxMovingAverageTenDay + "," 
				+ data.tnxRateOfChangeFiveDay + "," + data.tnxRateOfChangeTenDay + "," + data.vixClose + "," 
				+ data.vixMovingAverageFiveDay + "," + data.vixMovingAverageTenDay + "," + data.vixRateOfChangeFiveDay + "," 
				+ data.rateOfChangeTenDay + "," + data.label + " \n";
				
				writer.write(line);
			}
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}