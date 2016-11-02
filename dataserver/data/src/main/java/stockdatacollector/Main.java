package stockdatacollector;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import dataFrame.DataPoint;
import dataFrame.Stock;

public class Main {
	
	public static Stock stock;
	
	public static String root = "/Users/zhjin/Desktop/projects/DistributedML/data";
	public static String stockName = "Microsoft";
	public static String ticker = "MSFT";
	public static String[] indexList = {"spy", "dji", "ixic", "tnx", "vix"};

	
	public static void main(String[] args) {
		loadStockData();
		loadIndexData();
		stock.setParameters();
		writeToFile("/msft_combined.csv");
	}
	
	
	
	public static void loadStockData() {
		stock = new Stock(stockName, ticker);
		
		File file = new File(root + "/msft.csv");
		Scanner c;
		try {
			c = new Scanner(file);
			// skip the title line and the first line(because it doesn't have a label
			c.nextLine();
			c.nextLine();
			while (c.hasNextLine()) {
				String line = c.nextLine();
				stock.addStockEntry(line);
				
			}
			c.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void loadIndexData() {
		for (String index : indexList) {
			File file = new File(root + "/" + index + ".csv");
			Scanner c;
			try {
				c = new Scanner(file);
				while (c.hasNextLine()) {
					String line = c.nextLine();
					stock.addIndexEntry(line, index);
				}
				
				c.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void writeToFile(String file) {
		try {
			PrintWriter writer = new PrintWriter(root + file, "UTF-8");
			String header = "Open,high,low,close,volume,movingAverageFiveDay,"
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
			writer.println(header);
			for (String date : stock.dateList) {
				DataPoint data = stock.dataMap.get(date);
				String line = data.open + "," + data.high + "," + data.low + "," + data.close + "," 
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
				
				writer.println(line);
			}
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}