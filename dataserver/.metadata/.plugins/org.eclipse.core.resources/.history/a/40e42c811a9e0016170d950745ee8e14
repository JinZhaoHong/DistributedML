import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.mysql.jdbc.MysqlDataTruncation;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;

/**
 * 
 * @author Zhaohong Jin
 *
 */
public class DBManager {
	private Connection connection;
	public Properties props;

	/**
	 * load the database.properties into the props (key -> value map)
	 * 
	 * @throws IOException
	 */
	public void readProperties() throws IOException {
		props = new Properties();
		try {
			String dir = System.getProperty("user.dir");
			FileInputStream in = new FileInputStream(dir
					+ "/src/database.properties");
			props.load(in);
			in.close();
		} catch (FileNotFoundException e) {
			String dir = "E:/workstation/JEngine";
			FileInputStream in = new FileInputStream(dir
					+ "/src/database.properties");
			props.load(in);
			in.close();
		}
	}

	/**
	 * 
	 * @throws SQLException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void openConnection() throws SQLException, IOException,
			ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		String drivers = props.getProperty("jdbc.drivers");
		if (drivers != null)
			System.setProperty("jdbc.drivers", drivers);

		String url = props.getProperty("jdbc.url");
		System.out.println(url);
		String username = props.getProperty("jdbc.username");
		String password = props.getProperty("jdbc.password");

		connection = DriverManager.getConnection(url, username, password); // use
																			// given
																			// info
																			// to
																			// open
																			// the
																			// database
		System.out.println("Opening database connection successful.");
		System.out.println("User: " + username);
	}

	/**
	 * creating the table urls to store the URL info
	 * 
	 * @throws SQLException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void createURLDescriptionTable() throws SQLException, IOException,
			ClassNotFoundException {
		openConnection();

		Statement stat = connection.createStatement(); // used for executing a
														// static SQL statement
														// and returning the
														// results it produces.

		// Delete the table first if any
		try {
			stat.executeUpdate("DROP TABLE URLS");
		} catch (Exception e) {
		}

		// Create the table
		// Executes the given SQL statement, which may be an INSERT, UPDATE, or
		// DELETE statement or an SQL statement that returns nothing, such as an
		// SQL DDL statement.
		stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(1024), description VARCHAR(2048))");
	}

	/**
	 * create the table for words
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void createURLWordTable() throws SQLException, IOException {
		Statement stat = connection.createStatement();
		try {
			stat.executeUpdate("DROP TABLE WORDS");
		} catch (Exception e) {
		}
		stat.executeUpdate("CREATE TABLE WORDS (word VARCHAR(512), count INT, urlid INT, url VARCHAR(1024))");
	}

	/**
	 * 
	 * @param table
	 * @return whether a specific table exists or not
	 * @throws SQLException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public boolean tableExist(String table) throws SQLException, IOException,
			ClassNotFoundException {
		openConnection();
		Statement stat = connection.createStatement();
		String query = "SELECT count(*) FROM information_schema.tables WHERE table_schema = "
				+ "'mysql'" + "AND table_name =" + "'" + table + "'";
		ResultSet result = stat.executeQuery(query);
		result.next();
		int count = result.getInt("count(*)");
		if (count > 0) {
			return true;
		}
		return false;
	}

	/**
	 * insert into the URLS table
	 * 
	 * @param url
	 * @throws SQLException
	 * @throws IOException
	 */
	public void insertURLInDB(int id, String url, String description)
			throws SQLException, IOException {
		Statement stat = connection.createStatement();
		try {
			String query = "INSERT INTO URLS VALUES ('" + id + "','" + url
					+ "','" + description + "' )";
			// System.out.println("Executing "+query);
			stat.executeUpdate(query);
		} catch (MySQLSyntaxErrorException e) {
			System.out.println("Unable to insert into database url : " + url);
		} catch (MysqlDataTruncation e) {
			System.out.println("overflow! url : " + url);
		}
	}

	/**
	 * insert into the WORDS table
	 * 
	 * @param word
	 * @param urlid
	 * @throws SQLException
	 */
	public void insertWordInDB(String word, int count, int urlid, String url)
			throws SQLException {
		Statement stat = connection.createStatement();
		try {
			String query = "INSERT INTO WORDS VALUES ('" + word + "','" + count
					+ "','" + urlid + "','" + url + "' )";
			// System.out.println(query);
			stat.executeUpdate(query);
		} catch (MySQLSyntaxErrorException e) {
			System.out.println("unable to insert into the database : " + word);
		} catch (MysqlDataTruncation e) {
			System.out.println("overflow! word : " + word + " url : " + url);
		}
	}

	/**
	 * 
	 * @param urlFound
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public boolean urlInDB(String urlFound) throws SQLException, IOException {
		Statement stat = connection.createStatement();
		try {
			ResultSet result = stat
					.executeQuery("SELECT * FROM urls WHERE url LIKE '"
							+ urlFound + "'"); // Executes the given SQL
												// statement, which
												// returns a single ResultSet
												// object.
			if (result.next()) {
				System.out.println("URL " + urlFound + " already in DB");
				return true;
			}
			// System.out.println("URL "+urlFound+" not yet in DB");
			return false;
		} catch (MySQLSyntaxErrorException e) {
			System.out.println("Unable to run query for urlInDB");
			return false;
		}
	}

	/**
	 * 
	 * @param description
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public boolean descriptionInDB(String description) throws SQLException,
			IOException {
		Statement stat = connection.createStatement();
		try {
			ResultSet result = stat
					.executeQuery("SELECT * FROM urls WHERE description LIKE '"
							+ description + "'"); // Executes the given SQL
													// statement, which
													// returns a single
													// ResultSet object.

			if (result.next()) {
				System.out.println("description already in DB");
				return true;
			}
			// System.out.println("URL "+urlFound+" not yet in DB");
			return false;
		} catch (MySQLSyntaxErrorException e) {
			System.out.println("Unable to run query for descriptionInDB");
			return false;
		}
	}

	/**
	 * 
	 * @param word
	 * @param urlid
	 * @return
	 * @throws SQLException
	 */
	public boolean wordInDB(String word, int urlid) throws SQLException {
		Statement stat = connection.createStatement();
		try {
			ResultSet result = stat
					.executeQuery("SELECT * FROM Words WHERE word LIKE '"
							+ word + "' and urlid LIKE '" + urlid + "'");

			if (result.next()) {
				System.out.println("word " + word + "urlid" + urlid
						+ " already in DB");
				return true;
			}
			// System.out.println("URL "+urlFound+" not yet in DB");
			return false;
		} catch (MySQLSyntaxErrorException e) {
			System.out.println("Unable to run query for wordInDB");
			return false;
		}
	}

	/**
	 * 
	 * @param word
	 * @return resultset of the urlids that contain a specific word;
	 * @throws SQLException
	 */
	public ArrayList<Integer> geturlid(String word) throws SQLException {
		ArrayList<Integer> list = new ArrayList<Integer>();
		Statement stat = connection.createStatement();
		String query = "SELECT urlid FROM Words WHERE word LIKE '" + word
				+ "' ORDER BY count DESC";
		ResultSet result = stat.executeQuery(query);
		while (result.next()) {
			int urlid = result.getInt("urlid");
			list.add(urlid);
		}
		return list;
	}

	/**
	 * 
	 * @param index
	 * @return
	 * @throws SQLException
	 */
	public String geturl(int index) throws SQLException {
		String url = new String();
		Statement stat = connection.createStatement();
		String query = "SELECT url FROM URLS WHERE urlid = '" + index + "'";
		ResultSet result = stat.executeQuery(query);
		while (result.next()) {
			url = result.getString("url");
		}
		return url;
	}

	/**
	 * 
	 * @param index
	 * @return
	 * @throws SQLException
	 */
	public String getDescription(int index) throws SQLException {
		String description = new String();
		Statement stat = connection.createStatement();
		String query = "SELECT description FROM URLS WHERE urlid = '" + index
				+ "'";
		ResultSet result = stat.executeQuery(query);
		while (result.next()) {
			description = result.getString("description");
		}
		return description;
	}

}
