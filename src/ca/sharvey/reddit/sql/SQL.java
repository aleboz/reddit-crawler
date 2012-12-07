package ca.sharvey.reddit.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SQL {

	private static final String SQL_CLASS = "org.sqlite.JDBC";
	private static final String SQL_CONNECTOR = "jdbc:sqlite";
	private static final String SQL_STORE = "/tmp/dataset/reddit";
	private static final String DB_A = SQL_STORE+"/all.db";
	private static final String DB_R = SQL_STORE+"/subreddits.db";
	private static final String DB_U = SQL_STORE+"/users.db";
	
	private static SQL instance = new SQL();
	
	private Connection db_a;
	private Connection db_r;
	private Connection db_u;
	
	public static SQL getInstance() {
		return instance;
	}
	
	private SQL() {}
	
	public boolean init() {
		try {
			Class.forName(SQL_CLASS);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		
		db_a = openSQLConnection(DB_A);
		db_u = openSQLConnection(DB_U);
		db_r = openSQLConnection(DB_R);
		
		Statement stmt = null;
		
		try {
			stmt = db_a.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS table_posts "+SQLS_POST_CREATE_COLS);
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS table_comments "+SQLS_COMMENT_CREATE_COLS);
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS key_author "+SQLS_AUTHORKEY_CREATE_COLS);
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS key_subreddit "+SQLS_SUBREDDITKEY_CREATE_COLS);
			stmt.close();
			
			stmt = db_u.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS key_author "+SQLS_AUTHORKEY_CREATE_COLS);
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS key_subreddit "+SQLS_SUBREDDITKEY_CREATE_COLS);
			stmt.close();

			stmt = db_r.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS key_author "+SQLS_AUTHORKEY_CREATE_COLS);
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS key_subreddit "+SQLS_SUBREDDITKEY_CREATE_COLS);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return true;
	}
	
	public void close() {
		closeSQLConnection(db_a);
		closeSQLConnection(db_u);
		closeSQLConnection(db_r);
	}

	private static Connection openSQLConnection(String file) {
		Connection connection = null;
		try { connection = DriverManager.getConnection(SQL_CONNECTOR+":"+file); } catch(SQLException e) { e.printStackTrace(); }
		return connection;
	}

	private static void closeSQLConnection(Connection connection) {
		if (connection == null) return;
		try { connection.close(); } catch (SQLException e) { e.printStackTrace(); }
	}
	
	private static final String SQLS_POST_CREATE_COLS = "(" +
			"id VARCHAR(16) PRIMARY KEY," +
			"author_id VARCHAR(16)," +
			"subreddit_id VARCHAR(16)," +
			"title VARCHAR(256)," +
			"selftext TEXT," +
			"selftext_html TEXT," +
			"url VARCHAR(256)," +
			"created_utc BIGINT," +
			"ups INT," +
			"downs INT" +
			");";
	
	private static final String SQLS_COMMENT_CREATE_COLS = "(" +
			"id VARCHAR(16) PRIMARY KEY," +
			"parent_id VARCHAR(16)," +
			"link_id VARCHAR(16)," +
			"author_id VARCHAR(16)," +
			"subreddit_id VARCHAR(16)," +
			"title VARCHAR(256)," +
			"body TEXT," +
			"body_html TEXT," +
			"created_utc BIGINT," +
			"ups INT," +
			"downs INT" +
			");";

	private static final String SQLS_AUTHORKEY_CREATE_COLS = "(" +
			"id VARCHAR(16) PRIMARY KEY," +
			"name TEXT," +
			"created_utc BIGINT" +
			");";
	
	private static final String SQLS_SUBREDDITKEY_CREATE_COLS = "(" +
			"id VARCHAR(16) PRIMARY KEY," +
			"name TEXT" +
			");";
}
