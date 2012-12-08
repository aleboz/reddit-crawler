package ca.sharvey.reddit.control.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

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
	
	public String get_author_id(String name) {
		try {
			PreparedStatement stmt = db_a.prepareStatement("SELECT id FROM key_author WHERE name = ?");
			stmt.setString(1, name);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String id = rs.getString("id");
				return id;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void insertPost(Properties p) {
		insertPostToDB(p, db_a, null);
		insertPostToDB(p, db_r, "_"+p.getProperty("subreddit_id"));
		insertPostToDB(p, db_u, "_"+get_author_id(p.getProperty("author")));
	}
	
	private void insertPostToDB(Properties p, Connection db, String prefix) {
		try {
			String author_id = get_author_id(p.getProperty("author"));
			String table_name = (prefix == null)?"posts":prefix+"_posts";
			
			Statement stmt = db.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS "+table_name+" "+SQLS_POST_CREATE_COLS);
			stmt.close();
			
			db.setAutoCommit(false);
			PreparedStatement pstmt = null;
			
			pstmt = db.prepareStatement("INSERT OR REPLACE INTO "+table_name+" " +
					"(id,author_id,subreddit_id,title,selftext,selftext_html,url,created_utc,ups,downs) " +
					"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?);");
			pstmt.setString(1, p.getProperty("id"));
			pstmt.setString(2, author_id);
			pstmt.setString(3, p.getProperty("subreddit_id"));
			pstmt.setString(4, p.getProperty("title"));
			pstmt.setString(5, p.getProperty("selftext"));
			pstmt.setString(6, p.getProperty("selftext_html"));
			pstmt.setString(7, p.getProperty("url"));
			pstmt.setLong(8, Long.parseLong(p.getProperty("created_utc")));
			pstmt.setInt(9, Integer.parseInt(p.getProperty("ups")));
			pstmt.setInt(10, Integer.parseInt(p.getProperty("downs")));
			pstmt.execute();
			pstmt.close();
			
			pstmt = db.prepareStatement("INSERT OR REPLACE INTO key_subreddit " +
					"(id,name) " +
					"VALUES " +
					"(?,?);");
			pstmt.setString(1, p.getProperty("subreddit_id"));
			pstmt.setString(2, p.getProperty("subreddit"));
			pstmt.execute();
			pstmt.close();
			
			db.commit();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void insertComment(Properties p) {
		insertCommentToDB(p, db_a, null);
		insertCommentToDB(p, db_r, "_"+p.getProperty("subreddit_id"));
		insertCommentToDB(p, db_u, "_"+get_author_id(p.getProperty("author")));
	}
	
	private void insertCommentToDB(Properties p, Connection db, String prefix) {
		try {
			String author_id = get_author_id(p.getProperty("author"));
			String table_name = (prefix == null)?"comments":prefix+"_comments";
			
			Statement stmt = db.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS "+table_name+" "+SQLS_POST_CREATE_COLS);
			stmt.close();
			
			db.setAutoCommit(false);
			PreparedStatement pstmt = null;
			
			pstmt = db.prepareStatement("INSERT OR REPLACE INTO "+table_name+" " +
					"(id,parent_id,link_id,author_id,subreddit_id,title,body,body_html,url,created_utc,ups,downs) " +
					"VALUES " +
					"(?,?,?,?,?,?,?,?,?,?,?,?);");
			pstmt.setString(1, p.getProperty("id"));
			pstmt.setString(2, p.getProperty("parent_id").split("_")[1]);
			pstmt.setString(3, p.getProperty("link_id").split("_")[1]);
			pstmt.setString(4, author_id);
			pstmt.setString(5, p.getProperty("subreddit_id"));
			pstmt.setString(6, p.getProperty("title"));
			pstmt.setString(7, p.getProperty("selftext"));
			pstmt.setString(8, p.getProperty("selftext_html"));
			pstmt.setString(9, p.getProperty("url"));
			pstmt.setLong(10, Long.parseLong(p.getProperty("created_utc")));
			pstmt.setInt(11, Integer.parseInt(p.getProperty("ups")));
			pstmt.setInt(12, Integer.parseInt(p.getProperty("downs")));
			pstmt.execute();
			pstmt.close();
			
			pstmt = db.prepareStatement("INSERT OR REPLACE INTO key_subreddit " +
					"(id,name) " +
					"VALUES " +
					"(?,?);");
			pstmt.setString(1, p.getProperty("subreddit_id"));
			pstmt.setString(2, p.getProperty("subreddit"));
			pstmt.execute();
			pstmt.close();
			
			db.commit();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
