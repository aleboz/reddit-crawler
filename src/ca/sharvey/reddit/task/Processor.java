package ca.sharvey.reddit.task;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.crawl.AuthorCrawler;
import ca.sharvey.reddit.task.crawl.CommentCrawler;
import ca.sharvey.reddit.task.crawl.PostCrawler;
import ca.sharvey.reddit.task.crawl.SubredditCrawler;

public class Processor {

	public static final String DB_ALL = Main.SQLITE_STORE+"/all.db";
	public static final String DB_R = Main.SQLITE_STORE+"/subreddits.db";
	public static final String DB_U = Main.SQLITE_STORE+"/users.db";

	public static void processPostResult(Result result) {
		for (String key : result.getData().keySet()) {
			Properties properties = result.getData().get(key);
			String id = properties.getProperty("id");
			System.out.println(id);
			Connection connection = null;
			Statement stmt = null;
			try {
				connection = openSQLConnection(DB_R);
				if (connection != null) {
					String subreddit = properties.getProperty("subreddit");
					stmt = connection.createStatement();
					stmt.execute("BEGIN TRANSACTION;");
					stmt.execute(String.format(
							"CREATE TABLE IF NOT EXISTS %s_posts (" +
									"id VARCHAR(16) PRIMARY KEY," +
									"kind VARCHAR(3)," +
									"name TEXT," +
									"title TEXT," +
									"author TEXT," +
									"selftext TEXT," +
									"selftext_html TEXT," +
									"url TEXT," +
									"subreddit TEXT," +
									"subreddit_id VARCHAR(16)," +
									"created_utc BIGINT," +
									"ups INT," +
									"downs INT" +
									");"
									, subreddit));
					stmt.execute(String.format("INSERT OR REPLACE INTO %s_posts (id) VALUES ('%s');",subreddit,id));
					for (Object o : properties.keySet()) {
						if (o.equals("id")) continue;
						String k = (String)o;
						String v = (String)properties.get(o);
						if (k.equals("created_utc") || k.equals("ups") || k.equals("downs")) {
							long l = Long.parseLong(v);
							stmt.execute(String.format("UPDATE %s_posts SET %s = %d WHERE id = '%s';", subreddit,k,l,id));
						} else {
							stmt.execute(String.format("UPDATE %s_posts SET %s = '%s' WHERE id = '%s';", subreddit,k,v.replace("'", "''"),id));
						}
					}
					stmt.execute("COMMIT;");
					closeSQLConnection(connection);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				connection = openSQLConnection(DB_U);
				if (connection != null) {
					String author = properties.getProperty("author");
					stmt = connection.createStatement();
					stmt.execute("BEGIN TRANSACTION;");
					stmt.execute(String.format(
							"CREATE TABLE IF NOT EXISTS %s_posts (" +
									"id VARCHAR(16) PRIMARY KEY," +
									"kind VARCHAR(3)," +
									"name TEXT," +
									"title TEXT," +
									"author TEXT," +
									"selftext TEXT," +
									"selftext_html TEXT," +
									"url TEXT," +
									"subreddit TEXT," +
									"subreddit_id VARCHAR(16)," +
									"created_utc BIGINT," +
									"ups INT," +
									"downs INT" +
									");"
									, author));
					stmt.execute(String.format("INSERT OR REPLACE INTO %s_posts (id) VALUES ('%s');",author,id));
					for (Object o : properties.keySet()) {
						if (o.equals("id")) continue;
						String k = (String)o;
						String v = (String)properties.get(o);
						if (k.equals("created_utc") || k.equals("ups") || k.equals("downs")) {
							long l = Long.parseLong(v);
							stmt.execute(String.format("UPDATE %s_posts SET %s = %d WHERE id = '%s';", author,k,l,id));
						} else {
							stmt.execute(String.format("UPDATE %s_posts SET %s = '%s' WHERE id = '%s';", author,k,v.replace("'", "''"),id));
						}
					}
					stmt.execute("COMMIT;");
					closeSQLConnection(connection);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				connection = openSQLConnection(DB_ALL);
				if (connection != null) {
					stmt = connection.createStatement();
					stmt.execute("BEGIN TRANSACTION;");
					stmt.execute(String.format("INSERT OR REPLACE INTO posts (id) VALUES ('%s');",id));
					for (Object o : properties.keySet()) {
						if (o.equals("id")) continue;
						String k = (String)o;
						String v = (String)properties.get(o);
						if (k.equals("created_utc") || k.equals("ups") || k.equals("downs")) {
							long l = Long.parseLong(v);
							stmt.execute(String.format("UPDATE posts SET %s = %d WHERE id = '%s';", k,l,id));
						} else {
							stmt.execute(String.format("UPDATE posts SET %s = '%s' WHERE id = '%s';", k,v.replace("'", "''"),id));
						}
					}
					stmt.execute("COMMIT;");
					closeSQLConnection(connection);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static ArrayList<Task> processCommentResult(Result result) {
		ArrayList<Task> taskList = new ArrayList<Task>();
		for (String key : result.getData().keySet()) {
			if (key.equals("more")) {
				Properties more = result.getData().get(key);
				for (Object o : more.keySet()) {
					String id = (String)o;
					CommentCrawler task = new CommentCrawler(result.getTask().getID(), id);
					taskList.add(task);
				}
				continue;
			}
			Properties properties = result.getData().get(key);
			String id = properties.getProperty("id");

			Connection connection = null;
			try {
				connection = openSQLConnection(DB_R);
				if (connection != null) {
					String subreddit = properties.getProperty("subreddit");
					Statement stmt = connection.createStatement();
					stmt.execute("BEGIN TRANSACTION;");
					stmt.execute(String.format(
							"CREATE TABLE IF NOT EXISTS %s_comments (" +
									"id VARCHAR(16) PRIMARY KEY," +
									"kind VARCHAR(3)," +
									"name TEXT," +
									"title TEXT," +
									"author TEXT," +
									"body TEXT," +
									"body_html TEXT," +
									"parent_id VARCHAR(16)," +
									"link_id VARCHAR(16)," +
									"subreddit TEXT," +
									"subreddit_id VARCHAR(16)," +
									"created_utc BIGINT," +
									"ups INT," +
									"downs INT" +
									");"
									, subreddit));
					stmt.execute(String.format("INSERT OR REPLACE INTO %s_comments (id) VALUES ('%s');",subreddit,id));
					for (Object o : properties.keySet()) {
						if (o.equals("id")) continue;
						String k = (String)o;
						String v = (String)properties.get(o);
						if (k.equals("created_utc") || k.equals("ups") || k.equals("downs")) {
							long l = Long.parseLong(v);
							stmt.execute(String.format("UPDATE %s_comments SET %s = %d WHERE id = '%s';", subreddit,k,l,id));
						} else {
							stmt.execute(String.format("UPDATE %s_comments SET %s = '%s' WHERE id = '%s';", subreddit,k,v.replace("'", "''"),id));
						}
					}
					stmt.execute("COMMIT;");
					closeSQLConnection(connection);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				connection = openSQLConnection(DB_U);
				if (connection != null) {
					String author = properties.getProperty("author");
					Statement stmt = connection.createStatement();
					stmt.execute("BEGIN TRANSACTION;");
					stmt.execute(String.format(
							"CREATE TABLE IF NOT EXISTS %s_comments (" +
									"id VARCHAR(16) PRIMARY KEY," +
									"kind VARCHAR(3)," +
									"name TEXT," +
									"title TEXT," +
									"author TEXT," +
									"body TEXT," +
									"body_html TEXT," +
									"parent_id VARCHAR(16)," +
									"link_id VARCHAR(16)," +
									"subreddit TEXT," +
									"subreddit_id VARCHAR(16)," +
									"created_utc BIGINT," +
									"ups INT," +
									"downs INT" +
									");"
									, author));
					stmt.execute(String.format("INSERT OR REPLACE INTO %s_comments (id) VALUES ('%s');",author,id));
					for (Object o : properties.keySet()) {
						if (o.equals("id")) continue;
						String k = (String)o;
						String v = (String)properties.get(o);
						if (k.equals("created_utc") || k.equals("ups") || k.equals("downs")) {
							long l = Long.parseLong(v);
							stmt.execute(String.format("UPDATE %s_comments SET %s = %d WHERE id = '%s';", author,k,l,id));
						} else {
							stmt.execute(String.format("UPDATE %s_comments SET %s = '%s' WHERE id = '%s';", author,k,v.replace("'", "''"),id));
						}
					}
					stmt.execute("COMMIT;");
					closeSQLConnection(connection);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				connection = openSQLConnection(DB_ALL);
				if (connection != null) {
					Statement stmt = connection.createStatement();
					stmt.execute("BEGIN TRANSACTION;");
					stmt.execute(String.format("INSERT OR REPLACE INTO comments (id) VALUES ('%s');",id));
					for (Object o : properties.keySet()) {
						if (o.equals("id")) continue;
						String k = (String)o;
						String v = (String)properties.get(o);
						if (k.equals("created_utc") || k.equals("ups") || k.equals("downs")) {
							long l = Long.parseLong(v);
							stmt.execute(String.format("UPDATE comments SET %s = %d WHERE id = '%s';", k,l,id));
						} else {
							stmt.execute(String.format("UPDATE comments SET %s = '%s' WHERE id = '%s';", k,v.replace("'", "''"),id));
						}
					}
					stmt.execute("COMMIT;");
					closeSQLConnection(connection);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return taskList;
	}

	public static ArrayList<Task> processListingResult(Result result) {
		ArrayList<Task> tasks = new ArrayList<Task>();

		for (String k : result.getData().keySet()) {
			Properties p = result.getData().get(k);
			if (k.equalsIgnoreCase("more")) {
				switch (result.getTask().getType()) {
				case CRAWL_AUTHOR:
					AuthorCrawler ac = new AuthorCrawler(p.getProperty("id"), p.getProperty("after"), Integer.parseInt(p.getProperty("count")));
					tasks.add(ac);
					break;
				case CRAWL_SUBREDDIT:
					SubredditCrawler sc = new SubredditCrawler(p.getProperty("id"), p.getProperty("after"), Integer.parseInt(p.getProperty("count")));
					tasks.add(sc);
					break;
				default:
				}
			} else {
				if (p.getProperty("kind").equalsIgnoreCase("t3")) {
					PostCrawler pc = new PostCrawler(p.getProperty("id"));
					tasks.add(pc);
					CommentCrawler cc = new CommentCrawler(p.getProperty("id"));
					tasks.add(cc);
				} else if (p.getProperty("kind").equalsIgnoreCase("t1")) {
					
				}
			}
		}

		return tasks;
	}

	public static void initSQL() {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			System.exit(1);
		}
		Connection connection = openSQLConnection(DB_ALL);
		try {
			Statement stmt = connection.createStatement();
			stmt.execute(
					"CREATE TABLE IF NOT EXISTS posts (" +
							"id VARCHAR(16) PRIMARY KEY," +
							"kind VARCHAR(3)," +
							"name TEXT," +
							"title TEXT," +
							"author TEXT," +
							"selftext TEXT," +
							"selftext_html TEXT," +
							"url TEXT," +
							"subreddit TEXT," +
							"subreddit_id VARCHAR(16)," +
							"created_utc BIGINT," +
							"ups INT," +
							"downs INT" +
							");"
					);
			stmt.execute(
					"CREATE TABLE IF NOT EXISTS comments (" +
							"id VARCHAR(16) PRIMARY KEY," +
							"kind VARCHAR(3)," +
							"name TEXT," +
							"title TEXT," +
							"author TEXT," +
							"body TEXT," +
							"body_html TEXT," +
							"parent_id VARCHAR(16)," +
							"link_id VARCHAR(16)," +
							"subreddit TEXT," +
							"subreddit_id VARCHAR(16)," +
							"created_utc BIGINT," +
							"ups INT," +
							"downs INT" +
							");"
					);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		closeSQLConnection(connection);
	}

	private static Connection openSQLConnection(String file) {
		Connection connection = null;
		try { connection = DriverManager.getConnection("jdbc:sqlite:"+file); } catch(SQLException e) { e.printStackTrace(); }
		return connection;
	}

	private static void closeSQLConnection(Connection connection) {
		if (connection == null) return;
		try { connection.close(); } catch (SQLException e) { e.printStackTrace(); }
	}
}
