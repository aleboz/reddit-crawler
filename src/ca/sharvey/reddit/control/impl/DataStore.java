package ca.sharvey.reddit.control.impl;

import java.util.HashMap;
import java.util.PriorityQueue;

import ca.sharvey.reddit.task.Properties;
import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.Type;
import ca.sharvey.reddit.task.crawl.AuthorCrawler;
import ca.sharvey.reddit.task.crawl.CommentCrawler;
import ca.sharvey.reddit.task.crawl.PostCrawler;
import ca.sharvey.reddit.task.crawl.SubredditCrawler;

public class DataStore {

	private PriorityQueue<Task> crawlTaskList = new PriorityQueue<Task>();
	private PriorityQueue<Task> processTaskList = new PriorityQueue<Task>();
	private PriorityQueue<Result> crawlResultList = new PriorityQueue<Result>();
	private PriorityQueue<Result> processResultList = new PriorityQueue<Result>();

	private PriorityQueue<Properties> propertiesList = new PriorityQueue<Properties>();
	
	private static final DataStore instance = new DataStore();
	
	private DataStore() {
		init();
	}
	
	public static DataStore getInstance() {
		return instance;
	}
	
	public static String typeToReddit(Type t) {
		switch (t) {
		case CRAWL_AUTHOR: return "t2";
		case CRAWL_SUBREDDIT: return "t5";
		case CRAWL_COMMENT: return "t1";
		case CRAWL_POST: return "t3";
		default:
			return "unknown";
		}
	}
	
	public Task nextCrawlTask() {
		synchronized(crawlTaskList) { return crawlTaskList.poll(); }
	}
	
	public Task nextProcessTask() {
		synchronized(processTaskList) { return processTaskList.poll(); }
	}
	
	public void addCrawlResult(Result r) {
		synchronized(crawlResultList) { crawlResultList.add(r); }
	}

	public void addProcessResult(Result r) {
		synchronized(processResultList) { processResultList.add(r); }
	}

	public void init() {
		SubredditCrawler initialTask = new SubredditCrawler("all");
		crawlTaskList.add(initialTask);
		SQL.getInstance().init();
	}
	
	public void printPropertyList() {
		for (Properties p : propertiesList) {
			processProperties(p);
			System.out.println(p.getProperty("kind"));
		}
	}

	public void pullProperties(HashMap<String, Properties> data, Type type) {
		if (type == Type.CRAWL_SUBREDDIT) {
			for (String key : data.keySet()) {
				Properties p = data.get(key);
				if (key.equalsIgnoreCase("more")) {
					SubredditCrawler sc = new SubredditCrawler(p.getProperty("id"), p.getProperty("after"), Integer.parseInt(p.getProperty("count")));
					synchronized(crawlTaskList) { crawlTaskList.add(sc); }
				} else {
					if (p.getProperty("kind").equalsIgnoreCase("t3")) {
						PostCrawler pc = new PostCrawler(p.getProperty("id"));
						synchronized(crawlTaskList) { crawlTaskList.add(pc); }
						CommentCrawler cc = new CommentCrawler(p.getProperty("id"));
						synchronized(crawlTaskList) { crawlTaskList.add(cc); }
					} 
				}
			}
			return;
		}
		
		for (String key : data.keySet()) {
			Properties p = data.get(key);
			if (key.equalsIgnoreCase("more")) {
				String link_id = p.getProperty("link_id");
				for (Object o : p.keySet()) {
					String id = (String)o;
					if (id.equalsIgnoreCase("link_id")) continue;
					CommentCrawler task = new CommentCrawler(link_id, id);
					synchronized(crawlTaskList) { crawlTaskList.add(task); }
				}
				continue;
			}
			String kind = p.getProperty("kind");
			if (kind.equalsIgnoreCase("t1") || kind.equalsIgnoreCase("t3")) {
				String author = p.getProperty("author");
				String author_id = SQL.getInstance().get_author_id(author);
				if (author_id == null) {
					AuthorCrawler ac = new AuthorCrawler(author);
					synchronized(crawlTaskList) { crawlTaskList.add(ac); }
				}
			}
			synchronized(propertiesList) { propertiesList.add(p); }
		}
	}

	private boolean processProperties(Properties properties) {
		String kind = properties.getProperty("kind");
		if (kind.equalsIgnoreCase("t1") || kind.equalsIgnoreCase("t3")) {
			String author_id = SQL.getInstance().get_author_id(properties.getProperty("author"));
			if (author_id == null) {
				return false;
			}
		}
		
		if (kind.equalsIgnoreCase("t1")) { //comment
			SQL.getInstance().insertComment(properties);
		} else if (kind.equalsIgnoreCase("t2")) { //author
			SQL.getInstance().insertAuthor(properties);
		} else if (kind.equalsIgnoreCase("t3")) { //link
			SQL.getInstance().insertPost(properties);
		}
		return true;
	}

	private class ResultProcessor extends Thread {
		public void run() {
			while (!isInterrupted()) {
				Result result = null;
				synchronized(crawlResultList) { result = crawlResultList.poll(); }
				if (result == null)
					try { sleep(200); } catch (InterruptedException e) {}
				else {
					System.out.println("Processing result from "+result.getTask().getID()+" ("+typeToReddit(result.getTask().getType())+")");
					pullProperties(result.getData(), result.getTask().getType());
					System.gc();
				}
			}
		}
	};
	
	private class PropertiesProcessor extends Thread {
		public void run() {
			while (!isInterrupted()) {
				Properties properties = null;
				synchronized (propertiesList) { properties = propertiesList.peek(); }
				if (properties == null)
					try { sleep(50); } catch (InterruptedException e) {}
				else {
					boolean result = processProperties(properties);
					if (result) {
						System.out.println("Processing property "+properties.getProperty("kind")+"_"+properties.getProperty("id"));
						synchronized(propertiesList) { propertiesList.remove(properties); }
					}
					System.gc();
				}
				try { sleep(50); } catch (InterruptedException e) {}
			}
		}
	}

	public void startProcessors() {
		ResultProcessor rp = new ResultProcessor();
		rp.start();
		PropertiesProcessor pp = new PropertiesProcessor();
		pp.start();
	};
}
