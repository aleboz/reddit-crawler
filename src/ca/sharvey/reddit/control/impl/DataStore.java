package ca.sharvey.reddit.control.impl;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.crawl.AuthorCrawler;
import ca.sharvey.reddit.task.crawl.CommentCrawler;
import ca.sharvey.reddit.task.crawl.PostCrawler;
import ca.sharvey.reddit.task.crawl.SubredditCrawler;

public class DataStore {

	private ConcurrentLinkedQueue<Task> crawlTaskList = new ConcurrentLinkedQueue<Task>();
	private ConcurrentLinkedQueue<Task> processTaskList = new ConcurrentLinkedQueue<Task>();
	private ConcurrentLinkedQueue<Result> crawlResultList = new ConcurrentLinkedQueue<Result>();
	private ConcurrentLinkedQueue<Result> processResultList = new ConcurrentLinkedQueue<Result>();
	
	private static final DataStore instance = new DataStore();
	
	private DataStore() {
		init();
	}
	
	public static DataStore getInstance() {
		return instance;
	}
	
	public Task nextCrawlTask() {
		synchronized(crawlTaskList) { return crawlTaskList.poll(); }
	}
	
	public Task nextProcessTask() {
		synchronized(processTaskList) { return processTaskList.poll(); }
	}

	public void init() {
		SubredditCrawler initialTask = new SubredditCrawler("all");
		crawlTaskList.add(initialTask);
		SQL.getInstance().init();
	}
	
	public ArrayList<Task> pollForAuthors(Result result) {
		ArrayList<Task> taskList = new ArrayList<Task>();
		for (String key : result.getData().keySet()) {
			if (key.equals("more")) {
				continue;
			}
			Properties properties = result.getData().get(key);
			String author = properties.getProperty("author");
			String author_id = SQL.getInstance().get_author_id(author);
			if (author_id == null) {
				taskList.add(new AuthorCrawler(author));
			}
		}
		if (taskList.size() == 0) return null;
		return taskList;
	}

	public void processPostResult(Result result) {
		for (String key : result.getData().keySet()) {
			Properties properties = result.getData().get(key);
			String id = properties.getProperty("id");
			System.out.println(id);
			SQL.getInstance().insertPost(properties);
		}
	}

	public ArrayList<Task> processCommentResult(Result result) {
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


			System.out.println(id);
			SQL.getInstance().insertComment(properties);
		}
		return taskList;
	}

	public ArrayList<Task> processListingResult(Result result) {
		ArrayList<Task> tasks = new ArrayList<Task>();

		for (String k : result.getData().keySet()) {
			Properties p = result.getData().get(k);
			if (k.equalsIgnoreCase("more")) {
				switch (result.getTask().getType()) {
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
}
