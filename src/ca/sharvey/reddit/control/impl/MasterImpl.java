package ca.sharvey.reddit.control.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.control.Master;
import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.Type;
import ca.sharvey.reddit.task.crawl.AuthorCrawler;
import ca.sharvey.reddit.task.crawl.CommentCrawler;
import ca.sharvey.reddit.task.crawl.PostCrawler;
import ca.sharvey.reddit.task.crawl.SubredditCrawler;

public class MasterImpl implements Master, Serializable {

	private static final long serialVersionUID = -2092385164211184506L;
	public static final int DEFAULT_PORT = 1099;

	private HashMap<String, Integer> hostList = new HashMap<String, Integer>();
	private Thread[] threads;

	public MasterImpl() throws RemoteException {
		super();
	}

	public void start() {
		rmiRegistry.start();
		try { Thread.sleep(1000); } catch (InterruptedException e) {}
		try {
			Master stub = (Master) UnicastRemoteObject.exportObject(this, 0);
			Registry registry = LocateRegistry.getRegistry();
			registry.rebind(Main.RMI_NAME, stub);
			startProcessors();
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Started up!");
	}

	private void startProcessors() {
		threads = new Processor[Runtime.getRuntime().availableProcessors()];
		for (Thread t : threads) {
			t.start();
		}
	}

	@Override
	public synchronized void registerHost(String host, int cores) throws RemoteException {
		hostList.put(host, cores);
		System.out.println("Host connected: "+host+" with "+cores+" cores.");
	}

	@Override
	public Task[] getTasks(String host, Type type, int num) throws RemoteException {
		Task[] tasks = new Task[num];
		for (int i = 0; i < num; i++)
			tasks[i] = getTask(host, type);
		return tasks;
	}

	@Override
	public void updateResults(String host, Type type, Result[] results) throws RemoteException {
		for (Result r : results) {
			updateResult(host, type, r);
		}
	}

	@Override
	public Task getTask(String host, Type type) throws RemoteException {
		Task t = null;
		switch (type) {
		case CRAWL:
			t = DataStore.getInstance().nextCrawlTask(); break;
		case PROCESS:
			t = DataStore.getInstance().nextProcessTask(); break;
		default:
			return null;
		}

		if (t != null)
			System.out.println("Sending task "+t.getUID()+" to "+host);
		else
			System.out.println("Polling for no tasks");
		return t;
	}

	@Override
	public void updateResult(String host, Type type, Result result)
			throws RemoteException {
		switch (type) {
		case CRAWL:
			synchronized (crawlResultList) { crawlResultList.add(result); }
			break;
		case PROCESS:
			synchronized (processResultList) { processResultList.add(result); }
			break;
		default:
			break;
		}
	}

	private class Processor extends Thread {
		public void run() {
			while (!isInterrupted()) {
				Result result = crawlResultList.poll();
				if (result == null)
					try { sleep(1000); } catch (InterruptedException e) {}
				else {
					ArrayList<Task> tasks = null;
					switch (result.getTask().getType()) {
					case CRAWL_POST:
						tasks = pollForAuthors(result);
						if (tasks != null) {
							synchronized (crawlTaskList) { crawlTaskList.addAll(tasks); }
							synchronized (crawlResultList) { crawlResultList.add(result); }
						}
						processPostResult(result);
						break;
					case CRAWL_COMMENT:
						processCommentResult(result);
						break;
					case CRAWL_SUBREDDIT:
						tasks = processListingResult(result);
						synchronized (crawlTaskList) { crawlTaskList.addAll(tasks); }
						break;
					default: break;
					}
				}
			}
		}
	};

	private Thread rmiRegistry = new Thread() {
		public void run() {
			{ 
				try 
				{ 
					System.out.println("Starting rmiregistry...");
					Process p=Runtime.getRuntime().exec("rmiregistry");
					int returned = p.waitFor();
					BufferedReader reader=new BufferedReader(new InputStreamReader(p.getInputStream())); 
					String line=reader.readLine(); 
					while(line!=null) 
					{ 
						System.out.println(line); 
						line=reader.readLine(); 
					} 
					System.out.println("Exited with exit code "+returned);
					if (returned != 0) System.exit(returned);
				} 
				catch(IOException | InterruptedException e) { e.printStackTrace(); } 
			} 
		}
	};

}
