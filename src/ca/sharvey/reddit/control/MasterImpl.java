package ca.sharvey.reddit.control;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.Type;

public class MasterImpl implements Master, Serializable {

	private static final long serialVersionUID = -2092385164211184506L;
	public static final int DEFAULT_PORT = 1099;

	private HashMap<String, Integer> hostList = new HashMap<String, Integer>();
	private LinkedBlockingQueue<Task> crawlTaskList = new LinkedBlockingQueue<Task>();
	private LinkedBlockingQueue<Task> processTaskList = new LinkedBlockingQueue<Task>();
	private LinkedBlockingQueue<Result> crawlResultList = new LinkedBlockingQueue<Result>();
	private LinkedBlockingQueue<Result> processResultList = new LinkedBlockingQueue<Result>();

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
			processor.start();
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Started up!");
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
	public synchronized Task getTask(String host, Type type) throws RemoteException {
		switch (type) {
		case CRAWL:
			synchronized (crawlTaskList) { return crawlTaskList.poll(); }
		case PROCESS:
			synchronized (processTaskList) { return processTaskList.poll(); }
		default:
			return null;
		}
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
	
	private void init() {
		
	}

	private Thread processor = new Thread() {
		public void run() {
			init();
			while (!isInterrupted()) {
				try {
					sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
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
