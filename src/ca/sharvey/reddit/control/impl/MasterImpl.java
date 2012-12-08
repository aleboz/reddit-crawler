package ca.sharvey.reddit.control.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.control.Master;
import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.Type;

public class MasterImpl implements Master, Serializable {

	private static final long serialVersionUID = -2092385164211184506L;
	public static final int DEFAULT_PORT = 1099;

	private HashMap<String, Integer> hostList = new HashMap<String, Integer>();

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
			DataStore.getInstance().startProcessors();
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Started up!");
	}

	public synchronized void registerHost(String host, int cores) throws RemoteException {
		hostList.put(host, cores);
		System.out.println("Host connected: "+host+" with "+cores+" cores.");
	}

	public Task[] getTasks(String host, Type type, int num) throws RemoteException {
		Task[] tasks = new Task[num];
		for (int i = 0; i < num; i++)
			tasks[i] = getTask(host, type);
		return tasks;
	}

	public void updateResults(String host, Type type, Result[] results) throws RemoteException {
		for (Result r : results) {
			updateResult(host, type, r);
		}
	}

	public Task getTask(String host, Type type) throws RemoteException {
		Task t = null;
		switch (type) {
		case CRAWL:
			t = DataStore.getInstance().nextCrawlTask();
			if (t == null) System.out.println("error!");
			else
				System.out.println(DataStore.typeToReddit(t.getType())+" ("+t.getID()+") --> "+host);
			break;
		case PROCESS:
			t = DataStore.getInstance().nextProcessTask(); break;
		default:
			return null;
		}
		return t;
	}

	public void updateResult(String host, Type type, Result result)
			throws RemoteException {
		switch (type) {
		case CRAWL:
			Task t = result.getTask();
			System.out.println(DataStore.typeToReddit(t.getType())+" ("+t.getID()+") <-- "+host);
			DataStore.getInstance().addCrawlResult(result);
			break;
		case PROCESS:
			DataStore.getInstance().addProcessResult(result);
			break;
		default:
			break;
		}
	}

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
				} catch(IOException e) { e.printStackTrace(); } 
				catch (InterruptedException e) { e.printStackTrace(); } 
			} 
		}
	};

}
