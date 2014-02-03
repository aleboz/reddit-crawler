package ca.sharvey.reddit.control;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Calendar;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.control.impl.DataStore;
import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.Type;

public class Slave {

	private String hostname;
	private Master host;
	private String me;
	private int numCores;
	private Thread[] threads;

	private long lastRequested = 0;

	public Slave(String hostname) {
		this.hostname = hostname;
		try { me = InetAddress.getLocalHost().getHostName(); } catch (UnknownHostException e) { e.printStackTrace(); me = "localhost"; }
		numCores = Runtime.getRuntime().availableProcessors();
		threads = new Thread[numCores];
	}

	public void start() {
		try {
			String serverObjectName = "//"+hostname+"/"+Main.RMI_NAME; 
			host = ( Master ) Naming.lookup( serverObjectName );
			host.registerHost(me, numCores);
			System.out.println("Connected to "+ serverObjectName );

			for (int i = 0; i < numCores; i++) {
				threads[i] = new Processor();
				threads[i].start();
			}
		}
		catch ( java.rmi.ConnectException ce ) {
			ce.printStackTrace();
			System.err.println( "Connection to server failed. " + "Server may be temporarily unavailable." );
		}
		catch ( Exception e ) { 
			e.printStackTrace();
			System.exit( 1 ); 
		}      
	}
	
	public Type whatShouldIRequest() {
		synchronized (this) {
			long now = Calendar.getInstance().getTimeInMillis();
			if (lastRequested == 0 || (now - lastRequested) > 2000 ) {
				lastRequested = now;
				return Type.CRAWL;
			}
			return Type.PROCESS;
		}
	}

	public class Processor extends Thread {
		public void run() {
			while (!isInterrupted()) {
				try {
					Task t = null;
					Result r = null;
					Type ty = Type.PROCESS;
					while (t == null) {
						if (ty == Type.PROCESS) ty = whatShouldIRequest();
						t = host.getTask(me, ty);
						try { Thread.sleep(50); } catch (InterruptedException e) {}
					}
					r = t.execute();
					System.out.println("Executed "+typeToReddit(t.getType())+" ("+t.getID()+")");
					host.updateResult(me, ty, r);
					try { sleep(50); } catch (InterruptedException e) {}
					System.gc();
				} catch (RemoteException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
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
}
