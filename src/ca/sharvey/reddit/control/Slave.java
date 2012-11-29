package ca.sharvey.reddit.control;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Calendar;

import ca.sharvey.reddit.Main;
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

	public class Processor extends Thread {
		public void run() {
			while (!isInterrupted()) {
				try {
					Task t = null;
					Result r = null;
					Type ty = null;
					synchronized (this) {
						long now = Calendar.getInstance().getTimeInMillis();
						if (lastRequested == 0 || (now - lastRequested) > 2000 ) {
							lastRequested = now;
							ty = Type.CRAWL;
						}
					}
					if (ty == null) ty = Type.PROCESS;
					while (t == null) {
						t = host.getTask(me, ty);
						try { Thread.sleep(500); } catch (InterruptedException e) {}
					}
					r = t.execute();
					host.updateResult(me, ty, r);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
