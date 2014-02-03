package ca.sharvey.reddit;

import java.rmi.RemoteException;

import ca.sharvey.reddit.control.Slave;
import ca.sharvey.reddit.control.impl.MasterImpl;

public class Main {
	
	public static final String RMI_NAME = "RedditCrawler";
	public static final String BASE_SITE = "http://www.reddit.com";
	public static final String USER_AGENT = "Reddit Crawler written by /u/worldwise001 (http://github.com/worldwise001/reddit-crawler)";

	public static void main(String[] args) throws RemoteException {
		if (args.length < 1) {
			System.out.println("I need some arguments!");
			System.out.println("Some possibilities:");
			System.out.printf("   -master\n");
			System.out.printf("   -slave <host>\n");
			System.exit(1);
		} else {
			if (args[0].equalsIgnoreCase("-master")) {
				MasterImpl master = new MasterImpl();
				master.start();
			} else if (args[0].equalsIgnoreCase("-slave")) {
				if (args.length < 2) {
					System.out.println("I need a host!");
					System.exit(1);
				} else {
					Slave slave = new Slave(args[1]);
					slave.start();
				}
			} else {
				System.out.println("I need better arguments!");
				System.out.println("Some possibilities:");
				System.out.printf("   -master\n");
				System.out.printf("   -slave <host>\n");
				System.exit(1);
			}
		}
	}
}
