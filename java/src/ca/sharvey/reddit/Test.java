package ca.sharvey.reddit;

import ca.sharvey.reddit.control.impl.DataStore;
import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.crawl.AuthorCrawler;
import ca.sharvey.reddit.task.crawl.PostCrawler;

public class Test {
	public static void main(String[] args) {
		Result r;

		AuthorCrawler ac = new AuthorCrawler("worldwise001");
		r = ac.execute();
		DataStore.getInstance().pullProperties(r.getData(), r.getTask().getType());
		
		PostCrawler pc = new PostCrawler("s3eb7");
		r = pc.execute();
		DataStore.getInstance().pullProperties(r.getData(), r.getTask().getType());
		
		DataStore.getInstance().printPropertyList();
		
		//CommentCrawler cc = new CommentCrawler("146gop");//, "c7ac6w0");
		//r = cc.execute();


		//SubredditCrawler sc = new SubredditCrawler("uwaterloo");//, "t3_13ucru", 25);
		//Result r = sc.execute();
		
		/*
		HashMap<String, Properties> data = r.getData();
		for (String s : data.keySet()) {
			Properties p = data.get(s);
			System.out.println(s+" => ");
			for (Object o : p.keySet()) {
				String v = p.getProperty((String)o);
				System.out.println("   "+o+" ==> "+v);
			}
		}
		*/
	}
}
