package ca.sharvey.reddit;

import java.util.HashMap;
import java.util.Properties;

import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.crawl.CommentCrawler;

public class Test {
	public static void main(String[] args) {
		//PostCrawler pc = new PostCrawler("13y4ai");
		//pc.execute();
		
		CommentCrawler cc = new CommentCrawler("146gop");
		Result r = cc.execute();
		HashMap<String, Properties> data = r.getData();
		
		for (String s : data.keySet()) {
			Properties p = data.get(s);
			System.out.println(s+" => ");
			for (Object o : p.keySet()) {
				String v = p.getProperty((String)o);
				System.out.println("   "+o+" ==> "+v);
			}
		}
	}
}
