package ca.sharvey.reddit.task.crawl;

import java.net.MalformedURLException;
import java.net.URL;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.Result;

public class PostCrawler extends Crawl {

	private static final long serialVersionUID = 1585328031012008706L;
	private String postID;
	private String afterID;
	private String count;

	
	
	@Override
	URL formURL() throws MalformedURLException {
		return new URL(Main.BASE_SITE+"/");
	}

	@Override
	public Result execute() {
		// TODO Auto-generated method stub
		return null;
	}

}
