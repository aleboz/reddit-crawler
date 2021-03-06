package ca.sharvey.reddit.task.crawl;

import java.net.MalformedURLException;
import java.net.URL;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.Type;


public class SubredditCrawler extends ListingCrawler {

	private static final long serialVersionUID = 4748522194838572055L;

	public SubredditCrawler(String id) {
		super(id);
		setType(Type.CRAWL_SUBREDDIT);
	}
	
	public SubredditCrawler(String id, String after, int count) {
		super(id, after, count);
		setType(Type.CRAWL_SUBREDDIT);
	}

	@Override
	URL formURL() throws MalformedURLException {
		if (after == null)
			return new URL(Main.BASE_SITE+"/r/"+id+".json");
		else
			return new URL(Main.BASE_SITE+"/r/"+id+".json?count="+count+"&after="+after);
	}


}
