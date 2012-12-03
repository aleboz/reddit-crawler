package ca.sharvey.reddit.task.crawl;

import java.net.MalformedURLException;
import java.net.URL;

import ca.sharvey.reddit.Main;


public class AuthorCrawler extends ListingCrawler {
	
	private static final long serialVersionUID = 4765932750290452711L;

	public AuthorCrawler(String id) {
		super(id);
	}
	
	public AuthorCrawler(String id, String after, int count) {
		super(id, after, count);
	}

	@Override
	URL formURL() throws MalformedURLException {
		if (after == null)
			return new URL(Main.BASE_SITE+"/user/"+id+".json");
		else
			return new URL(Main.BASE_SITE+"/user/"+id+".json?count="+count+"&after="+after);
	}


}
