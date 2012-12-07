package ca.sharvey.reddit.task.crawl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.Type;


public class AuthorCrawler extends Crawler {
	
	private static final long serialVersionUID = 4765932750290452711L;

	public AuthorCrawler(String id) {
		this.id = id;
		setType(Type.CRAWL_AUTHOR);
	}
	
	public AuthorCrawler(String id, String after, int count) {
		super(id, after, count);
		setType(Type.CRAWL_AUTHOR);
	}

	@Override
	URL formURL() throws MalformedURLException {
		if (after == null)
			return new URL(Main.BASE_SITE+"/user/"+id+".json");
		else
			return new URL(Main.BASE_SITE+"/user/"+id+".json?count="+count+"&after="+after);
	}

	@Override
	HashMap<String, Properties> processData(String content) {
		// TODO Auto-generated method stub
		return null;
	}


}
