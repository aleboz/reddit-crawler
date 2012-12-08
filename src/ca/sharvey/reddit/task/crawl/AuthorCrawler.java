package ca.sharvey.reddit.task.crawl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.Type;


public class AuthorCrawler extends Crawler {
	
	private static final long serialVersionUID = 4765932750290452711L;

	public AuthorCrawler(String id) {
		this.id = id;
		setType(Type.CRAWL_AUTHOR);
	}

	@Override
	URL formURL() throws MalformedURLException {
		return new URL(Main.BASE_SITE+"/user/"+id+"/about.json");
	}

	@Override
	HashMap<String, Properties> processData(String content) {
		try {
			HashMap<String, Properties> data = new HashMap<String, Properties>();
			JSONObject obj = new JSONObject(content).getJSONObject("data");
			Properties p = new Properties();
			p.setProperty("name", obj.getString("name"));
			p.setProperty("created_utc", obj.getInt("created_utc")+"");
			p.setProperty("id", obj.getString("id"));
			data.put("more", p);
			
			return data;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}


}
