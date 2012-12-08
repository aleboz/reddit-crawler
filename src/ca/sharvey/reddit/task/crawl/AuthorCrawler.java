package ca.sharvey.reddit.task.crawl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.Properties;
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
			JSONObject obj = new JSONObject(content);
			JSONObject d = obj.getJSONObject("data");
			Properties p = new Properties();
			p.setProperty("name", d.getString("name"));
			p.setProperty("created_utc", d.getInt("created_utc")+"");
			p.setProperty("id", d.getString("id"));
			p.setProperty("kind", obj.getString("kind"));
			data.put(d.getString("id"), p);
			
			return data;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}


}
