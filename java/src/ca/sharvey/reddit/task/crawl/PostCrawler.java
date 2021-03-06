package ca.sharvey.reddit.task.crawl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.Properties;
import ca.sharvey.reddit.task.Type;

public class PostCrawler extends Crawler {

	private static final long serialVersionUID = 1585328031012008706L;
	private static final String[] KEYS_STR = { "id", "name", "selftext", "selftext_html", "url", "title", "author", "subreddit", "subreddit_id" };
	private static final String[] KEYS_INT = { "created_utc", "ups", "downs" };

	public PostCrawler(String id) {
		this.id = id;
		setType(Type.CRAWL_POST);
	}
	
	@Override
	URL formURL() throws MalformedURLException {
		return new URL(Main.BASE_SITE+"/by_id/t3_"+id+".json");
	}

	@Override
	HashMap<String, Properties> processData(String content) {
		try {
			HashMap<String, Properties> data = new HashMap<String, Properties>();
			JSONObject obj = new JSONObject(content);
			JSONObject o1 = obj.getJSONObject("data").getJSONArray("children").getJSONObject(0).getJSONObject("data");
			String type = obj.getJSONObject("data").getJSONArray("children").getJSONObject(0).getString("kind");
			
			Properties properties = new Properties();
			
			for (String k : KEYS_STR) {
				properties.setProperty(k, o1.optString(k));
			}
			
			for (String k : KEYS_INT) {
				properties.setProperty(k, o1.getInt(k)+"");
			}
			
			properties.setProperty("kind", type);
			
			data.put(properties.getProperty("name"), properties);
			return data;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

}
