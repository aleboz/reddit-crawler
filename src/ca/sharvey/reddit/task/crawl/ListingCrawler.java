package ca.sharvey.reddit.task.crawl;

import java.util.HashMap;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import ca.sharvey.reddit.task.Type;

public abstract class ListingCrawler extends Crawler {

	private static final long serialVersionUID = 1585328031012008706L;
	String after;
	int count;
	
	public ListingCrawler(String id) {
		this.id = id;
		setType(Type.CRAWL_LISTING);
	}
	
	public ListingCrawler(String id, String after, int count) {
		this.id = id;
		this.after = after;
		this.count = count;
		setType(Type.CRAWL_LISTING);
	}

	@Override
	HashMap<String, Properties> processData(String content) {
		try {
			HashMap<String, Properties> data = new HashMap<String, Properties>();
			JSONObject obj = new JSONObject(content);
			JSONArray array = obj.getJSONObject("data").getJSONArray("children");
			for (int i = 0; i < array.length(); i++) {
				JSONObject val = array.getJSONObject(i);
				JSONObject d = val.getJSONObject("data");
				String id = val.getJSONObject("data").getString("id");
				String kind = val.getString("kind");
				String parent_id = "";
				if (kind.equalsIgnoreCase("t1"))
					parent_id = val.getJSONObject("data").getString("parent_id");
				
				Properties p = new Properties();
				p.setProperty("id", id);
				p.setProperty("name", d.getString("name"));
				p.setProperty("kind", kind);
				p.setProperty("subreddit", d.getString("subreddit"));
				p.setProperty("subreddit_id", d.getString("subreddit_id"));
				p.setProperty("parent_id", parent_id);
				data.put(id, p);
			}
			Properties p = new Properties();
			String after = obj.getJSONObject("data").getString("after");
			p.setProperty("after", after);
			p.setProperty("count", (count+25)+"");
			p.setProperty("id", this.id);
			data.put("more", p);
			
			return data;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

}
