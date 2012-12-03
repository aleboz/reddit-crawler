package ca.sharvey.reddit.task.crawl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import ca.sharvey.reddit.Main;

public class CommentCrawler extends Crawler {

	private static final long serialVersionUID = 8842559019162126555L;
	private static final String[] KEYS_STR = { "id", "name", "body", "body_html", "parent_id", "link_id", "author", "subreddit", "subreddit_id" };
	private static final String[] KEYS_INT = { "created_utc", "ups", "downs" };

	private String id;
	private String start;

	public CommentCrawler(String id) {
		this.id = id;
	}

	public CommentCrawler(String id, String start) {
		this.id = id;
		this.start = start;
	}

	@Override
	URL formURL() throws MalformedURLException {
		if (start == null)
			return new URL(Main.BASE_SITE+"/comments/"+id+".json");
		else
			return new URL(Main.BASE_SITE+"/comments/"+id+"/_/"+start+".json");
	}

	private Properties pullComment(JSONObject jobj, Properties more) {
		try {
			String type = jobj.getString("kind");
			JSONObject data = jobj.getJSONObject("data");
			if (!type.equalsIgnoreCase("more")) {
				Properties properties = new Properties();
				for (String k : KEYS_STR) {
					properties.setProperty(k, data.getString(k));
				}

				for (String k : KEYS_INT) {
					properties.setProperty(k, data.getInt(k)+" ");
				}
				return properties;
			} else {
				more.setProperty(data.getString("id"), data.getString("name"));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void recurseComments(JSONObject jobj, Properties more, HashMap<String, Properties> data) {
		try {
			Properties properties = pullComment(jobj, more);
			if (properties != null) {
				data.put(properties.getProperty("name"), properties);
				if (!jobj.getJSONObject("data").isNull("replies") && !(jobj.getJSONObject("data").get("replies") instanceof String)) {
					JSONObject replies = jobj.getJSONObject("data").getJSONObject("replies");
					JSONArray array = replies.getJSONObject("data").getJSONArray("children");
					for (int i = 0; i < array.length(); i++) {
						recurseComments(array.getJSONObject(i), more, data);
					}
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			System.out.println(jobj);
			e.printStackTrace();
			System.exit(1);
		}

	}

	@Override
	HashMap<String, Properties> processData(String content) {
		try {
			HashMap<String, Properties> data = new HashMap<String, Properties>();
			Properties more = new Properties();
			JSONObject commentRoot = new JSONArray(content).getJSONObject(1);
			JSONArray array = commentRoot.getJSONObject("data").getJSONArray("children");
			for (int i = 0; i < array.length(); i++) {
				recurseComments(array.getJSONObject(i), more, data);
			}

			data.put("more", more);
			return data;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

}
