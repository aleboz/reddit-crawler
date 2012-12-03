package ca.sharvey.reddit.task;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

public class Result implements Serializable {

	private static final long serialVersionUID = -4933647674235696300L;
	private Type type = Type.CRAWL;
	private HashMap<String, Properties> data;
	
	public Result(Type type, HashMap<String, Properties> data) {
		this.type = type;
		this.data = data;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public HashMap<String, Properties> getData() {
		return data;
	}

	public void setData(HashMap<String, Properties> data) {
		this.data = data;
	}
}
