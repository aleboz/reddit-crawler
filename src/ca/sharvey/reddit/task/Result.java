package ca.sharvey.reddit.task;

import java.io.Serializable;
import java.util.HashMap;

public class Result implements Serializable, Comparable<Object> {

	private static final long serialVersionUID = -4933647674235696300L;
	private Type type = Type.CRAWL;
	private HashMap<String, Properties> data;
	private Task task;
	
	public Result(Type type, Task task, HashMap<String, Properties> data) {
		this.type = type;
		this.data = data;
		this.task = task;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public HashMap<String, ca.sharvey.reddit.task.Properties> getData() {
		return data;
	}

	public void setData(HashMap<String, Properties> data) {
		this.data = data;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}
	
	@Override
	public int compareTo(Object o) {
		if (!(o instanceof Result)) return 1;
		Result r = (Result)o;
		return kindValue()-r.kindValue();
	}
	
	private int kindValue() {
		Type type = task.getType();
		if (type == Type.CRAWL_AUTHOR) return 0;
		if (type == Type.CRAWL_POST) return 2;
		if (type == Type.CRAWL_COMMENT) return 1;
		return 3;
	}
}
