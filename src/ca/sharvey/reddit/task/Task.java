package ca.sharvey.reddit.task;

import java.io.Serializable;

public abstract class Task implements Serializable, Comparable<Object> {

	private static final long serialVersionUID = -2900557920133756338L;
	private Type type = Type.CRAWL;
	private static long CLASS_UID = 1;
	private long uid = CLASS_UID++;
	protected String id;
	
	public Task(Type type) {
		this.type = type;
	}

	public Type getType() {
		return type;
	}
	
	protected void setType(Type type) {
		this.type = type;
	}
	
	public abstract Result execute();
	
	public static void sleep(int millis) {
		try { Thread.sleep(millis); } catch (InterruptedException e) {}
	}
	
	public long getUID() {
		return uid;
	}
	
	public String getID() {
		return id;
	}
	
	public int compareTo(Object o) {
		if (!(o instanceof Task)) return 1;
		Task t = (Task)o;
		return kindValue()-t.kindValue();
	}
	
	private int kindValue() {
		if (type == Type.CRAWL_AUTHOR) return 0;
		if (type == Type.CRAWL_POST) return 2;
		if (type == Type.CRAWL_COMMENT) return 1;
		return 3;
	}
}
