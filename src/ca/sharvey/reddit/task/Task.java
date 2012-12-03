package ca.sharvey.reddit.task;

import java.io.Serializable;

public abstract class Task implements Serializable {

	private static final long serialVersionUID = -2900557920133756338L;
	private Type type = Type.CRAWL;
	
	public Task(Type type) {
		this.type = type;
	}

	public Type getType() {
		return type;
	}
	
	public abstract Result execute();
	
	public static void sleep(int millis) {
		try { Thread.sleep(millis); } catch (InterruptedException e) {}
	}
}
