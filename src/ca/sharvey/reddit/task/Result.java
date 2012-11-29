package ca.sharvey.reddit.task;

import java.io.Serializable;

public abstract class Result implements Serializable {

	private static final long serialVersionUID = -4933647674235696300L;
	private Type type = Type.CRAWL;
	
	public Result(Type type) {
		this.type = type;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public abstract void process();
}
