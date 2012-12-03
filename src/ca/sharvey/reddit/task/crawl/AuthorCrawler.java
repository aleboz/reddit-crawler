package ca.sharvey.reddit.task.crawl;

import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.Type;

public class AuthorCrawler extends Task {

	public AuthorCrawler() {
		super(Type.CRAWL);
	}
	
	private static final long serialVersionUID = 6736970924056772824L;

	@Override
	public Result execute() {
		return null;
	}

}
