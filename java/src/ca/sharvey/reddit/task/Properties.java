package ca.sharvey.reddit.task;


public class Properties extends java.util.Properties implements Comparable<Object> {

	private static final long serialVersionUID = -4419831785099203324L;

	public int compareTo(Object o) {
		if (!(o instanceof Properties)) return 1;
		Properties p = (Properties)o;
		return kindValue()-p.kindValue();
	}
	
	private int kindValue() {
		String kind = getProperty("kind");
		if (kind.equalsIgnoreCase("t2")) return 0;
		if (kind.equalsIgnoreCase("t3")) return 2;
		if (kind.equalsIgnoreCase("t1")) return 1;
		return 3;
	}

}
