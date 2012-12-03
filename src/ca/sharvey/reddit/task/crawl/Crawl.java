package ca.sharvey.reddit.task.crawl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;

import ca.sharvey.reddit.Main;
import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.Type;

public abstract class Crawl extends Task {

	public Crawl() {
		super(Type.CRAWL);
	}

	private static final long serialVersionUID = -3079666786550335691L;
	byte[] data;

	abstract URL formURL()  throws MalformedURLException;

	String downloadURL() {
		String line, result = null;
		int code = 200;
		boolean first = true;
		do {
			if (first) {
				first = false;
			} else {
				sleep(1000);
			}
			try {
				result = "";
				URL url = formURL();
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setRequestProperty("User-Agent", Main.USER_AGENT);
				conn.setRequestMethod("GET");
				BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				while ((line = rd.readLine()) != null) {
					result += line;
				}
				rd.close();
				code = conn.getResponseCode();
				if (code == 200) {
					try {
						JSONObject obj = new JSONObject(result);
						if (obj.has("error"))
							code = new JSONObject(result).getInt("error");
					} catch (JSONException e) {}
				}
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} while (code != 200);
		return result;
	}
	/*
	byte[] deflate(byte[] input) {
		byte[] output = new byte[1024];
		Deflater compresser = new Deflater();
		compresser.setInput(input);
		compresser.finish();
		int compressedDataLength = compresser.deflate(output);

		byte[] finalOutput = Arrays.copyOf(output, compressedDataLength);
		return finalOutput;
	}

	byte[] inflate(byte[] input) {
		try {
			Inflater decompresser = new Inflater();
			decompresser.setInput(input, 0, input.length);
			byte[] output = new byte[4096];
			int resultLength = decompresser.inflate(output);
			decompresser.end();

			byte[] finalOutput = Arrays.copyOf(output, resultLength);
			return finalOutput;
		} catch (java.util.zip.DataFormatException ex) {}
		return null;
	}
	 */
	
	abstract HashMap<String, Properties> processData(String content);
	
	@Override
	public Result execute() {
		String content = downloadURL();
		HashMap<String, Properties> data = processData(content);
		return new Result(getType(), data);
	}

}
