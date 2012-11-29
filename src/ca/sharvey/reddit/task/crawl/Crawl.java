package ca.sharvey.reddit.task.crawl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

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
		String line;
		String result = "";
		try {
			URL url = formURL();
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

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

}
