package de.nuttercode.storm;

import de.nuttercode.storm.ObjectTransformer;
import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.WritableBuffer;

/**
 * Trivial {@link ObjectTransformer} implementation for {@link String} as a
 * singleton.
 * 
 * @author Johannes B. Latzel
 *
 */
public class StringTransformer implements ObjectTransformer<String> {

	private static StringTransformer instance = null;

	/**
	 * singleton implementation
	 * 
	 * @return the {@link StringTransformer}
	 */
	public static StringTransformer get() {
		synchronized (StringTransformer.class) {
			if (instance == null)
				instance = new StringTransformer();
		}
		return instance;
	}

	private StringTransformer() {
	}

	@Override
	public void putInto(String value, WritableBuffer writableBuffer) {
		writableBuffer.putString(value);
	}

	@Override
	public String getFrom(ReadableBuffer readableBuffer) {
		return readableBuffer.getString();
	}

}
