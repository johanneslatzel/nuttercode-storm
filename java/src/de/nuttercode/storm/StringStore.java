package de.nuttercode.storm;

import java.io.IOException;

import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.WritableBuffer;

/**
 * 
 * {@link de.nuttercode.storm.Store} implementation for {@link java.lang.String}
 * 
 * @author Johannes B. Latzel
 *
 */
public class StringStore extends Store<String> {

	public StringStore(StoreConfiguration storeConfiguration) throws IOException {
		super(storeConfiguration);
	}

	@Override
	protected void putInto(String value, WritableBuffer buffer) {
		buffer.putString(value);
	}

	@Override
	protected String getFrom(ReadableBuffer buffer) {
		return buffer.getString();
	}

}
