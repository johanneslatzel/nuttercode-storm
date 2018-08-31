package de.nuttercode.store;

import java.io.IOException;

import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.WriteableBuffer;

/**
 * 
 * {@link de.nuttercode.store.Store} implementation for {@link java.lang.String}
 * 
 * @author Johannes B. Latzel
 *
 */
public class StringStore extends Store<String> {

	public StringStore(StoreConfiguration storeConfiguration) throws IOException {
		super(storeConfiguration);
	}

	@Override
	protected void transfer(String value, WriteableBuffer buffer) {
		buffer.putString(value);
	}

	@Override
	protected String restore(ReadableBuffer buffer) {
		return buffer.getString();
	}

}
