package de.nuttercode.storm;

import java.io.IOException;

import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.WritableBuffer;

/**
 * {@link de.nuttercode.storm.Store} implementation for int[]
 * 
 * @author Johannes B. Latzel
 *
 */
public class IntArrayStore extends Store<int[]> {

	public IntArrayStore(StoreConfiguration storeConfiguration) throws IOException {
		super(storeConfiguration);
	}

	@Override
	protected void transfer(int[] value, WritableBuffer buffer) {
		buffer.putInt(value.length);
		for (int i : value)
			buffer.putInt(i);
	}

	@Override
	protected int[] restore(ReadableBuffer buffer) {
		int[] intArray = new int[buffer.getInt()];
		for (int a = 0; a < intArray.length; a++)
			intArray[a] = buffer.getInt();
		return intArray;
	}

}
