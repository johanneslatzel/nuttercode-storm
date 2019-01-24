package de.nuttercode.storm;

import de.nuttercode.util.assurance.NotNull;
import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.WritableBuffer;

/**
 * extension for objects to be able to be put into {@link WritableBuffer} and
 * got from a {@link ReadableBuffer}
 * 
 * @author Johannes B. Latzel
 *
 * @param <T>
 *            Type of the object
 */
public interface ObjectTransformer<T> {

	/**
	 * implementations put all of the values relevant data into writableBuffer
	 * 
	 * @param writableBuffer
	 * @param value
	 */
	void putInto(@NotNull T value, @NotNull WritableBuffer writableBuffer);

	/**
	 * implementations read from the readableBuffer to construct a new object
	 * 
	 * @param readableBuffer
	 * @return a new object
	 */
	@NotNull
	T getFrom(@NotNull ReadableBuffer readableBuffer);

}
