package de.nuttercode.storm;

import java.io.IOException;

import de.nuttercode.util.Assurance;
import de.nuttercode.util.buffer.Bufferer;
import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.WritableBuffer;

/**
 * generic implementation of a {@link Store}. Delegates the abstract methods of
 * {@link Store} to a {@link Bufferer}
 * 
 * @author Johannes B. Latzel
 *
 * @param <T>
 *            some type
 */
public class GenericStore<T> extends Store<T> {

	private final Bufferer<T> bufferer;

	/**
	 * 
	 * @param storeConfiguration
	 * @param bufferer
	 * @throws IOException
	 *             when {@link Store#Store(StoreConfiguration)} does
	 * @throws IllegalArgumenException
	 *             when {@link Store#Store(StoreConfiguration)} does or if bufferer
	 *             is null
	 */
	public GenericStore(StoreConfiguration storeConfiguration, Bufferer<T> bufferer) throws IOException {
		super(storeConfiguration);
		Assurance.assureNotNull(bufferer);
		this.bufferer = bufferer;
	}

	@Override
	protected void putInto(T value, WritableBuffer buffer) {
		Assurance.assureNotNull(value);
		bufferer.putInto(value, buffer);
	}

	@Override
	protected T getFrom(ReadableBuffer buffer) {
		return bufferer.getFrom(buffer);
	}

}
