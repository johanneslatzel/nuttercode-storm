package de.nuttercode.storm.core;

import de.nuttercode.util.buffer.DynamicBuffer;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import de.nuttercode.storm.Store;
import de.nuttercode.util.LongInterval;

/**
 * a {@link util.buffer.DynamicBuffer} extension. can save and restore
 * {@link StoreItemDescription}s
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreBuffer extends DynamicBuffer {

	/**
	 * size of a storeItemDescription in bytes
	 */
	public static final int BINARY_SIZE = Long.BYTES * 8;

	/**
	 * calculates CRC-32 values
	 */
	private CRC32 crcCalculator;

	/**
	 * buffers values from which a CRC value will be calculated at a later point
	 */
	private ByteBuffer crcBuffer;

	/**
	 * called by {@link Store}. don't manually create {@link StoreBuffer}s.
	 */
	public StoreBuffer() {
		super(0, true);
		crcCalculator = new CRC32();
		crcBuffer = ByteBuffer.allocate(Long.BYTES * 7);
	}

	/**
	 * @param l save l in this buffer and in the {@link #crcBuffer}
	 */
	private void putLongWithCrc(long l) {
		crcBuffer.putLong(l);
		putLong(l);
	}

	/**
	 * @param i save i in this buffer and in the {@link #crcBuffer}
	 */
	private void putIntWithCrc(int i) {
		crcBuffer.putInt(i);
		putInt(i);
	}

	/**
	 * gets an int from this buffer and saves it in the {@link #crcBuffer}
	 * 
	 * @return an int
	 */
	private int getIntWithCrc() {
		int i = getInt();
		crcBuffer.putInt(i);
		return i;
	}

	/**
	 * gets a long from this buffer and saves it in the {@link #crcBuffer}
	 * 
	 * @return a long
	 */
	private long getLongWithCrc() {
		long l = getLong();
		crcBuffer.putLong(l);
		return l;
	}

	/**
	 * resets the {@link #crcCalculator} and rewinds the {@link #crcBuffer}
	 */
	private void prepareCrcBuffer() {
		crcCalculator.reset();
		crcBuffer.rewind();
	}

	/**
	 * flips the {@link #crcBuffer}, updates the {@link #crcCalculator} with the
	 * {@link #crcBuffer}, and returns the value of the {@link #crcBuffer}
	 * 
	 * @return value of the {@link #crcBuffer}
	 */
	private int getCrcValue() {
		crcBuffer.flip();
		crcCalculator.update(crcBuffer);
		return (int) crcCalculator.getValue();
	}

	/**
	 * puts the storeItemDescription in this buffer
	 * 
	 * @param storeItemDescription
	 */
	public void putStoreItemDescription(StoreItemDescription storeItemDescription) {
		prepareCrcBuffer();
		putLongWithCrc(storeItemDescription.getStoreID());
		putLongWithCrc(storeItemDescription.getStoreLocation().getBegin());
		putLongWithCrc(storeItemDescription.getStoreLocation().getEnd());
		// empty space for future flags and expansions
		for (int a = 0; a < 4; a++)
			putLongWithCrc(0);
		putIntWithCrc(0);
		// end
		putInt(getCrcValue());
	}

	/**
	 * @param index
	 * @return {@link StoreItemDescription} given by this buffers content and the
	 *         given index or null if begin and end of the description are 0
	 */
	public StoreItemDescription getStoreItemDescription(long index) {
		prepareCrcBuffer();
		long storeID = getLongWithCrc();
		long begin = getLongWithCrc();
		long end = getLongWithCrc();
		if (begin == 0 && end == 0) {
			return null;
		}
		// empty space for future flags and expansions
		for (int a = 0; a < 4; a++)
			getLongWithCrc();
		getIntWithCrc();
		// end
		int readCrc = getInt();
		if (getCrcValue() != readCrc)
			throw new IllegalStateException("crc does not match at index " + index);
		return new StoreItemDescription(new LongInterval(begin, end), storeID, index);
	}

}
