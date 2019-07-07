package de.nuttercode.storm;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import de.nuttercode.util.LongInterval;
import de.nuttercode.util.Range;
import de.nuttercode.util.assurance.Assurance;
import de.nuttercode.util.buffer.DataQueue;
import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.WritableBuffer;

/**
 * represents the DAF
 * 
 * @author Johannes B. Latzel
 *
 */
class DataFile implements Closeable {

	/**
	 * minimum value of an id
	 */
	public final static long MIN_ID = 500;

	/**
	 * maximum value of an id
	 */
	public final static long MAX_ID = Long.MAX_VALUE;

	/**
	 * number of bytes needed to represent the next id in the DAF
	 */
	private final static int NEXT_ID_SIZE = Long.BYTES;

	/**
	 * constant number of indices per index block
	 */
	private final static int INDICES_PER_BLOCK = 100;

	/**
	 * constant size of a index block consists of a pointer to the next index block
	 * (or 0) and {@link #INDICES_PER_BLOCK} number of indices
	 */
	private final static int INDEX_BLOCK_SIZE = Long.BYTES + INDICES_PER_BLOCK * Index.INDEX_LENGTH;

	/**
	 * minimum size of this DAF (according to file format, not
	 * {@link ClientConfiguration})
	 */
	private final static int MINIMUM_SIZE = NEXT_ID_SIZE + INDEX_BLOCK_SIZE;

	/**
	 * indices with this id are interpreted as empty indices
	 */
	private final static long EMPTY_INDEX_ID = 0;

	/**
	 * next available unique identification
	 */
	private long nextId;

	/**
	 * list of start positions of all empty indices
	 */
	private final List<Long> emptyIndices;

	/**
	 * free locations sorted by start address
	 */
	private final TreeSet<LongInterval> freeLocationByStart;

	/**
	 * free locations sorted by length
	 */
	private final TreeSet<LongInterval> freeLocationByLength;

	/**
	 * actual buffer for DAF I/O (direct)
	 */
	private final ByteBuffer byteBuffer;

	/**
	 * dynamic buffer for DAF I/O
	 */
	private final DataQueue dataQueue;

	/**
	 * the DAF
	 */
	private final RandomAccessFile file;

	/**
	 * the channel of the DAF
	 */
	private final FileChannel channel;

	/**
	 * the store configuration
	 */
	private final StoreConfiguration configuration;

	/**
	 * start position of the last read block in the DAF
	 */
	private long lastIndexBlockStart;

	/**
	 * used to log activities or null if logging is disabled
	 */
	private final StoreLog storeLog;

	/**
	 * creates a new (uninitialized) DAF
	 * 
	 * @param configuration
	 * @throws IOException
	 */
	public DataFile(StoreConfiguration configuration, StoreLog storeLog) throws IOException {
		this.configuration = configuration;
		this.storeLog = storeLog;
		boolean isNewFile = false;
		File dataFile = configuration.getDataFile();
		if (!dataFile.exists()) {
			if (!dataFile.createNewFile()) {
				throw new IOException("can not create new file");
			}
			isNewFile = true;
		}
		emptyIndices = new ArrayList<>();
		freeLocationByStart = new TreeSet<LongInterval>((l, r) -> {
			return Long.compare(l.getBegin(), r.getBegin());
		});
		freeLocationByLength = new TreeSet<LongInterval>((l, r) -> {
			return Long.compare(l.getLength(), r.getLength());
		});
		file = new RandomAccessFile(dataFile, "rw");
		channel = file.getChannel();
		byteBuffer = ByteBuffer.allocateDirect(configuration.getByteBufferSize());
		dataQueue = new DataQueue(configuration.getByteBufferSize());
		if (isNewFile) {
			file.setLength(Math.max(MINIMUM_SIZE, configuration.getMinimumDataFileSize()));
			position(0);
			dataQueue.putLong(configuration.getStartID());
			dataQueue.putBytes(new byte[INDEX_BLOCK_SIZE]);
			writeBytes();
		}
	}

	/**
	 * writes all available bytes of the {@link #dataQueue} into the
	 * {@link #channel} at the current position
	 * 
	 * @throws IOException
	 */
	private void writeBytes() throws IOException {
		while (dataQueue.hasAvailable()) {
			byteBuffer.rewind();
			dataQueue.transferDataInto(byteBuffer);
			byteBuffer.flip();
			while (byteBuffer.hasRemaining())
				channel.write(byteBuffer);
		}
		channel.force(true);
	}

	/**
	 * reads at least count bytes from the current position of the {@link #channel}
	 * and puts them into the {@link #dataQueue}
	 * 
	 * @param count
	 * @throws IOException
	 * @throws IllegalArgumentException if count < 1 or count >
	 *                                  {@link Integer#MAX_VALUE}
	 */
	private void readBytes(long count) throws IOException {
		Assurance.assureBoundaries(count, 1, Integer.MAX_VALUE);
		if (dataQueue.available() >= count)
			return;
		long remaining = count;
		int current;
		while (remaining > 0) {
			byteBuffer.rewind();
			current = channel.read(byteBuffer);
			if (current == -1)
				throw new IOException("not enough bytes in file");
			remaining -= current;
			byteBuffer.flip();
			dataQueue.putByteBuffer(byteBuffer);
		}
		dataQueue.retain((int) count);
	}

	/**
	 * removes the location from the set of free locations
	 * 
	 * @param location
	 */
	private void removeFree(LongInterval location) {
		if (storeLog != null)
			storeLog.log("marking " + location + " as used");
		freeLocationByLength.remove(location);
		freeLocationByStart.remove(location);
	}

	/**
	 * adds the location to the set of free locations
	 * 
	 * @param location
	 */
	private void addFree(LongInterval location) {
		if (storeLog != null)
			storeLog.log("marking " + location + " as free");
		freeLocationByLength.add(location);
		freeLocationByStart.add(location);
	}

	/**
	 * marks the location in the DAF as reserved. already reserved locations can not
	 * be reserved again, until they are free. partly reserved locations can not be
	 * reserved completely, until the already reserved parts are free.
	 */
	private void reserve(LongInterval location) {
		if (storeLog != null)
			storeLog.log("reserving " + location);
		if (freeLocationByStart.isEmpty())
			throw new IllegalStateException("no free locations left");
		LongInterval free = freeLocationByStart.floor(location);
		if (free == null)
			throw new IllegalArgumentException("no free element containing " + location + " available");
		if (!free.contains(location))
			throw new IllegalArgumentException(free + " does not contain " + location);
		removeFree(free);
		if (free.getBegin() != location.getBegin())
			addFree(Range.of(free.getBegin(), location.getBegin()));
		if (location.getEnd() != free.getEnd())
			addFree(Range.of(location.getEnd(), free.getEnd()));
	}

	/**
	 * sets {@link #channel} position to position and clears the {@link #dataQueue}
	 * 
	 * @param position
	 * @throws IOException
	 */
	private void position(long position) throws IOException {
		channel.position(position);
		dataQueue.clear();
	}

	/**
	 * creates a new location and saves it as free in the DAF of at least dataLength
	 * bytes length. this may increase the DAF size.
	 * 
	 * @param dataLength
	 * @throws IOException
	 */
	private void createFree(long dataLength) throws IOException {
		long length = Math.max(dataLength, configuration.getDataFileIncrease());
		long begin = file.length();
		LongInterval free = Range.of(begin, begin + length);
		if (storeLog != null)
			storeLog.log("creating new free location " + free);
		file.setLength(free.getEnd());
		addFree(free);
	}

	/**
	 * increases {@link #nextId} and saves it at position 0 in the DAF.
	 * 
	 * @return the next available id
	 * @throws IOException
	 */
	private long getId() throws IOException {
		long id = nextId++;
		position(0);
		dataQueue.putLong(nextId);
		writeBytes();
		return Assurance.assureBoundaries(id, MIN_ID, MAX_ID);
	}

	/**
	 * creates a new index block, marks the new indices as free, and links the new
	 * block to the previously created block. the first block at position
	 * {@link #NEXT_ID_SIZE} always exists.
	 * 
	 * @throws IOException
	 */
	private void createIndexBlock() throws IOException {
		LongInterval free = getFree(INDEX_BLOCK_SIZE);
		if (storeLog != null)
			storeLog.log("creating index block " + free);
		position(lastIndexBlockStart);
		lastIndexBlockStart = free.getBegin();
		dataQueue.putLong(lastIndexBlockStart);
		writeBytes();
		position(lastIndexBlockStart);
		dataQueue.putBytes(new byte[INDEX_BLOCK_SIZE]);
		writeBytes();
		for (long indexLocation = lastIndexBlockStart + Long.BYTES; indexLocation < free
				.getEnd(); indexLocation += Index.INDEX_LENGTH) {
			emptyIndices.add(indexLocation);
		}
	}

	/**
	 * finds or creates a data location with a length of dataLength bytes. this may
	 * increase the size of the DAF.
	 * 
	 * @param dataLength
	 * @return a data location with a length of dataLength bytes
	 * @throws IOException
	 */
	private LongInterval getFree(long dataLength) throws IOException {
		if (storeLog != null)
			storeLog.log("retrieving free location of size " + dataLength);
		LongInterval free = freeLocationByLength.ceiling(Range.of(0, dataLength));
		long newEnd;
		if (free == null) {
			createFree(dataLength);
			return getFree(dataLength);
		}
		if (free.getLength() < dataLength) {
			throw new IllegalStateException(
					"free location is not big enough (" + free.getLength() + " < " + dataLength + ")!");
		}
		removeFree(free);
		if (free.getLength() > dataLength) {
			newEnd = free.getBegin() + dataLength;
			addFree(Range.of(newEnd, free.getEnd()));
			free = Range.of(free.getBegin(), newEnd);
		}
		if (free.getLength() < dataLength) {
			throw new IllegalStateException(
					"free location has been cut too short (" + free.getLength() + " < " + dataLength + ")!");
		}
		if (storeLog != null)
			storeLog.log("found/created free location " + free);
		return free;
	}

	/**
	 * removes a free index and returns its location
	 * 
	 * @return index location of an free index
	 * @throws IOException
	 */
	private long getEmptyIndex() throws IOException {
		if (emptyIndices.isEmpty())
			createIndexBlock();
		return emptyIndices.remove(emptyIndices.size() - 1);
	}

	/**
	 * reserves the specified dataLength space in the DAF for the specified id
	 * 
	 * @param storeId
	 * @param dataLength
	 * @return a new index (with the specified id) whose data location has the
	 *         length of dataLength
	 * @throws IOException
	 */
	Index reserveSpace(long storeId, long dataLength) throws IOException {
		if (storeLog != null)
			storeLog.log("reserving space of size " + dataLength + " for id " + storeId);
		Index entry = new Index(storeId, getFree(dataLength), getEmptyIndex());
		position(entry.getIndexBegin());
		dataQueue.putLong(entry.getId());
		dataQueue.putLong(entry.getDataLocation().getBegin());
		dataQueue.putLong(entry.getDataLocation().getEnd());
		writeBytes();
		if (storeLog != null)
			storeLog.log("created " + entry);
		return entry;
	}

	/**
	 * @param dataLength
	 * @return a new index (with a new id) whose data location has the length of
	 *         dataLength
	 * @throws IOException
	 */
	Index reserveSpace(long dataLength) throws IOException {
		return reserveSpace(getId(), dataLength);
	}

	/**
	 * clears the id of the index in the DAF and marks the index as free
	 * 
	 * @param index
	 * @throws IOException
	 */
	void free(Index index) throws IOException {
		if (storeLog != null)
			storeLog.log("freeing " + index);
		addFree(index.getDataLocation());
		position(index.getIndexBegin());
		dataQueue.putLong(EMPTY_INDEX_ID);
		writeBytes();
	}

	/**
	 * writes the data of the buffer into the data location of the index
	 * 
	 * @param index
	 * @param buffer
	 * @throws IOException
	 */
	void writeData(Index index, ReadableBuffer buffer) throws IOException {
		LongInterval location = index.getDataLocation();
		position(location.getBegin());
		dataQueue.putBuffer(buffer);
		writeBytes();
	}

	/**
	 * puts the data of the index at its data location into the buffer
	 * 
	 * @param index
	 * @param buffer
	 * @throws IOException
	 */
	void readData(Index index, WritableBuffer buffer) throws IOException {
		LongInterval location = index.getDataLocation();
		position(location.getBegin());
		readBytes(location.getLength());
		buffer.putBuffer(dataQueue);
	}

	/**
	 * initializes {@link #nextId}, {@link #freeLocationByLength}, and
	 * {@link #freeLocationByStart}. all free indices will be saved, all reserved
	 * indices will be returned and their data location will be reserved.
	 * 
	 * @return all used Indices
	 * @throws IOException
	 */
	Collection<Index> initialize() throws IOException {

		// variables
		Collection<Index> collection = new ArrayList<>();
		long blockStart = NEXT_ID_SIZE;
		lastIndexBlockStart = blockStart;
		long id, dataStart, dataEnd, indexLocation, blockEnd;
		Index current;

		// handle nextId
		position(0);
		readBytes(NEXT_ID_SIZE);
		nextId = dataQueue.getLong();
		addFree(Range.of(NEXT_ID_SIZE, channel.size()));

		// load indices
		do {
			position(blockStart);
			blockEnd = blockStart + INDEX_BLOCK_SIZE;
			reserve(Range.of(blockStart, blockStart + INDEX_BLOCK_SIZE));
			indexLocation = blockStart + Long.BYTES;
			readBytes(INDEX_BLOCK_SIZE);
			blockStart = dataQueue.getLong();
			if (blockStart != 0)
				lastIndexBlockStart = blockStart;
			while (indexLocation < blockEnd) {
				id = dataQueue.getLong();
				dataStart = dataQueue.getLong();
				dataEnd = dataQueue.getLong();
				if (id == 0) {
					emptyIndices.add(indexLocation);
				} else {
					current = new Index(id, Range.of(dataStart, dataEnd), indexLocation);
					if (storeLog != null)
						storeLog.log("loading " + current);
					reserve(current.getDataLocation());
					collection.add(current);
				}
				indexLocation += Index.INDEX_LENGTH;
			}
		} while (blockStart != 0);

		// return indices
		return collection;
	}

	@Override
	public void close() throws IOException {
		if (storeLog != null)
			storeLog.log("closing datafile");
		channel.force(true);
		channel.close();
		file.close();
	}

}
