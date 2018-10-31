package de.nuttercode.storm.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import de.nuttercode.util.buffer.WritableBuffer;
import de.nuttercode.util.LongInterval;
import de.nuttercode.util.buffer.BufferMode;
import de.nuttercode.storm.Store;
import de.nuttercode.storm.StoreConfiguration;
import de.nuttercode.storm.StoreItem;
import de.nuttercode.util.Closeable;
import de.nuttercode.util.Initializable;
import de.nuttercode.util.buffer.DynamicBuffer;
import de.nuttercode.util.buffer.ReadableBuffer;

/**
 * manages the files related to a {@link Store}
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreFileManager implements Closeable, Initializable {

	/**
	 * directory in which all files related to the {@link Store} are saved
	 */
	private final Path storeDirectory;

	/**
	 * channel of the description file
	 */
	private final FileChannel descriptionChannel;

	/**
	 * channel of the last id file
	 */
	private final FileChannel lastIDChannel;

	/**
	 * description of the data file
	 */
	private final FileChannel dataChannel;

	/**
	 * true if this manager has been closed
	 */
	private boolean isClosed;

	/**
	 * configuration of the {@link Store}
	 */
	private final StoreConfiguration storeConfiguration;

	/**
	 * buffer for file transactions
	 */
	private final ByteBuffer byteBuffer;

	/**
	 * clear buffer for clearing sections of the description file
	 */
	private final ByteBuffer clearBuffer;

	/**
	 * all available indices for new {@link StoreItem}
	 */
	private final TreeSet<Long> emptyStoreItemDescriptionIndexSet;

	/**
	 * buffer for reading / writing {@link #lastID} to/from {@link #lastIDChannel}
	 */
	private final ByteBuffer lastIDBuffer;

	/**
	 * total of used space by the {@link Store}
	 */
	private long totalSpace;

	/**
	 * true if this manager has been initialized
	 */
	private boolean isInitialized;

	/**
	 * begin id of available ids. all ids within [idBegin, idEnd] are available
	 */
	private long idBegin;

	/**
	 * end id of available ids. all ids within [idBegin, idEnd] are available
	 */
	private long idEnd;

	public StoreFileManager(StoreConfiguration storeConfiguration) throws IOException {
		assert (storeConfiguration != null);
		this.storeDirectory = storeConfiguration.getStoreDirectory();
		this.storeConfiguration = new StoreConfiguration(storeConfiguration);
		File storeDirectoryFile = storeDirectory.toFile();
		if (!storeDirectoryFile.exists()) {
			if (!storeDirectoryFile.mkdirs())
				throw new IOException("can't create directories");
		}
		isClosed = false;
		dataChannel = FileChannel.open(getDataFilePath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
				StandardOpenOption.READ);
		descriptionChannel = FileChannel.open(getDescriptionFilePath(), StandardOpenOption.CREATE,
				StandardOpenOption.WRITE, StandardOpenOption.READ);
		lastIDChannel = FileChannel.open(getLastIDFilePath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
				StandardOpenOption.READ);
		byteBuffer = ByteBuffer.allocateDirect(storeConfiguration.getByteBufferSize());
		clearBuffer = ByteBuffer.allocateDirect(StoreBuffer.BINARY_SIZE);
		emptyStoreItemDescriptionIndexSet = new TreeSet<>();
		lastIDBuffer = ByteBuffer.allocateDirect(Long.BYTES);
		long lastID = readLastID();
		idBegin = lastID;
		idEnd = lastID;
		totalSpace = getDataFilePath().toFile().length();
		isInitialized = false;
	}

	private long fetchID() throws IOException {
		long id = idBegin;
		if (idEnd - idBegin == 0) {
			idEnd = id + storeConfiguration.getIdIncrease();
			writeLastID(idEnd + 1);
		}
		idBegin++;
		return id;
	}

	/**
	 * @return absolute path to the store directory
	 */
	private String getAbsoluteSD() {
		return storeDirectory.toString();
	}

	/**
	 * @return absolute path to the data file
	 */
	private Path getDataFilePath() {
		return Paths.get(getAbsoluteSD(),
				storeConfiguration.getStoreName() + '.' + storeConfiguration.getDataFileSuffix());
	}

	/**
	 * @return absolute path to the description file
	 */
	private Path getDescriptionFilePath() {
		return Paths.get(getAbsoluteSD(),
				storeConfiguration.getStoreName() + '.' + storeConfiguration.getDescriptionFileSuffix());
	}

	/**
	 * @return absolute path to the lastID file
	 */
	private Path getLastIDFilePath() {
		return Paths.get(getAbsoluteSD(),
				storeConfiguration.getStoreName() + '.' + storeConfiguration.getIDFileSuffix());
	}

	/**
	 * writes all bytes from buffer to the fileChannel beginning at begin with the
	 * eof of end
	 * 
	 * @param fileChannel
	 * @param begin
	 * @param end
	 * @param buffer
	 * @throws IOException           when {@link FileChannel#position(long)} or
	 *                               {@link FileChannel#write(ByteBuffer)} does
	 * @throws IllegalStateException if buffer has more data than end - begin bytes
	 */
	private void writeComplete(FileChannel fileChannel, long begin, long end, ReadableBuffer buffer)
			throws IOException {
		fileChannel.position(begin);
		while (buffer.hasTransferableData()) {
			byteBuffer.clear();
			buffer.transferDataInto(byteBuffer);
			byteBuffer.flip();
			if (fileChannel.position() + byteBuffer.remaining() > end)
				throw new IllegalStateException(fileChannel.position() + " + " + byteBuffer.remaining() + " > " + end);
			while (byteBuffer.hasRemaining()) {
				fileChannel.write(byteBuffer);
			}
		}
	}

	/**
	 * writes {@link #lastID} to {@link #lastIDChannel}
	 * 
	 * @throws IOException when {@link FileChannel#write(ByteBuffer)} or
	 *                     {@link FileChannel#position(long)} does
	 */
	private void writeLastID(long lastID) throws IOException {
		lastIDBuffer.clear();
		lastIDBuffer.putLong(lastID);
		lastIDBuffer.flip();
		lastIDChannel.position(0);
		int writtenBytes = 0;
		int currentWrittenBytes;
		while ((currentWrittenBytes = lastIDChannel.write(lastIDBuffer)) != -1 && writtenBytes < Long.BYTES) {
			writtenBytes += currentWrittenBytes;
		}
	}

	/**
	 * @return lastID saved in {@link #lastIDChannel}
	 * @throws IOException when {@link FileChannel#position(long)} or
	 *                     {@link FileChannel#read(ByteBuffer)} does
	 */
	private long readLastID() throws IOException {
		if (getLastIDFilePath().toFile().length() < Long.BYTES)
			return 0;
		lastIDBuffer.clear();
		lastIDChannel.position(0);
		int readBytes = 0;
		int currentReadBytes;
		while ((currentReadBytes = lastIDChannel.read(lastIDBuffer)) != -1 && readBytes < Long.BYTES) {
			readBytes += currentReadBytes;
		}
		lastIDBuffer.flip();
		return lastIDBuffer.getLong();
	}

	/**
	 * writes all data of buffer to the data file
	 * 
	 * @param storeLocation
	 * @param buffer
	 * @throws IOException when
	 *                     {@link #writeComplete(FileChannel, long, long, ReadableBuffer)}
	 *                     does
	 */
	public void writeData(LongInterval storeLocation, ReadableBuffer buffer) throws IOException {
		assert (!isClosed());
		assert (isInitialized());
		writeComplete(dataChannel, storeLocation.getBegin(), storeLocation.getEnd(), buffer);
	}

	/**
	 * writes the buffer to the description file at the given index
	 * 
	 * @param index
	 * @param buffer
	 * @throws IOException when
	 *                     {@link #writeComplete(FileChannel, long, long, ReadableBuffer)}
	 *                     does
	 */
	public void writeDescription(long index, ReadableBuffer buffer) throws IOException {
		assert (!isClosed());
		assert (isInitialized());
		long begin = index * StoreBuffer.BINARY_SIZE;
		writeComplete(descriptionChannel, begin, begin + StoreBuffer.BINARY_SIZE, buffer);
	}

	/**
	 * clears the description of the entry given by the index
	 * 
	 * @param index
	 * @throws IOException when {@link FileChannel#position(long)} or
	 *                     {@link FileChannel#write(ByteBuffer)} does
	 */
	public void clearDescription(long index) throws IOException {
		assert (!isClosed());
		assert (isInitialized());
		clearBuffer.rewind();
		descriptionChannel.position(index * StoreBuffer.BINARY_SIZE);
		while (clearBuffer.hasRemaining()) {
			descriptionChannel.write(clearBuffer);
		}
	}

	/**
	 * reads all data specified by the storeLocation from the data file and puts it
	 * into buffer
	 * 
	 * @param storeLocation
	 * @param buffer
	 * @throws IOException when {@link FileChannel#position(long)} or
	 *                     {@link FileChannel#read(ByteBuffer)} does
	 */
	public void readData(LongInterval storeLocation, WritableBuffer buffer) throws IOException {
		assert (!isClosed());
		assert (isInitialized());
		long end = storeLocation.getEnd();
		dataChannel.position(storeLocation.getBegin());
		while (dataChannel.position() < end) {
			byteBuffer.clear();
			if (dataChannel.position() + byteBuffer.capacity() > end) {
				byteBuffer.limit((int) (end - dataChannel.position()));
			}
			dataChannel.read(byteBuffer);
			byteBuffer.flip();
			buffer.putByteBuffer(byteBuffer);
		}
	}

	/**
	 * @param storeLocation
	 * @return new {@link StoreItemDescription} given by the storeLocation
	 * @throws IOException when {@link #writeLastID()} does
	 */
	public StoreItemDescription createNewStoreCacheEntryDescription(LongInterval storeLocation) throws IOException {
		assert (!isClosed());
		assert (isInitialized());
		long id = fetchID();
		long index;
		if (emptyStoreItemDescriptionIndexSet.isEmpty())
			index = id;
		else
			index = emptyStoreItemDescriptionIndexSet.pollFirst();
		return new StoreItemDescription(storeLocation, id, index);
	}

	/**
	 * initializes this manager
	 * 
	 * @param storeBuffer
	 * @return initial {@link StoreItemDescription}s
	 * @throws IOException when {@link FileChannel#read(ByteBuffer)} does
	 */
	public Set<StoreItemDescription> initialize(StoreBuffer storeBuffer) throws IOException {

		assert (!isClosed());
		assert (!isInitialized());

		Set<StoreItemDescription> storeItemDescriptionSet = new HashSet<>();
		boolean hasMoreData = true;
		long storeItemDescriptionIndex = 0;
		long currentEnd;
		DynamicBuffer temporaryBuffer;
		StoreItemDescription storeItemDescription;
		emptyStoreItemDescriptionIndexSet.clear();
		byteBuffer.clear();

		try {
			temporaryBuffer = new DynamicBuffer(byteBuffer.capacity(), true);
			while (descriptionChannel.read(byteBuffer) != -1 || hasMoreData) {
				hasMoreData = false;
				byteBuffer.flip();
				if (temporaryBuffer.getMode().equals(BufferMode.Read)) {
					storeBuffer.putBuffer(temporaryBuffer);
					temporaryBuffer.setMode(BufferMode.Write);
				}
				storeBuffer.putByteBuffer(byteBuffer);
				storeBuffer.setMode(BufferMode.Read);
				if (storeBuffer.transferableData() >= StoreBuffer.BINARY_SIZE) {
					while (storeBuffer.transferableData() >= StoreBuffer.BINARY_SIZE) {
						storeItemDescription = storeBuffer.getStoreItemDescription(storeItemDescriptionIndex);
						if (storeItemDescription != null) {
							storeItemDescriptionSet.add(storeItemDescription);
							currentEnd = storeItemDescription.getStoreLocation().getEnd();
							if (currentEnd > totalSpace)
								totalSpace = currentEnd;
						} else {
							emptyStoreItemDescriptionIndexSet.add(storeItemDescriptionIndex);
						}
						storeItemDescriptionIndex++;
					}
					if (storeBuffer.transferableData() > 0) {
						temporaryBuffer.putBuffer(storeBuffer);
						temporaryBuffer.setMode(BufferMode.Read);
						hasMoreData = true;
					}
				}
				storeBuffer.setMode(BufferMode.Write);
				byteBuffer.clear();
			}
		} catch (RuntimeException e) {
			throw new RuntimeException(e);
		}

		isInitialized = true;

		return storeItemDescriptionSet;
	}

	@Override
	public void close() throws IOException {
		assert (!isClosed());
		isClosed = true;
		dataChannel.force(true);
		dataChannel.close();
		descriptionChannel.force(true);
		descriptionChannel.close();
		writeLastID(idBegin);
		lastIDChannel.force(true);
		lastIDChannel.close();
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	/**
	 * adds an index to {@link #emptyStoreItemDescriptionIndexSet}
	 * 
	 * @param index
	 */
	public void addEmptyIndex(long index) {
		emptyStoreItemDescriptionIndexSet.add(index);
	}

	/**
	 * @param size
	 * @return new {@link StoreLocation} at the end of the data file specified by
	 *         size
	 */
	public LongInterval createNewStoreLocation(long size) {
		long begin = totalSpace;
		long end = totalSpace + size;
		totalSpace = end;
		return new LongInterval(begin, end);
	}

	/**
	 * @return {@link #totalSpace}
	 */
	public long getTotalSpace() {
		return totalSpace;
	}

	@Override
	public boolean isInitialized() {
		return isInitialized;
	}

	/**
	 * truncates the data file to size
	 * 
	 * @param size
	 * @throws IOException when {@link FileChannel#truncate(long)} does
	 */
	public void setDataFileSize(long size) throws IOException {
		assert (!isClosed());
		assert (isInitialized());
		dataChannel.truncate(size);
		totalSpace = size;
	}

	/**
	 * trims to description file to a minimal size
	 * 
	 * @throws IOException when {@link FileChannel#truncate(long)} does
	 */
	public void trimDescriptionFileSize() throws IOException {
		assert (!isClosed());
		assert (isInitialized());
		if (emptyStoreItemDescriptionIndexSet.isEmpty())
			return;
		long lastIndex = (descriptionChannel.size() / StoreBuffer.BINARY_SIZE) - 1;
		long currentIndex = emptyStoreItemDescriptionIndexSet.last();
		while (currentIndex == lastIndex && currentIndex != -1) {
			lastIndex--;
			emptyStoreItemDescriptionIndexSet.remove(currentIndex);
			if (!emptyStoreItemDescriptionIndexSet.isEmpty())
				currentIndex = emptyStoreItemDescriptionIndexSet.last();
			else
				currentIndex = -1;
		}
		descriptionChannel.truncate((lastIndex + 1) * StoreBuffer.BINARY_SIZE);
	}

}
