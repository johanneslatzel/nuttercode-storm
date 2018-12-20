package de.nuttercode.storm;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import de.nuttercode.util.buffer.WritableBuffer;
import de.nuttercode.util.cache.Cache;
import de.nuttercode.util.cache.WeakCache;
import de.nuttercode.storm.core.StoreBuffer;
import de.nuttercode.storm.core.StoreItemDescription;
import de.nuttercode.storm.core.StoreFileManager;
import de.nuttercode.storm.core.StoreLocationManager;
import de.nuttercode.util.LongInterval;
import de.nuttercode.util.assurance.Assurance;
import de.nuttercode.util.assurance.NotNull;
import de.nuttercode.util.buffer.BufferMode;
import de.nuttercode.util.buffer.transformer.ObjectTransformer;
import de.nuttercode.util.buffer.ReadableBuffer;

/**
 * @author Johannes B. Latzel
 *
 * @param <T> type of objects which will be stored in this store
 */
public class Store<T> implements Closeable {

	private boolean isClosed;
	private final StoreLocationManager storeLocationManager;
	private final StoreFileManager storeFileManager;
	private final StoreBuffer storeBuffer;
	private final ReadableBuffer readableStoreBufferWrapper;
	private final WritableBuffer writableStoreBufferWrapper;
	private final ObjectTransformer<T> objectTransformer;
	private final Cache<Long, T> itemCache;
	private final Map<Long, StoreItemDescription> descriptionMap;

	public Store(@NotNull StoreConfiguration storeConfiguration, @NotNull ObjectTransformer<T> objectTransformer)
			throws IOException {
		Assurance.assureNotNull(storeConfiguration);
		Assurance.assureNotNull(objectTransformer);
		this.objectTransformer = objectTransformer;
		isClosed = false;
		storeFileManager = new StoreFileManager(storeConfiguration);
		storeLocationManager = new StoreLocationManager(storeFileManager, storeConfiguration);
		storeBuffer = new StoreBuffer();
		readableStoreBufferWrapper = storeBuffer.readableView();
		writableStoreBufferWrapper = storeBuffer.writableView();
		itemCache = new WeakCache<>();
		descriptionMap = new HashMap<>();
		initialize();
	}

	private void initialize() throws IOException {

		// get data
		Set<StoreItemDescription> initialStoreItemDescriptionSet;
		initialStoreItemDescriptionSet = storeFileManager.initialize(storeBuffer);

		// initialize components
		storeLocationManager.initialize(initialStoreItemDescriptionSet);
		for (StoreItemDescription storeItemDescription : initialStoreItemDescriptionSet)
			descriptionMap.put(storeItemDescription.getStoreID(), storeItemDescription);

	}

	private void assureOpen() {
		if (isClosed)
			throw new IllegalStateException("the store is closed");
	}

	private void saveDescription(StoreItemDescription description) throws IOException {
		storeBuffer.setMode(BufferMode.Write);
		storeBuffer.putStoreItemDescription(description);
		storeBuffer.setMode(BufferMode.Read);
		storeFileManager.writeDescription(description.getIndex(), storeBuffer);
	}

	private void clearDescription(long index) throws IOException {
		storeFileManager.clearDescription(index);
		storeFileManager.addEmptyIndex(index);
	}

	private void setContent(long storeID, T content) {
		itemCache.cache(storeID, content);
	}

	private void cache(long storeID) throws IOException {
		if (!contains(storeID))
			throw new NoSuchElementException();
		if (itemCache.contains(storeID))
			return;
		storeBuffer.setMode(BufferMode.Write);
		storeFileManager.readData(getStoreLocation(storeID), storeBuffer);
		storeBuffer.setMode(BufferMode.Read);
		setContent(storeID, objectTransformer.getFrom(readableStoreBufferWrapper));
	}

	private LongInterval getStoreLocation(long storeID) {
		return descriptionMap.get(storeID).getStoreLocation();
	}

	private long getStoreIndex(long storeID) {
		return descriptionMap.get(storeID).getIndex();
	}

	public StoreQuery<T> query() {
		return new StoreQuery<>(this, Collections.unmodifiableSet(descriptionMap.keySet()));
	}

	public final boolean contains(long storeID) {
		return descriptionMap.containsKey(storeID);
	}

	public final StoreItem<T> update(long storeID, T content) throws IOException {
		assureOpen();
		if (!contains(storeID))
			throw new NoSuchElementException();
		storeBuffer.setMode(BufferMode.Write);
		objectTransformer.putInto(content, writableStoreBufferWrapper);
		storeBuffer.setMode(BufferMode.Read);
		LongInterval storeLocation = storeLocationManager.getFreeLocation(storeBuffer.transferableData());
		storeFileManager.writeData(storeLocation, storeBuffer);
		StoreItemDescription storeItemDescription = new StoreItemDescription(storeLocation, storeID,
				getStoreIndex(storeID));
		storeLocationManager.addFreeLocation(getStoreLocation(storeID));
		descriptionMap.put(storeID, storeItemDescription);
		saveDescription(storeItemDescription);
		setContent(storeID, content);
		return new StoreItem<>(storeID, content);
	}

	public final StoreItem<T> get(long storeID) throws IOException {
		assureOpen();
		cache(storeID);
		return new StoreItem<>(storeID, itemCache.get(storeID));
	}

	public final StoreItem<T> store(T content) throws IOException {
		assureOpen();
		StoreItemDescription storeItemDescription;
		LongInterval storeLocation;
		storeBuffer.setMode(BufferMode.Write);
		objectTransformer.putInto(content, writableStoreBufferWrapper);
		storeBuffer.setMode(BufferMode.Read);
		storeLocation = storeLocationManager.getFreeLocation(storeBuffer.transferableData());
		storeFileManager.writeData(storeLocation, storeBuffer);
		storeItemDescription = storeFileManager.createNewStoreCacheEntryDescription(storeLocation);
		descriptionMap.put(storeItemDescription.getStoreID(), storeItemDescription);
		saveDescription(storeItemDescription);
		setContent(storeItemDescription.getStoreID(), content);
		return new StoreItem<>(storeItemDescription.getStoreID(), content);
	}

	/**
	 * 
	 * @param storeID
	 * @throws IOException
	 */
	public final void delete(long storeID) throws IOException {
		assureOpen();
		if (!contains(storeID))
			throw new NoSuchElementException();
		LongInterval storeLocation = getStoreLocation(storeID);
		long index = getStoreIndex(storeID);
		descriptionMap.remove(storeID);
		clearDescription(index);
		if (itemCache.contains(storeID))
			itemCache.remove(storeID);
		storeLocationManager.addFreeLocation(storeLocation);
	}

	@Override
	public final void close() throws IOException {
		if (isClosed)
			return;
		isClosed = true;
		storeFileManager.close();
	}

	/**
	 * @return total size of DAF in bytes
	 */
	public final long getTotalSpace() {
		return storeFileManager.getTotalSpace();
	}

	/**
	 * @return unallocated space in DAF in bytes
	 */
	public final long getFreeSpace() {
		return storeLocationManager.getFreeSpace();
	}

	/**
	 * @return difference of {@link #getTotalSpace()} - {@link #getFreeSpace()}
	 */
	public final long getUsedSpace() {
		return getTotalSpace() - getFreeSpace();
	}

	/**
	 * rate of free bytes over free locations. measure of fragmentation.
	 * 
	 * @return 0 if no free locations are left or the rate of
	 *         {@link #getFreeSpace()} over the number of free locations
	 */
	public final double getFreeLocationFractionRate() {
		int count = storeLocationManager.getFreeLocationCount();
		if (count == 0)
			return 0;
		return getFreeSpace() / count;
	}

	/**
	 * tries to trim the DEF and DAF and to merge free locations
	 * 
	 * @throws IOException when {@link StoreFileManager#trimDescriptionFileSize()},
	 *                     {@link StoreLocationManager#mergeFreeLocations()}, or
	 *                     {@link StoreLocationManager#trimDataFile()} does
	 */
	public final void organize() throws IOException {
		storeFileManager.trimDescriptionFileSize();
		storeLocationManager.mergeFreeLocations();
		storeLocationManager.trimDataFile();
	}

	/**
	 * @return unmodifiable set of ids of every object stored
	 */
	public final Set<Long> getIds() {
		return Collections.unmodifiableSet(descriptionMap.keySet());
	}

}
