package de.nuttercode.storm;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import de.nuttercode.util.buffer.WritableBuffer;
import de.nuttercode.util.LongInterval;
import de.nuttercode.storm.core.StoreBuffer;
import de.nuttercode.storm.core.StoreCacheEntry;
import de.nuttercode.storm.core.StoreCacheEntryDescription;
import de.nuttercode.storm.core.StoreFileManager;
import de.nuttercode.storm.core.StoreLocationManager;
import de.nuttercode.util.assurance.Assurance;
import de.nuttercode.util.assurance.NotNull;
import de.nuttercode.util.buffer.BufferMode;
import de.nuttercode.util.buffer.ObjectTransformer;
import de.nuttercode.util.buffer.ReadableBuffer;

public class Store<T> implements Closeable {

	/**
	 * maps {@link StoreItem} ids to {@link StoreCacheEntry}s
	 */
	private final Map<Long, StoreCacheEntry<T>> itemMap;

	private boolean isClosed;
	private final StoreLocationManager storeLocationManager;
	private final StoreFileManager storeFileManager;
	private final StoreBuffer storeBuffer;
	private final ReadableBuffer readableStoreBufferWrapper;
	private final WritableBuffer writableStoreBufferWrapper;
	private final ObjectTransformer<T> objectTransformer;

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
		itemMap = new HashMap<>();
		initialize();
	}

	private void initialize() throws IOException {

		// get data
		Set<StoreCacheEntryDescription> initialStoreItemDescriptionSet;
		initialStoreItemDescriptionSet = storeFileManager.initialize(storeBuffer);

		// initialize components
		storeLocationManager.initialize(initialStoreItemDescriptionSet);
		for (StoreCacheEntryDescription storeItemDescription : initialStoreItemDescriptionSet)
			itemMap.put(storeItemDescription.getStoreID(), new StoreCacheEntry<>(storeItemDescription));

	}

	private void assureOpen() {
		if (isClosed())
			throw new IllegalStateException("the store is closed");
	}

	private void saveDescription(StoreCacheEntryDescription description) throws IOException {
		storeBuffer.putStoreItemDescription(description);
		storeBuffer.setMode(BufferMode.Read);
		storeFileManager.writeDescription(description.getIndex(), storeBuffer);
		storeBuffer.setMode(BufferMode.Write);
	}

	private void clearDescription(long index) throws IOException {
		storeFileManager.clearDescription(index);
		storeFileManager.addEmptyIndex(index);
	}

	private void setContent(long storeID, T content) {
		itemMap.get(storeID).setContent(content);
	}

	private void setContent(long storeID, StoreCacheEntry<T> item) {
		itemMap.put(storeID, item);
	}

	private void cache(long storeID) throws IOException {
		if (!contains(storeID))
			throw new NoSuchElementException();
		storeFileManager.readData(getStoreLocation(storeID), storeBuffer);
		storeBuffer.setMode(BufferMode.Read);
		setContent(storeID, objectTransformer.getFrom(readableStoreBufferWrapper));
		storeBuffer.setMode(BufferMode.Write);
	}

	private LongInterval getStoreLocation(long storeID) {
		return itemMap.get(storeID).getDescription().getStoreLocation();
	}

	private long getStoreIndex(long storeID) {
		return itemMap.get(storeID).getDescription().getIndex();
	}

	public final void clearCache() {
		for (StoreCacheEntry<T> item : itemMap.values())
			item.setContent(null);
	}

	public StoreQuery<T> query() {
		return new StoreQuery<>(this, Collections.unmodifiableSet(itemMap.keySet()));
	}

	public final boolean contains(long storeID) {
		return itemMap.containsKey(storeID);
	}

	public final StoreItem<T> update(long storeID, T content) throws IOException {
		assureOpen();
		objectTransformer.putInto(content, writableStoreBufferWrapper);
		storeBuffer.setMode(BufferMode.Read);
		LongInterval storeLocation = storeLocationManager.getFreeLocation(storeBuffer.transferableData());
		storeFileManager.writeData(storeLocation, storeBuffer);
		storeBuffer.setMode(BufferMode.Write);
		StoreCacheEntryDescription storeItemDescription = new StoreCacheEntryDescription(storeLocation, storeID,
				getStoreIndex(storeID));
		storeLocationManager.addFreeLocation(getStoreLocation(storeID));
		itemMap.remove(storeID);
		saveDescription(storeItemDescription);
		StoreCacheEntry<T> newItem = new StoreCacheEntry<>(storeItemDescription, content);
		setContent(storeID, newItem);
		return newItem.createStoreItem();
	}

	public final StoreItem<T> get(long storeID) throws IOException {
		assureOpen();
		cache(storeID);
		return itemMap.get(storeID).createStoreItem();
	}

	public final StoreItem<T> store(T content) throws IOException {
		assureOpen();
		StoreCacheEntryDescription storeItemDescription;
		LongInterval storeLocation;
		objectTransformer.putInto(content, writableStoreBufferWrapper);
		storeBuffer.setMode(BufferMode.Read);
		storeLocation = storeLocationManager.getFreeLocation(storeBuffer.transferableData());
		storeFileManager.writeData(storeLocation, storeBuffer);
		storeBuffer.setMode(BufferMode.Write);
		storeItemDescription = storeFileManager.createNewStoreCacheEntryDescription(storeLocation);
		saveDescription(storeItemDescription);
		StoreCacheEntry<T> item = new StoreCacheEntry<>(storeItemDescription, content);
		setContent(storeItemDescription.getStoreID(), item);
		return item.createStoreItem();
	}

	public final void delete(long storeID) throws IOException {
		assureOpen();
		if (!contains(storeID))
			throw new NoSuchElementException();
		LongInterval storeLocation = getStoreLocation(storeID);
		long index = getStoreIndex(storeID);
		clearDescription(index);
		itemMap.remove(storeID);
		storeLocationManager.addFreeLocation(storeLocation);
	}

	public final boolean isClosed() {
		return isClosed;
	}

	@Override
	public final void close() throws IOException {
		if (isClosed())
			return;
		isClosed = true;
		storeFileManager.close();
	}

	public final long getTotalSpace() {
		return storeFileManager.getTotalSpace();
	}

	public final long getFreeSpace() {
		return storeLocationManager.getFreeSpace();
	}

	public final long getUsedSpace() {
		return getTotalSpace() - getFreeSpace();
	}

	public final double getFreeLocationFractionRate() {
		int count = storeLocationManager.getFreeLocationCount();
		if (count == 0)
			return Double.MAX_VALUE;
		return getFreeSpace() / count;
	}

	public final void organize() throws IOException {
		storeFileManager.trimDescriptionFileSize();
		storeLocationManager.mergeFreeLocations();
		storeLocationManager.trimDataFile();
	}

}
