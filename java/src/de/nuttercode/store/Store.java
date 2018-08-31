package de.nuttercode.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Set;

import de.nuttercode.store.core.StoreBuffer;
import de.nuttercode.store.core.StoreCacheEntry;
import de.nuttercode.store.core.StoreCacheEntryDescription;
import de.nuttercode.store.core.StoreFileManager;
import de.nuttercode.store.core.StoreItemManager;
import de.nuttercode.store.core.StoreLocation;
import de.nuttercode.store.core.StoreLocationManager;
import de.nuttercode.util.buffer.WriteableBuffer;
import de.nuttercode.util.buffer.WriteableBufferWrapper;
import de.nuttercode.util.buffer.BufferMode;
import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.ReadableBufferWrapper;

public abstract class Store<T> implements Closeable {

	private boolean isClosed;
	private final StoreLocationManager storeLocationManager;
	private final StoreFileManager storeFileManager;
	private final StoreItemManager<T> storeItemManager;
	private final StoreBuffer storeBuffer;
	private final ReadableBufferWrapper readableStoreBufferWrapper;
	private final WriteableBufferWrapper writeableStoreBufferWrapper;

	public Store(StoreConfiguration storeConfiguration) throws IOException {
		isClosed = false;
		storeFileManager = new StoreFileManager(storeConfiguration);
		storeLocationManager = new StoreLocationManager(storeFileManager, storeConfiguration);
		storeItemManager = new StoreItemManager<>();
		storeBuffer = new StoreBuffer();
		readableStoreBufferWrapper = new ReadableBufferWrapper(storeBuffer);
		writeableStoreBufferWrapper = new WriteableBufferWrapper(storeBuffer);
		initialize();
	}

	private void initialize() throws IOException {

		// get data
		Set<StoreCacheEntryDescription> initialStoreItemDescriptionSet;
		initialStoreItemDescriptionSet = storeFileManager.initialize(storeBuffer);

		// initialize components
		storeLocationManager.initialize(initialStoreItemDescriptionSet);
		for (StoreCacheEntryDescription storeItemDescription : initialStoreItemDescriptionSet) {
			storeItemManager.newItem(storeItemDescription);
		}

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

	private void cache(long storeID) throws IOException {
		if (!contains(storeID))
			throw new NoSuchElementException();
		storeFileManager.readData(storeItemManager.getStoreLocation(storeID), storeBuffer);
		storeBuffer.setMode(BufferMode.Read);
		storeItemManager.set(storeID, restore(readableStoreBufferWrapper));
		storeBuffer.setMode(BufferMode.Write);
	}

	public final void clearCache() {
		storeItemManager.clearCache();
	}

	public StoreQuery<T> query() {
		return new StoreQuery<>(storeItemManager, this);
	}

	public final boolean contains(long storeID) {
		return storeItemManager.contains(storeID);
	}

	public final StoreItem<T> update(long storeID, T content) throws IOException {
		assureOpen();
		transfer(content, writeableStoreBufferWrapper);
		storeBuffer.setMode(BufferMode.Read);
		StoreLocation storeLocation = storeLocationManager.getFreeLocation(storeBuffer.transferableData());
		storeFileManager.writeData(storeLocation, storeBuffer);
		storeBuffer.setMode(BufferMode.Write);
		StoreCacheEntryDescription storeItemDescription = new StoreCacheEntryDescription(storeLocation, storeID,
				storeItemManager.getStoreIndex(storeID));
		storeLocationManager.addFreeLocation(storeItemManager.getStoreLocation(storeID));
		storeItemManager.remove(storeID);
		saveDescription(storeItemDescription);
		StoreCacheEntry<T> newItem = new StoreCacheEntry<>(storeItemDescription, content);
		storeItemManager.set(storeID, newItem);
		return newItem.createStoreItem();
	}

	public final StoreItem<T> get(long storeID) throws IOException {
		assureOpen();
		cache(storeID);
		return storeItemManager.get(storeID);
	}

	public final StoreItem<T> store(T content) throws IOException {
		assureOpen();
		StoreCacheEntryDescription storeItemDescription;
		StoreLocation storeLocation;
		transfer(content, writeableStoreBufferWrapper);
		storeBuffer.setMode(BufferMode.Read);
		storeLocation = storeLocationManager.getFreeLocation(storeBuffer.transferableData());
		storeFileManager.writeData(storeLocation, storeBuffer);
		storeBuffer.setMode(BufferMode.Write);
		storeItemDescription = storeFileManager.createNewStoreCacheEntryDescription(storeLocation);
		saveDescription(storeItemDescription);
		StoreCacheEntry<T> item = new StoreCacheEntry<>(storeItemDescription, content);
		storeItemManager.set(storeItemDescription.getStoreID(), item);
		return item.createStoreItem();
	}

	public final void delete(long storeID) throws IOException {
		assureOpen();
		if (!storeItemManager.contains(storeID))
			throw new NoSuchElementException();
		StoreLocation storeLocation = storeItemManager.getStoreLocation(storeID);
		long index = storeItemManager.getStoreIndex(storeID);
		clearDescription(index);
		storeItemManager.remove(storeID);
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
		if( count == 0 ) return Double.MAX_VALUE;
		return getFreeSpace() / count;
	}

	public final void organize() throws IOException {
		storeFileManager.trimDescriptionFileSize();
		storeLocationManager.mergeFreeLocations();
		storeLocationManager.trimDataFile();
	}

	protected abstract void transfer(T value, WriteableBuffer buffer);

	protected abstract T restore(ReadableBuffer buffer);

}
