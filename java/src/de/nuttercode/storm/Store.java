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
import de.nuttercode.util.LongInterval;
import de.nuttercode.storm.core.StoreBuffer;
import de.nuttercode.storm.core.StoreItemDescription;
import de.nuttercode.storm.core.StoreFileManager;
import de.nuttercode.storm.core.StoreLocationManager;
import de.nuttercode.util.assurance.Assurance;
import de.nuttercode.util.assurance.NotNull;
import de.nuttercode.util.buffer.BufferMode;
import de.nuttercode.util.buffer.transformer.ObjectTransformer;
import de.nuttercode.util.buffer.ReadableBuffer;

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
		if (isClosed())
			throw new IllegalStateException("the store is closed");
	}

	private void saveDescription(StoreItemDescription description) throws IOException {
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
		itemCache.cache(storeID, content);
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
		objectTransformer.putInto(content, writableStoreBufferWrapper);
		storeBuffer.setMode(BufferMode.Read);
		LongInterval storeLocation = storeLocationManager.getFreeLocation(storeBuffer.transferableData());
		storeFileManager.writeData(storeLocation, storeBuffer);
		storeBuffer.setMode(BufferMode.Write);
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
		objectTransformer.putInto(content, writableStoreBufferWrapper);
		storeBuffer.setMode(BufferMode.Read);
		storeLocation = storeLocationManager.getFreeLocation(storeBuffer.transferableData());
		storeFileManager.writeData(storeLocation, storeBuffer);
		storeBuffer.setMode(BufferMode.Write);
		storeItemDescription = storeFileManager.createNewStoreCacheEntryDescription(storeLocation);
		descriptionMap.put(storeItemDescription.getStoreID(), storeItemDescription);
		saveDescription(storeItemDescription);
		setContent(storeItemDescription.getStoreID(), content);
		return new StoreItem<>(storeItemDescription.getIndex(), content);
	}

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

	public final Set<Long> getIds() {
		return Collections.unmodifiableSet(descriptionMap.keySet());
	}

}
