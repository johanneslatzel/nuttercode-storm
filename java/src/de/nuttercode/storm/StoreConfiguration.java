package de.nuttercode.storm;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import de.nuttercode.util.assurance.Assurance;
import de.nuttercode.util.assurance.InLongRange;
import de.nuttercode.util.assurance.NotEmpty;
import de.nuttercode.util.assurance.NotNegative;
import de.nuttercode.util.assurance.NotNull;
import de.nuttercode.util.assurance.Positive;

/**
 * This class describes the configuration of a
 * {@link de.nuttercode.store.Store}. All settings have default values except
 * the {@link #storeName} and the {@link #basePath}. Note: Once a
 * {@link de.nuttercode.store.Store} has been created its proper functioning
 * depends on this attributes - choose appropriate values according to the
 * attributes' descriptions.
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreConfiguration {

	/**
	 * suffix of the data files - should be a short acronym like "daf" or something
	 * similar. must not be empty or null.
	 */
	private String dataFileSuffix;

	/**
	 * the size of {@link java.nio.ByteBuffer}s used throughout the library. must be
	 * positive.
	 */
	private int byteBufferSize;

	/**
	 * the minimum size of new data files. must be positive.
	 */
	private long minimumDataFileSize;

	/**
	 * the unique name of the {@link de.nuttercode.store.Store} within the
	 * {@link #basePath} directory. must not be empty or null.
	 */
	private final String storeName;

	/**
	 * the path to the directory in which all items related to the
	 * {@link de.nuttercode.store.Store} will be saved and loaded. must not be empty
	 * or null.
	 */
	private final Path basePath;

	/**
	 * minimum size the data file will be increased when an increase is performed
	 */
	private int dataFileIncrease;

	/**
	 * first id used in the store
	 */
	private long startID;

	/**
	 * copy-constructor
	 * 
	 * @param configuration
	 */
	public StoreConfiguration(@NotNull StoreConfiguration configuration) {
		Assurance.assureNotNull(configuration);
		storeName = configuration.getStoreName();
		basePath = configuration.getBasePath();
		setDataFileSuffix(configuration.getDataFileSuffix());
		setByteBufferSize(configuration.getByteBufferSize());
		setMinimumDataFileSize(configuration.getMinimumDataFileSize());
		setDataFileIncrease(configuration.getDataFileIncrease());
		setStartID(configuration.getStartID());
	}

	/**
	 * same as {@link #StoreConfiguration(String, Path)
	 * StoreConfiguration(storeName, Paths.get(System.getProperty("user.dir")))}
	 * 
	 * @param storeName
	 */
	public StoreConfiguration(@NotEmpty String storeName) {
		this(storeName, Paths.get(System.getProperty("user.dir")));
	}

	/**
	 * default initializes all attributes other than {@link #storeName} and
	 * {@link #basePath}
	 * 
	 * @param storeName the unique name of the {@link de.nuttercode.store.Store}
	 *                  within the {@link #basePath}a
	 * @param basePath  the root directory path in which all store-files will be
	 *                  saved and loaded
	 */
	public StoreConfiguration(@NotEmpty String storeName, @NotNull Path basePath) {
		Assurance.assureNotEmpty(storeName);
		Assurance.assureNotNull(basePath);
		this.storeName = storeName;
		this.basePath = basePath;
		dataFileSuffix = "daf";
		byteBufferSize = 8192;
		minimumDataFileSize = 1024;
		setDataFileIncrease(512);
		setStartID(500);
	}

	/**
	 * @return suffix of the data files
	 * 
	 */
	public @NotEmpty String getDataFileSuffix() {
		return dataFileSuffix;
	}

	/**
	 * @return the size of {@link ByteBuffer}s used in the {@link Store} library.
	 * 
	 */
	public @Positive int getByteBufferSize() {
		return byteBufferSize;
	}

	/**
	 * @return the minimum size of new data file
	 */
	public @NotNegative long getMinimumDataFileSize() {
		return minimumDataFileSize;
	}

	/**
	 * @return the unique name of the {@link Store} within the
	 *         {@link #getBasePath()} directory
	 */
	public @NotEmpty String getStoreName() {
		return storeName;
	}

	/**
	 * @return {@link #basePath}
	 */
	public Path getBasePath() {
		return basePath;
	}

	/**
	 * @return the data file (DAF)
	 * 
	 */
	public @NotNull File getDataFile() {
		return new File(getBasePath().toString() + File.separatorChar + getStoreName() + "." + getDataFileSuffix());
	}

	/**
	 * sets suffix of the data files. should be a short acronym like "daf" or
	 * something similar.
	 * 
	 * @param dataFileSuffix
	 * @throws IllegalArgumentException if dataFileSuffix is empty or null
	 */
	public void setDataFileSuffix(@NotEmpty String dataFileSuffix) {
		Assurance.assureNotEmpty(dataFileSuffix);
		this.dataFileSuffix = dataFileSuffix;
	}

	/**
	 * the size of {@link java.nio.ByteBuffer}s used throughout the library.
	 * 
	 * @param byteBufferSize
	 * @throws IllegalArgumentException if byteBufferSize <= 0
	 */
	public void setByteBufferSize(@Positive int byteBufferSize) {
		Assurance.assurePositive(byteBufferSize);
		this.byteBufferSize = byteBufferSize;
	}

	/**
	 * sets the minimum size of new data files.
	 * 
	 * @param minimumDataFileSize
	 * @throws IllegalArgumentException if minimumDataFileSize <= 0
	 */
	public void setMinimumDataFileSize(@Positive long minimumDataFileSize) {
		Assurance.assurePositive(minimumDataFileSize);
		this.minimumDataFileSize = minimumDataFileSize;
	}

	/**
	 * @return minimum size the data file will be increased when an increase is
	 *         performed
	 * 
	 */
	public int getDataFileIncrease() {
		return dataFileIncrease;
	}

	/**
	 * set minimum size the data file will be increased when an increase is
	 * performed
	 * 
	 * @param dataFileIncrease
	 */
	public void setDataFileIncrease(@NotNegative int dataFileIncrease) {
		Assurance.assureNotNegative(dataFileIncrease);
		this.dataFileIncrease = dataFileIncrease;
	}

	/**
	 * @return first id used in the store
	 */
	public long getStartID() {
		return startID;
	}

	/**
	 * sets the first id used in the store. all other ids will be in the range of
	 * [Long.MIN_VALUE, Long.MAX_VALUE] \ [0, startID)
	 * 
	 * @param startID
	 */
	public void setStartID(@InLongRange(begin = DataFile.MIN_ID, end = DataFile.MAX_ID) long startID) {
		Assurance.assureBoundaries(startID, DataFile.MIN_ID, DataFile.MAX_ID);
		this.startID = startID;
	}

}
