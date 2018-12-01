package de.nuttercode.storm;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import de.nuttercode.storm.core.StoreFileManager;
import de.nuttercode.util.assurance.Assurance;
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
	 * suffix of the description files - should be a short acronym like "def" or
	 * something similar. must not be empty or null.
	 */
	private String descriptionFileSuffix;

	/**
	 * suffix of the id file - should be a short acronym like "id" or something
	 * similar. must not be empty or null.
	 */
	private String idFileSuffix;

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
	 * the number of ids {@link StoreFileManager} will buffer at most
	 */
	private int idIncrease;

	/**
	 * copy-constructor
	 * 
	 * @param configuration
	 */
	public StoreConfiguration(StoreConfiguration configuration) {
		storeName = configuration.getStoreName();
		basePath = configuration.getBasePath();
		dataFileSuffix = configuration.getDataFileSuffix();
		descriptionFileSuffix = configuration.getDescriptionFileSuffix();
		byteBufferSize = configuration.getByteBufferSize();
		minimumDataFileSize = configuration.getMinimumDataFileSize();
		idFileSuffix = configuration.getIdFileSuffix();
		idIncrease = configuration.getIdIncrease();
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
		descriptionFileSuffix = "def";
		idFileSuffix = "id";
		byteBufferSize = 512;
		minimumDataFileSize = 1024;
		idIncrease = 500;
	}

	/**
	 * @return suffix of the data files
	 * 
	 */
	public @NotEmpty String getDataFileSuffix() {
		return dataFileSuffix;
	}

	/**
	 * @return suffix of the description files
	 * 
	 */
	public String getDescriptionFileSuffix() {
		return descriptionFileSuffix;
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
	 * @return the unique name of the {@link Store} within the
	 *         {@link #getBasePath()} directory.
	 * 
	 */
	public @NotNull Path getStoreDirectory() {
		return Paths.get(getBasePath().toString(), getStoreName());
	}

	/**
	 * @return suffix of the id file
	 * 
	 */
	public @NotEmpty String getIdFileSuffix() {
		return idFileSuffix;
	}

	/**
	 * @return number of ids {@link StoreFileManager} will buffer at most
	 * 
	 */
	public int getIdIncrease() {
		return idIncrease;
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
	 * suffix of the description files. should be a short acronym like "def"
	 * orsomething similar.
	 * 
	 * @param descriptionFileSuffix
	 * @throws IllegalArgumentException if descriptionFileSuffix is empty or null
	 */
	public void setDescriptionFileSuffix(@NotEmpty String descriptionFileSuffix) {
		Assurance.assureNotEmpty(dataFileSuffix);
		this.descriptionFileSuffix = descriptionFileSuffix;
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
	public void setMinimumDataFileSize(@Positive int minimumDataFileSize) {
		Assurance.assurePositive(minimumDataFileSize);
		this.minimumDataFileSize = minimumDataFileSize;
	}

	/**
	 * sets suffix of the id file. should be a short acronym like "id" or something
	 * similar
	 * 
	 * @param idFileSuffix
	 * @throws IllegalArgumentException if {@link #idFileSuffix} is empty or null
	 */
	public void setIdFileSuffix(@NotEmpty String idFileSuffix) {
		Assurance.assureNotEmpty(idFileSuffix);
		this.idFileSuffix = idFileSuffix;
	}

	/**
	 * sets the number of ids {@link StoreFileManager} will buffer at most
	 * 
	 * @param idIncrease
	 */
	public void setIdIncrease(@Positive int idIncrease) {
		Assurance.assurePositive(idIncrease);
		this.idIncrease = idIncrease;
	}

}
