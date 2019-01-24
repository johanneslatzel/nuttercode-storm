package de.nuttercode.storm;

import de.nuttercode.util.Immutable;
import de.nuttercode.util.LongInterval;

/**
 * represents the metadata of an item in a {@link Store}.
 * 
 * @author Johannes B. Latzel
 *
 */
@Immutable
class Index {

	/**
	 * length of an {@link Index} in bytes
	 */
	public final static int INDEX_LENGTH = 3 * Long.BYTES;

	/**
	 * id of the represented item
	 */
	private final long id;

	/**
	 * location of the data of the represented item
	 */
	private final LongInterval dataLocation;

	/**
	 * begin position of this index in the DAF. the off-the-end position is
	 * {@link #getIndexBegin()} + {@link #INDEX_LENGTH}
	 */
	private final long indexBegin;

	/**
	 * new Index
	 * 
	 * @param id
	 * @param dataLocation
	 * @param indexLocation
	 */
	public Index(long id, LongInterval dataLocation, long indexLocation) {
		this.id = id;
		this.dataLocation = dataLocation;
		this.indexBegin = indexLocation;
	}

	/**
	 * @return id of the represented item
	 */
	public long getId() {
		return id;
	}

	/**
	 * @return location of the data of the represented item
	 */
	public LongInterval getDataLocation() {
		return dataLocation;
	}

	/**
	 * @return begin position of this index in the DAF. the off-the-end position is
	 *         {@link #getIndexBegin()} + {@link #INDEX_LENGTH}
	 */
	public long getIndexBegin() {
		return indexBegin;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataLocation == null) ? 0 : dataLocation.hashCode());
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + (int) (indexBegin ^ (indexBegin >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Index other = (Index) obj;
		if (dataLocation == null) {
			if (other.dataLocation != null)
				return false;
		} else if (!dataLocation.equals(other.dataLocation))
			return false;
		if (id != other.id)
			return false;
		if (indexBegin != other.indexBegin)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Index [id=" + id + ", dataLocation=" + dataLocation + ", indexBegin=" + indexBegin + "]";
	}

}
