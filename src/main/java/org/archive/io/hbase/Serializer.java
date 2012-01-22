package org.archive.io.hbase;

// TODO: Auto-generated Javadoc
/**
 * The Interface Serializer.
 */
public interface Serializer {

	/**
	 * Implement if you want to serialize bytes in a custom manner.
	 *
	 * @param bytes the bytes
	 * @return serialized bytes
	 */
	public byte[] serialize(byte[] bytes);

}
