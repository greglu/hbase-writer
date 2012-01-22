package org.archive.io.hbase;

public interface Serializer {

	/**
	 * Implement if you want to serialize bytes in a custom manner.
	 * 
	 * @param bytes
	 * @return serialized bytes
	 */
	public byte[] serialize(byte[] bytes);

}
