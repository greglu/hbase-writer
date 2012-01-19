package org.archive.io.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;

public class HBaseWriterPool extends WriterPool {

	private String _zkQuorum;
	private int _zkClientPort;
	private String _tableName;
	private HBaseParameters _parameters;
	
	public HBaseWriterPool(AtomicInteger serial, WriterPoolSettings settings,
			int poolMaximumActive, int poolMaximumWait,
			final String zkQuorum, final int zkClientPort, final String tableName, HBaseParameters parameters) {

		super(serial, settings, poolMaximumActive, poolMaximumWait);

		_zkQuorum = zkQuorum;
		_zkClientPort = zkClientPort;
		_tableName = tableName;
		_parameters = parameters;
	}

	@Override
	protected WriterPoolMember makeWriter() {
		try {
			return new HBaseWriter(getSerialNo(), getSettings(), _zkQuorum, _zkClientPort, _tableName, _parameters);
		} catch (IOException e) {
			throw new RuntimeException("Couldn't create a " + HBaseWriter.class.getName() + " writer object");
		}
	}

}
