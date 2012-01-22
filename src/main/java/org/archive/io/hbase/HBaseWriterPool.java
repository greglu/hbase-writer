package org.archive.io.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;

// TODO: Auto-generated Javadoc
/**
 * The Class HBaseWriterPool.
 */
public class HBaseWriterPool extends WriterPool {

	/** The _parameters. */
	private HBaseParameters _parameters;

	/**
	 * Instantiates a new h base writer pool.
	 *
	 * @param serial the serial
	 * @param settings the settings
	 * @param poolMaximumActive the pool maximum active
	 * @param poolMaximumWait the pool maximum wait
	 * @param parameters the parameters
	 */
	public HBaseWriterPool(AtomicInteger serial, WriterPoolSettings settings, int poolMaximumActive, int poolMaximumWait, HBaseParameters parameters) {

		super(serial, settings, poolMaximumActive, poolMaximumWait);

		_parameters = parameters;
	}

	/* (non-Javadoc)
	 * @see org.archive.io.WriterPool#makeWriter()
	 */
	@Override
	protected WriterPoolMember makeWriter() {
		try {
			return new HBaseWriter(getSerialNo(), getSettings(), _parameters);
		} catch (IOException e) {
			throw new RuntimeException("Couldn't create a " + HBaseWriter.class.getName() + " writer object");
		}
	}

}
