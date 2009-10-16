package com.powerset.heritrix.writer.test;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.powerset.heritrix.writer.HBaseWriterPool;

/**
 * The Class TestHBaseWriterPool.
 */
public class TestHBaseWriterPool {
	
	/** The zkQuorum. */
	String zkQuorum = "locahost";
	
	/** zkClientPort */
	int zkClientPort = 2181;
	
	/** The table. */
	String table = "test";
	
	/** The pool maximum active. */
	int poolMaximumActive = 10;
	
	/** The pool maximum wait. */
	int poolMaximumWait = 20;

	/** The hwp. */
	HBaseWriterPool hwp;

	/**
	 * Creates the h base writer pool.
	 */
	@BeforeClass()
	public void createHBaseWriterPool() {
		hwp = new HBaseWriterPool(zkQuorum, zkClientPort, table, poolMaximumActive, poolMaximumWait);
	}

	/**
	 * Test h base writer pool integrity.
	 */
	@Test()
	public void testHBaseWriterPoolIntegrity() {
		Assert.assertNotNull(hwp);
		Assert.assertEquals(hwp.getNumActive(), 0);
		Assert.assertEquals(hwp.getNumIdle(), 0);
		Assert.assertEquals(hwp.getSerialNo().intValue(), 0);
		Assert.assertFalse(hwp.getSettings().isCompressed());
		Assert.assertNull(hwp.getSettings().getPrefix());
		Assert.assertNull(hwp.getSettings().getSuffix());
	}
}
