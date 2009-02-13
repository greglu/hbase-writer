package com.powerset.heritrix.writer.test;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.powerset.heritrix.writer.HBaseWriterPool;

public class TestHBaseWriterPool {
	String master = "locahost";
	String table = "test";
	int poolMaximumActive = 10;
	int poolMaximumWait = 20;

	HBaseWriterPool hwp;

	@BeforeClass()
	public void createHBaseWriterPool() {
		hwp = new HBaseWriterPool(master, table, poolMaximumActive,
				poolMaximumWait);
	}

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
