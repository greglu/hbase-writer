package com.powerset.heritrix.writer.test;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.powerset.heritrix.writer.HBaseWriter;

public class TestHBaseWriter {
	String master = "localhost:60000";
	String table = "test";
	int poolMaximumActive = 10;
	int poolMaximumWait = 20;

	HBaseWriter hw;

	/**
	 * Test that bad table values cannot be used when creating an instance of
	 * HbaseWriter.
	 * 
	 */
	@Test()
	public void testCreateHBaseWriter() throws IOException {
		// Test
		try {
			hw = new HBaseWriter(master, null);
			Assert.assertNull(hw);
		} catch (IllegalArgumentException e) {
			Assert.assertNotNull(e);
		}

		try {
			hw = new HBaseWriter(master, "");
			Assert.assertNull(hw);
		} catch (IllegalArgumentException e) {
			Assert.assertNotNull(e);
		}

	}
}
