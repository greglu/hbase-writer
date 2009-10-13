package com.powerset.heritrix.writer.test;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.powerset.heritrix.writer.HBaseWriter;

// TODO: Auto-generated Javadoc
/**
 * The Class TestHBaseWriter.
 */
public class TestHBaseWriter {
	
	/** The master. */
	String zkQuorum = "localhost";
	
	/** The table. */
	String table = "test";
	
	/** The pool maximum active. */
	int poolMaximumActive = 10;
	
	/** The pool maximum wait. */
	int poolMaximumWait = 20;

	/** The hw. */
	HBaseWriter hw;

	/**
	 * Test that bad table values cannot be used when creating an instance of
	 * HbaseWriter.
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test()
	public void testCreateHBaseWriter() throws IOException {
		// Test
		try {
			hw = new HBaseWriter(zkQuorum, null);
			Assert.assertNull(hw);
		} catch (IllegalArgumentException e) {
			Assert.assertNotNull(e);
		}

		try {
			hw = new HBaseWriter(zkQuorum, "");
			Assert.assertNull(hw);
		} catch (IllegalArgumentException e) {
			Assert.assertNotNull(e);
		}

	}
}
