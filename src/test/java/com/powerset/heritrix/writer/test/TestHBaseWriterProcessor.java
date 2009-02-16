package com.powerset.heritrix.writer.test;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.powerset.heritrix.writer.HBaseWriterProcessor;

/**
 * 
 * @author rsmith
 * 
 * TODO: mock objects should be used here to test the api integrity.
 *
 */
public class TestHBaseWriterProcessor {
	HBaseWriterProcessor hwproc;

	@BeforeClass()
	public void createHBaseWriterProcessor() {
		hwproc = new HBaseWriterProcessor();
	}

	@Test()
	public void testHBaseWriterProcessorIntegrity() {
		Assert.assertNotNull(hwproc);
		Assert.assertEquals(hwproc.getURICount(), 0);
	}
	
	
}
