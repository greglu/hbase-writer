/**
 * HBaseWriter
 *
 * $Id$
 *
 * Created on June 23rd, 2007
 *
 * This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 * Heritrix is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * any later version.
 *
 * Heritrix is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser Public License for more details.
 *
 * You should have received a copy of the GNU Lesser Public License
 * along with Heritrix; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package com.powerset.heritrix.writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Keying;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveFileConstants;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.modules.ProcessorURI;

/**
 * Write crawled content as records to an HBase table. 
 * Puts content into the 'content:raw_data' column and all else into the
 * 'curi:' column family. Makes a row key of an url transformation. Creates
 * table if it does not exist.
 * 
	The following is a complete list of columns that get written to by default:
	
	content:raw_data
	
	curi:ip
	curi:path-from-seed
	curi:is-seed
	curi:via
	curi:url
	curi:request
	
 * 
 * <p>
 * Limitations: Hard-coded table schema.  
 */
public class HBaseWriter extends WriterPoolMember implements ArchiveFileConstants {
	
	/** The LOG. */
	private final Logger LOG = Logger.getLogger(this.getClass().getName());
	
	/** The client. */
	private final HTable client;
	// TODO: make this variable configurable in the heritrix sheet:
	// CONTENT_COLUMN_FAMILY
	/** The Constant CONTENT_COLUMN_FAMILY. */
	public static final String CONTENT_COLUMN_FAMILY = "content";
	// TODO: make this variable configurable in the heritrix sheet:
	/** The Constant CONTENT_COLUMN. */
	public static final String CONTENT_COLUMN_NAME = "raw_data";
	// TODO: make this variable configurable in the heritrix sheet:
	// CURI_COLUMN_FAMILY
	/** The Constant CURI_COLUMN_FAMILY. */
	public static final String CURI_COLUMN_FAMILY = "curi";
	// TODO: make this variable configurable in the heritrix sheet:
	/** The Constant IP_COLUMN. */
	private static final String IP_COLUMN_NAME = "ip";
	// TODO: make this variable configurable in the heritrix sheet:
	/** The Constant PATH_FROM_SEED_COLUMN. */
	private static final String PATH_FROM_SEED_COLUMN_NAME = "path-from-seed";
	// TODO: make this variable configurable in the heritrix sheet:
	/** The Constant IS_SEED_COLUMN. */
	private static final String IS_SEED_COLUMN_NAME = "is-seed";
	// TODO: make this variable configurable in the heritrix sheet:
	/** The Constant VIA_COLUMN. */
	private static final String VIA_COLUMN_NAME = "via";
	// TODO: make this variable configurable in the heritrix sheet:
	/** The Constant URL_COLUMN. */
	private static final String URL_COLUMN_NAME = "url";
	// TODO: make this variable configurable in the heritrix sheet:
	/** The Constant REQUEST_COLUMN. */
	private static final String REQUEST_COLUMN_NAME = "request";

	/**
	 * Gets the HTable client.
	 * 
	 * @return the client
	 */
	public HTable getClient() {
		return client;
	}
	
	/**
	 * Instantiates a new HBaseWriter for the WriterPool to use in heritrix2.
	 * 
	 * @param zkQuorum 
	 * 		the zookeeper quorum. The list of hosts that make up you zookeeper quorum.  
	 * 		i.e.:  zkHost1,zkHost2,zkHost3  
	 * @param tableName 
	 * 		the table in hbase to write to.  i.e. : webtable
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public HBaseWriter(final String zkQuorum, final String tableName) throws IOException {
		super(null, null, null, false, null);
		if (tableName == null || tableName.length() <= 0) {
			throw new IllegalArgumentException("Must specify a table name");
		}
		HBaseConfiguration hbaseConfiguration = new HBaseConfiguration();
		if (zkQuorum != null && zkQuorum.length() > 0) {
			LOG.info("setting zookeeper quorum to : " + zkQuorum);
			hbaseConfiguration.setStrings(HConstants.ZOOKEEPER_QUORUM, zkQuorum.split(","));
		}
		LOG.debug("zookeeper quorum value: " + hbaseConfiguration.get(HConstants.ZOOKEEPER_QUORUM));
		createCrawlTable(hbaseConfiguration, tableName);
		this.client = new HTable(hbaseConfiguration, tableName);
	}

	/**
	 * Creates the crawl table in HBase.
	 * 
	 * @param hbaseConfiguration the c
	 * @param hbaseTableName the table
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void createCrawlTable(final HBaseConfiguration hbaseConfiguration, final String hbaseTableName) throws IOException {
		// an HBase admin object to manage hbase tables.
		HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfiguration);
		if (hbaseAdmin.tableExists(hbaseTableName)) {
			boolean foundContentColumnFamily = false;
			boolean foundCURIColumnFamily = false;
			LOG.debug("Checking table: " + hbaseTableName + " for structure...");
			// Check the existing table and manipulate it if necessary
			// to conform to the pre-existing table schema.
			HTableDescriptor existingHBaseTable = hbaseAdmin.getTableDescriptor(Bytes.toBytes(hbaseTableName));
			for (HColumnDescriptor hColumnDescriptor: existingHBaseTable.getFamilies()) {
				if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(CONTENT_COLUMN_FAMILY)) {
					foundContentColumnFamily = true;
				} else if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(CURI_COLUMN_FAMILY)) {
					foundCURIColumnFamily = true;
				}
			}
			// modify the table if it's missing any of the column families.
			if (!foundContentColumnFamily || !foundCURIColumnFamily) {
				LOG.info("Disabling table: " + hbaseTableName);
				hbaseAdmin.disableTable(hbaseTableName);
				if (!foundContentColumnFamily) {
					LOG.info("Adding column to table: " + hbaseTableName + " column: " + CONTENT_COLUMN_FAMILY);
					existingHBaseTable.addFamily(new HColumnDescriptor(CONTENT_COLUMN_FAMILY));		
				}
				if (!foundCURIColumnFamily) {
					LOG.info("Adding column to table: " + hbaseTableName + " column: " + CURI_COLUMN_FAMILY);
					existingHBaseTable.addFamily(new HColumnDescriptor(CURI_COLUMN_FAMILY));	
				}
				LOG.info("Enabling table: " + hbaseTableName);
				hbaseAdmin.enableTable(hbaseTableName);	
			}
			LOG.debug("Done checking table: " + hbaseTableName);
		} else {
			// create a new hbase table
			LOG.info("Creating table " + hbaseTableName);
			HTableDescriptor newHBaseTable = new HTableDescriptor(hbaseTableName);
			newHBaseTable.addFamily(new HColumnDescriptor(CONTENT_COLUMN_FAMILY));
			newHBaseTable.addFamily(new HColumnDescriptor(CURI_COLUMN_FAMILY));
			// create the table
			hbaseAdmin.createTable(newHBaseTable);
			LOG.info("Created table " + newHBaseTable.toString());
		}
	}

	/**
	 * Write the crawled output to the configured HBase table.
	 * Write each row key as the url with reverse domain and optionally process any content.
	 * 
	 * @param curi URI of crawled document
	 * @param ip IP of remote machine.
	 * @param ros recording input stream that captured the response
	 * @param ris recording output stream that captured the GET request
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void write(final ProcessorURI curi, final String ip, final RecordingOutputStream ros, final RecordingInputStream ris)
			throws IOException {
		// generate the target url of the crawled document
		String url = curi.toString();
		// create the hbase friendly rowkey
		String rowKey = Keying.createKey(url);
		if (LOG.isTraceEnabled()) {
			LOG.trace("Writing " + url + " as " + rowKey.toString());
		}
		// create an hbase updateable object (the put object)
		// Constructor takes the rowkey as the only argument
		Put batchPut = new Put(Bytes.toBytes(rowKey));
		// write the target url to the url column
		batchPut.add(Bytes.toBytes(CURI_COLUMN_FAMILY), Bytes.toBytes(URL_COLUMN_NAME), curi.getFetchBeginTime(), Bytes.toBytes(url));
		// write the target ip to the ip column
		batchPut.add(Bytes.toBytes(CURI_COLUMN_FAMILY), Bytes.toBytes(IP_COLUMN_NAME), curi.getFetchBeginTime(), Bytes.toBytes(ip));
		// is the url part of the seed url (the initial url(s) used to start the crawl)
		if (curi.isSeed()) {
			batchPut.add(Bytes.toBytes(CURI_COLUMN_FAMILY), Bytes.toBytes(IS_SEED_COLUMN_NAME), Bytes.toBytes(Boolean.TRUE));
			if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0) {
				batchPut.add(Bytes.toBytes(CURI_COLUMN_FAMILY), Bytes.toBytes(PATH_FROM_SEED_COLUMN_NAME), Bytes.toBytes(curi.getPathFromSeed().trim()));
			}
		}
		String viaStr = (curi.getVia() != null) ? curi.getVia().toString().trim() : null;
		if (viaStr != null && viaStr.length() > 0) {
			batchPut.add(Bytes.toBytes(CURI_COLUMN_FAMILY), Bytes.toBytes(VIA_COLUMN_NAME), Bytes.toBytes(viaStr));
		}
		// Write the Crawl Request to the Put object
		if (ros.getSize() > 0) {
			batchPut.add(Bytes.toBytes(CURI_COLUMN_FAMILY), Bytes.toBytes(REQUEST_COLUMN_NAME), 
					getByteArrayFromInputStream(ros.getReplayInputStream(), (int) ros.getSize()));
		}
		// Write the Crawl Response to the Put object
		batchPut.add(Bytes.toBytes(CONTENT_COLUMN_FAMILY), Bytes.toBytes(CONTENT_COLUMN_NAME), 
				getByteArrayFromInputStream(ris.getReplayInputStream(), (int) ris.getSize()));
		
		// reset the input steam for the content processor.
		ris.getReplayInputStream().setToResponseBodyStart();
		// process the content (optional)
		processContent(batchPut, ris.getReplayInputStream(), (int) ris.getSize());
		// Set crawl time as the timestamp to the Put object.
		batchPut.setTimeStamp(curi.getFetchBeginTime());
		// write the Put object to the HBase table
		this.client.put(batchPut);
	}

	/**
	 * Read the ReplayInputStream and write it to the given BatchUpdate with the given column.
	 * 
	 * @param column the column for the given data.
	 * @param replayInputStream the ris the cell data as a replay input stream
	 * @param streamSize the size
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected byte[] getByteArrayFromInputStream(final ReplayInputStream replayInputStream, final int streamSize) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(streamSize);
		try {
			// read the InputStream to the ByteArrayOutputStream
			replayInputStream.readFullyTo(baos);
		} finally {
			replayInputStream.close();
		}
		baos.close();
		return baos.toByteArray();
	}

	/**
	 * This is a stub method and is here to allow extension/overriding for
	 * custom content parsing, data manipulation and to populate new columns.
	 * 
	 * For Example : html parsing, text extraction, analysis and transformation
	 * and storing the results in new column families/columns using the batch
	 * update object.
	 * 
	 * @param batchUpdate the batchUpdate - the hbase row object whose state can be manipulated
	 * before the object is written.
	 */
	protected void processContent(Put put, ReplayInputStream replayInputStream, int streamSize) throws IOException {
		// process content array and parse it to a new byte array.....
		// byte[] rowKey = put.getRow();
		// byte[] rawContent = this.getByteArrayFromInputStream(replayInputStream, streamSize)
		// byte[] someParsedByteArray = ....
		// put.add(Bytes.toBytes("some_column_family"), Bytes.toBytes("a_new_column_name"), someParsedByteArray);
	}
}