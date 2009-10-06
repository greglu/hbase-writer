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
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Keying;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveFileConstants;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.modules.ProcessorURI;

// TODO: Auto-generated Javadoc
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
	public static final String CONTENT_COLUMN_FAMILY = "content:";
	// TODO: make this variable configurable in the heritrix sheet:
	// CONTENT_COLUMN
	/** The Constant CONTENT_COLUMN. */
	public static final String CONTENT_COLUMN = CONTENT_COLUMN_FAMILY + "raw_data";
	// TODO: make this variable configurable in the heritrix sheet:
	// CURI_COLUMN_FAMILY
	/** The Constant CURI_COLUMN_FAMILY. */
	public static final String CURI_COLUMN_FAMILY = "curi:";

	/** The Constant IP_COLUMN. */
	private static final String IP_COLUMN = CURI_COLUMN_FAMILY + "ip";
	
	/** The Constant PATH_FROM_SEED_COLUMN. */
	private static final String PATH_FROM_SEED_COLUMN = CURI_COLUMN_FAMILY + "path-from-seed";
	
	/** The Constant IS_SEED_COLUMN. */
	private static final String IS_SEED_COLUMN = CURI_COLUMN_FAMILY + "is-seed";
	
	/** The Constant VIA_COLUMN. */
	private static final String VIA_COLUMN = CURI_COLUMN_FAMILY + "via";
	
	/** The Constant URL_COLUMN. */
	private static final String URL_COLUMN = CURI_COLUMN_FAMILY + "url";
	
	/** The Constant REQUEST_COLUMN. */
	private static final String REQUEST_COLUMN = CURI_COLUMN_FAMILY + "request";

	/**
	 * Instantiates a new HBaseWriter for the WriterPool to use in heritrix2.
	 * 
	 * @param master the master
	 * @param table the table
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public HBaseWriter(final String master, final String table) throws IOException {
		super(null, null, null, false, null);
		if (table == null || table.length() <= 0) {
			throw new IllegalArgumentException("Must specify a table name");
		}
		HBaseConfiguration c = new HBaseConfiguration();
		if (master != null && master.length() > 0) {
			c.set(HConstants.MASTER_ADDRESS, master);
		}
		createCrawlTable(c, table);
		this.client = new HTable(c, table);
	}

	/**
	 * Gets the HTable client.
	 * 
	 * @return the client
	 */
	public HTable getClient() {
		return client;
	}

	/**
	 * Creates the crawl table in HBase.
	 * 
	 * @param c the c
	 * @param table the table
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void createCrawlTable(final HBaseConfiguration c, final String table) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(c);
		if (admin.tableExists(table)) {
			return;
		}
		HTableDescriptor htd = new HTableDescriptor(table);
		htd.addFamily(new HColumnDescriptor(CONTENT_COLUMN_FAMILY));
		htd.addFamily(new HColumnDescriptor(CURI_COLUMN_FAMILY));
		admin.createTable(htd);
		LOG.info("Created table " + htd.toString());
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
		String url = curi.toString();
		String row = Keying.createKey(url);
		if (LOG.isTraceEnabled()) {
			LOG.trace("Writing " + url + " as " + row.toString());
		}
		BatchUpdate bu = new BatchUpdate(row);
		bu.put(URL_COLUMN, Bytes.toBytes(url));
		bu.put(IP_COLUMN, Bytes.toBytes(ip));
		if (curi.isSeed()) {
			// TODO: Make Bytes.toBytes that takes a boolean.
			bu.put(IS_SEED_COLUMN, Bytes.toBytes(Boolean.TRUE.toString()));
			if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0) {
				bu.put(PATH_FROM_SEED_COLUMN, Bytes.toBytes(curi.getPathFromSeed().trim()));
			}
		}
		String viaStr = (curi.getVia() != null) ? curi.getVia().toString().trim() : null;
		if (viaStr != null && viaStr.length() > 0) {
			bu.put(VIA_COLUMN, Bytes.toBytes(viaStr));
		}
		// Request
		if (ros.getSize() > 0) {
			add(bu, REQUEST_COLUMN, ros.getReplayInputStream(), (int) ros.getSize());
		}
		// Response
		add(bu, CONTENT_COLUMN, ris.getReplayInputStream(), (int) ris.getSize());
		// Set crawl time.
		bu.setTimestamp(curi.getFetchBeginTime());
		// process the content (optional)
		processContent(bu);
		this.client.commit(bu);
	}

	/**
	 * This is a stub method and is here to allow extension/overriding for
	 * custom content parsing, data manipulation and to populate new columns.
	 * 
	 * For Example : html parsing, text extraction, analysis and transformation
	 * and storing the results in new column families/columns using the batch
	 * update object.
	 * 
	 * @param bu the bu
	 */
	protected void processContent(BatchUpdate bu) {
		// byte[] content = bu.get(CONTENT_COLUMN);
		// process content.....
		// bu.put("some:new_column", someParsedByteArray);
	}

	/*
	 * Add ReplayInputStream to the passed BatchUpdate.
	 * 
	 * @param bu
	 * 
	 * @param key
	 * 
	 * @param ris
	 * 
	 * @param baos
	 * 
	 * @throws IOException
	 */
	/**
	 * Adds the.
	 * 
	 * @param bu the bu
	 * @param key the key
	 * @param ris the ris
	 * @param size the size
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void add(final BatchUpdate bu, final String key, final ReplayInputStream ris, final int size) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
		try {
			ris.readFullyTo(baos);
		} finally {
			ris.close();
		}
		baos.close();
		bu.put(key, baos.toByteArray());
	}
}