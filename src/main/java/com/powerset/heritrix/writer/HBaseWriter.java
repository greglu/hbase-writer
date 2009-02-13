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

/**
 * Write to HBase. Puts content into the 'content:' column and all else into the
 * 'curi:' column family. Makes a row key of an url transformation. Creates
 * table if it does not exist.
 * 
 * <p>
 * Limitations: Hard-coded table schema.
 */
public class HBaseWriter extends WriterPoolMember implements ArchiveFileConstants {
	private final Logger LOG = Logger.getLogger(this.getClass().getName());
	private final HTable client;
	// TODO: make this variable configurable in the heritrix sheet:
	// CONTENT_COLUMN_FAMILY
	public static final String CONTENT_COLUMN_FAMILY = "content:";
	// TODO: make this variable configurable in the heritrix sheet:
	// CONTENT_COLUMN
	public static final String CONTENT_COLUMN = CONTENT_COLUMN_FAMILY + "raw_data";
	// TODO: make this variable configurable in the heritrix sheet:
	// CURI_COLUMN_FAMILY
	public static final String CURI_COLUMN_FAMILY = "curi:";

	private static final String IP_COLUMN = CURI_COLUMN_FAMILY + "ip";
	private static final String PATH_FROM_SEED_COLUMN = CURI_COLUMN_FAMILY + "path-from-seed";
	private static final String IS_SEED_COLUMN = CURI_COLUMN_FAMILY + "is-seed";
	private static final String VIA_COLUMN = CURI_COLUMN_FAMILY + "via";
	private static final String URL_COLUMN = CURI_COLUMN_FAMILY + "url";
	private static final String REQUEST_COLUMN = CURI_COLUMN_FAMILY + "request";

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

	public HTable getClient() {
		return client;
	}

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
	 * @param curi
	 *            URI of crawled document
	 * @param ip
	 *            IP of remote machine.
	 * @param ros
	 *            recording input stream that captured the response
	 * @param ris
	 *            recording output stream that captured the GET request
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
		// process the content (optional)
		processContent(bu);
		// Set crawl time.
		bu.setTimestamp(curi.getFetchBeginTime());
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
	 * @param bu
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