/*
 * HBaseWriter
 *
 * $Id$
 *
 * Created on June 23rd, 2007
 *
 * Copyright (C) 2007 stack
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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Keying;
import org.archive.crawler.datamodel.CoreAttributeConstants;
import org.archive.crawler.datamodel.CrawlURI;
import org.archive.io.ArchiveFileConstants;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;

/**
 * Write to HBase.
 * Puts content into the 'content:' column and all else into the 'curi:'
 * column family.  Makes a row key of an url transformation.  Creates table
 * if it does not exist.
 * 
 * <p>Limitations: Hard-coded table name and schema.
 * @author stack
 */
public class HBaseWriter extends WriterPoolMember implements
        ArchiveFileConstants {
    private final Logger LOG = Logger.getLogger(this.getClass().getName());
    
    public static final String CONTENT_COLUMN_FAMILY = "content:";
    public static final String CURI_COLUMN_FAMILY = "curi:";
    public static final String IP_COLUMN = CURI_COLUMN_FAMILY + "ip";
    public static final String PATH_FROM_SEED_COLUMN =
      CURI_COLUMN_FAMILY + "path-from-seed";
    public static final String IS_SEED_COLUMN = CURI_COLUMN_FAMILY + "is-seed";
    public static final String VIA_COLUMN = CURI_COLUMN_FAMILY + "via";
    public static final String URL_COLUMN = CURI_COLUMN_FAMILY + "url";
    public static final String CRAWL_TIME_COLUMN = CURI_COLUMN_FAMILY + "time";
    public static final String REQUEST_COLUMN = CURI_COLUMN_FAMILY + "request";
    
    private final HTable client;

    public HBaseWriter(final String master, final String table)
    throws IOException {
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
    
    protected void createCrawlTable(final HBaseConfiguration c,
      final String table)
    throws IOException {
      HBaseAdmin admin = new HBaseAdmin(c);
      if (admin.tableExists(table)) {
        return;
      }
      HTableDescriptor htd = new HTableDescriptor(table);
      htd.addFamily(new HColumnDescriptor(CONTENT_COLUMN_FAMILY));
      htd.addFamily(new HColumnDescriptor(CURI_COLUMN_FAMILY));
      admin.createTable(htd);
    }

    /**
     * @param curi URI of crawled document
     * @param ip IP of remote machine.
     * @param ros recording input stream that captured the response
     * @param ris recording output stream that captured the GET request (for
     * http*)
     */
    public void write(final CrawlURI curi, final String ip,
      final RecordingOutputStream ros, final RecordingInputStream ris)
    throws IOException {
      String url = curi.toString();
      LOG.info(this.toString() + " " + url);
      String row = Keying.createKey(url);
      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine("Writing " + url + " as " + row.toString());
      }
      BatchUpdate bu = new BatchUpdate(row);
      bu.put(URL_COLUMN, Bytes.toBytes(url));
      bu.put(IP_COLUMN, Bytes.toBytes(ip));
      if (curi.isSeed()) {
        // TODO: Make Bytes.toBytes that takes a boolean.
        bu.put(IS_SEED_COLUMN, Bytes.toBytes(Boolean.TRUE.toString()));
        if (curi.getPathFromSeed() != null
            && curi.getPathFromSeed().trim().length() > 0) {
          bu.put(PATH_FROM_SEED_COLUMN, Bytes.toBytes(curi.getPathFromSeed().trim()));
        }
      }
      String viaStr = (curi.getVia() != null)?
        curi.getVia().toString().trim(): null;
      if (viaStr != null && viaStr.length() > 0) {
        bu.put(VIA_COLUMN, Bytes.toBytes(viaStr));
      }
      // TODO: Reuseable byte buffer rather than allocate each time.
      // Check this is the right size.
      // Save request.
      ReplayInputStream replayStream = null;
      if (ros.getSize() >= 0) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream((int)ros.getSize());
        replayStream = ros.getReplayInputStream();
        try {
          replayStream.readFullyTo(baos);
        } finally {
          replayStream.close();
        }
        baos.close();
        bu.put(REQUEST_COLUMN, baos.toByteArray());
      }
      // Response
      ByteArrayOutputStream baos = new ByteArrayOutputStream((int)ris.getSize());
      replayStream = ris.getReplayInputStream();
      try {
        replayStream.readFullyTo(baos);
      } finally {
        replayStream.close();
      }
      bu.put(CONTENT_COLUMN_FAMILY, baos.toByteArray());
      bu.setTimestamp(curi.getLong(CoreAttributeConstants.A_FETCH_BEGAN_TIME));
      this.client.commit(bu);
    }
}