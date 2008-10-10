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
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HClient;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.util.Keying;
import org.apache.hadoop.io.Text;
import org.archive.crawler.datamodel.CrawlURI;
import org.archive.io.ArchiveFileConstants;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;

/**
 * Write to HBase.
 * Puts content into the 'content:' column and all else into the 'curi:'
 * column family.  Makes a row key of an url transformation.
 * 
 * <p>Limitations: Only a hard-coded number of versions for now and currently
 * hbase does not allow specification of crawltime as version time.
 * @author stack
 */
public class HBaseWriter extends WriterPoolMember implements
        ArchiveFileConstants {
    private static final Logger LOG = Logger.getLogger(HBaseWriter.class.
        getName());
    
    public static final Text CONTENT_COLUMN_FAMILY = new Text("content:");
    public static final Text CURI_COLUMN_FAMILY = new Text("curi:");
    public static final Text IP_COLUMN = new Text(CURI_COLUMN_FAMILY + "ip");
    public static final Text PATH_FROM_SEED_COLUMN =
        new Text(CURI_COLUMN_FAMILY + "path-from-seed");
    public static final Text IS_SEED_COLUMN =
        new Text(CURI_COLUMN_FAMILY + "is-seed");
    public static final Text VIA_COLUMN = new Text(CURI_COLUMN_FAMILY + "via");
    public static final Text URL_COLUMN = new Text(CURI_COLUMN_FAMILY + "url");
    public static final Text CRAWL_TIME_COLUMN =
        new Text(CURI_COLUMN_FAMILY + "time");
    public static final Text REQUEST_COLUMN =
        new Text(CURI_COLUMN_FAMILY + "request");
    
    private final HClient client;

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
        this.client = new HClient(c);
        Text t = new Text(table);
        if (!this.client.tableExists(t)) {
            createCrawlTable(t);
        }
        this.client.openTable(t);
    }
    
    protected void createCrawlTable(final Text tableName)
    throws IOException {
      HTableDescriptor htd = new HTableDescriptor(tableName.toString());
      this.client.createTable(htd);
      boolean success = false;
      try {
          this.client.disableTable(tableName);
          HColumnDescriptor hcd = new HColumnDescriptor(CONTENT_COLUMN_FAMILY,
              3, CompressionType.RECORD, false, Integer.MAX_VALUE, null);
          this.client.addColumn(tableName, hcd);
          hcd = new HColumnDescriptor(CURI_COLUMN_FAMILY, 3,
              CompressionType.NONE, false, Integer.MAX_VALUE, null);
          this.client.addColumn(tableName, hcd);
          this.client.enableTable(tableName);
          success = true;
      } finally {
        if (!success) {
          this.client.deleteTable(tableName);
        }
      }
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
        String row;
        try {
            row = Keying.createKey(url);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Writing " + url + " as " + row.toString());
            }
        } catch (URISyntaxException e) {
           throw new IOException(e.toString());
        }
        long lockid = this.client.startUpdate(new Text(row));
        try {
            this.client.put(lockid, URL_COLUMN,
                url.getBytes(HConstants.UTF8_ENCODING));
            this.client.put(lockid, IP_COLUMN,
                ip.getBytes(HConstants.UTF8_ENCODING));
            // TODO: Make this baos reuseable.
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(curi.getLong(CrawlURI.A_FETCH_BEGAN_TIME));
            dos.close();
            this.client.put(lockid, CRAWL_TIME_COLUMN, baos.toByteArray());
            if (curi.isSeed()) {
                baos.reset();
                dos = new DataOutputStream(baos);
                dos.writeBoolean(curi.isSeed());
                dos.close();
                this.client.put(lockid, IS_SEED_COLUMN, baos.toByteArray());
            }
            if (curi.getPathFromSeed() != null &&
                curi.getPathFromSeed().trim().length() > 0) {
              this.client.put(lockid, PATH_FROM_SEED_COLUMN,
                  curi.getPathFromSeed().trim().
                  getBytes(HConstants.UTF8_ENCODING));
            }
            String viaStr = (curi.getVia() != null)?
                curi.getVia().toString().trim(): null;
            if (viaStr != null && viaStr.length() > 0) {
                this.client.put(lockid, VIA_COLUMN,
                    viaStr.getBytes(HConstants.UTF8_ENCODING));
            }
            // TODO: Reuseable byte buffer rather than allocate each time.
            // Check this is the right size.
            // Save request.
            ReplayInputStream replayStream = null;
            if (ros.getSize() >= 0) {
                baos = new ByteArrayOutputStream((int) ros.getSize());
                replayStream = ros.getReplayInputStream();
                try {
                    replayStream.readFullyTo(baos);
                } finally {
                    replayStream.close();
                }
                baos.close();
                this.client.put(lockid, REQUEST_COLUMN, baos.toByteArray());
            }
            // Response
            baos = new ByteArrayOutputStream((int)ris.getSize());
            replayStream = ris.getReplayInputStream();
            try {
                replayStream.readFullyTo(baos);
            } finally {
               replayStream.close();
            }
            this.client.put(lockid, CONTENT_COLUMN_FAMILY, baos.toByteArray());
            this.client.commit(lockid);
            lockid = -1;
        } finally {
            if (lockid != -1) {
                this.client.abort(lockid);
            }
        }
    }
}
