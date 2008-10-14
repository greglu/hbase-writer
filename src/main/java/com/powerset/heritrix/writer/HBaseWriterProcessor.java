/*
 * HBaseWriterProcessor
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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.HConstants;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.modules.ModuleAttributeConstants;
import org.archive.modules.ProcessResult;
import org.archive.modules.Processor;
import org.archive.modules.ProcessorURI;
import org.archive.modules.fetcher.FetchStatusCodes;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.ServerCache;
import org.archive.modules.net.ServerCacheUtil;
import org.archive.state.Expert;
import org.archive.state.Immutable;
import org.archive.state.Key;
import org.archive.state.KeyManager;
import org.archive.state.StateProvider;
import org.archive.util.IoUtils;

/**
 * HBase writer.
 * @author stack
 */
public class HBaseWriterProcessor extends Processor {
  private static final long serialVersionUID = 7166781798179114353L;

  private final Logger logger = Logger.getLogger(this.getClass().getName());

  /**
   * Location of hbase master.
   */
  @Immutable
  public static final Key<String> MASTER =
    Key.make(HConstants.DEFAULT_MASTER_ADDRESS);

  /**
   * HBase table to crawl into.
   */
  @Immutable
  public static final Key<String> TABLE = Key.make("crawl");

  /**
   * Maximum active files in pool. This setting cannot be varied over the life
   * of a crawl.
   */
  @Immutable
  final public static Key<Integer> POOL_MAX_ACTIVE =
    Key.make(WriterPool.DEFAULT_MAX_ACTIVE);


  /**
   * Maximum time to wait on pool element (milliseconds). This setting cannot
   * be varied over the life of a crawl.
   */
  @Immutable
  final public static Key<Integer> POOL_MAX_WAIT =
    Key.make(WriterPool.DEFAULT_MAXIMUM_WAIT);

  @Immutable
  final public static Key<ServerCache> SERVER_CACHE = 
    Key.makeAuto(ServerCache.class);

  /**
   * Total file bytes to write to disk. Once the size of all files on disk has
   * exceeded this limit, this processor will stop the crawler. A value of
   * zero means no upper limit.
   */
  @Immutable @Expert
  final public static Key<Long> TOTAL_BYTES_TO_WRITE = Key.make(0L);


  /**
   * Reference to pool.
   */
  private transient WriterPool pool = null;
  private ServerCache serverCache;
  private int maxActive;
  private int maxWait;
  private String master;
  private String table;

  /*
   * Total number of bytes written to disc.
   */
  private long totalBytesWritten = 0;


  static {
    KeyManager.addKeys(HBaseWriterProcessor.class);
  }

  public HBaseWriterProcessor() {
    super();
  }

  public synchronized void initialTasks(StateProvider context) {
    this.serverCache = context.get(this, SERVER_CACHE);
    this.maxActive = context.get(this, POOL_MAX_ACTIVE).intValue();
    this.maxWait = context.get(this, POOL_MAX_WAIT).intValue();
    this.master = context.get(this, MASTER);
    this.table = context.get(this, TABLE);
    setupPool();
  }

  protected String getMaster() {
    return this.master;
  }

  protected String getTable() {
    return this.table;
  }

  protected void setupPool() {
    setPool(new HBaseWriterPool(getMaster(), getTable(), getMaxActive(), getMaxWait()));
  }

  protected int getMaxActive() {
    return maxActive;
  }

  protected int getMaxWait() {
    return maxWait;
  }

  protected void setPool(WriterPool pool) {
    this.pool = pool;
  }

  protected WriterPool getPool() {
    return this.pool;
  }

  protected long getTotalBytesWritten() {
    return this.totalBytesWritten;
  }

  protected void setTotalBytesWritten(final long b) {
    this.totalBytesWritten = b;
  }

  protected ProcessResult innerProcessResult(final ProcessorURI puri) {
    ProcessorURI curi = puri;
    long recordLength = getRecordedSize(curi);
    ReplayInputStream ris = null;
    try {
      if (shouldWrite(curi)) {
        ris = curi.getRecorder().getRecordedInput().getReplayInputStream();
        return write(curi, recordLength, ris, getHostAddress(curi));
      }
      logger.info("does not write " + curi.toString());
    } catch (IOException e) {
      curi.getNonFatalFailures().add(e);
      logger.log(Level.SEVERE, "Failed write of Record: " +
          curi.toString(), e);
    } finally {
      IoUtils.close(ris);
    }
    return ProcessResult.PROCEED;
  }

  /**
   * Return IP address of given URI suitable for recording (as in a
   * classic ARC 5-field header line).
   * 
   * @param curi ProcessorURI
   * @return String of IP address
   */
  protected String getHostAddress(ProcessorURI curi) {
    // special handling for DNS URIs: want address of DNS server
    if (curi.getUURI().getScheme().toLowerCase().equals("dns")) {
      return (String)curi.getData().get(ModuleAttributeConstants.A_DNS_SERVER_IP_LABEL);
    }
    // otherwise, host referenced in URI
    CrawlHost h = ServerCacheUtil.getHostFor(serverCache, curi.getUURI());
    if (h == null) {
      throw new NullPointerException("Crawlhost is null for " +
          curi + " " + curi.getVia());
    }
    InetAddress a = h.getIP();
    if (a == null) {
      throw new NullPointerException("Address is null for " +
          curi + " " + curi.getVia() + ". Address " +
          ((h.getIpFetched() == CrawlHost.IP_NEVER_LOOKED_UP)?
              "was never looked up.":
                (System.currentTimeMillis() - h.getIpFetched()) +
                " ms ago."));
    }
    return h.getIP().getHostAddress();
  }

  /**
   * Whether the given ProcessorURI should be written to archive files.
   * Annotates ProcessorURI with a reason for any negative answer.
   * 
   * @param curi ProcessorURI
   * @return true if URI should be written; false otherwise
   */
  protected boolean shouldWrite(ProcessorURI curi) {
    boolean retVal;
    String scheme = curi.getUURI().getScheme().toLowerCase();
    if (scheme.equals("dns")) {
      retVal = curi.getFetchStatus() == FetchStatusCodes.S_DNS_SUCCESS;
    } else if (scheme.equals("http") || scheme.equals("https")) {
      retVal = curi.getFetchStatus() > 0 && curi.getHttpMethod() != null;
    } else if (scheme.equals("ftp")) {
      retVal = curi.getFetchStatus() == 200;
    } else {
      curi.getAnnotations().add("unwritten:scheme");
      return false;
    }

    if (retVal == false) {
      // status not deserving writing
      curi.getAnnotations().add("unwritten:status");
      return false;
    }
    return true;
  }

  protected ProcessResult write(final ProcessorURI curi,
      @SuppressWarnings("unused") long recordLength, 
      @SuppressWarnings("unused") InputStream in,
      @SuppressWarnings("unused") String ip)
  throws IOException {
    WriterPoolMember writer = getPool().borrowFile();
    long position = writer.getPosition();
    HBaseWriter w = (HBaseWriter)writer;
    try {
      w.write(curi, getHostAddress(curi),
          curi.getRecorder().getRecordedOutput(),
          curi.getRecorder().getRecordedInput());
    } catch (IOException e) {
      writer = null;
      throw e;
    } finally {
      if (writer != null) {
        setTotalBytesWritten(getTotalBytesWritten() +
            (writer.getPosition() - position));
        getPool().returnFile(writer);
      }
    }
    return checkBytesWritten(curi);
  }

  protected ProcessResult checkBytesWritten(StateProvider context) {
    long max = context.get(this, TOTAL_BYTES_TO_WRITE).longValue();
    if (max <= 0) {
      return ProcessResult.PROCEED;
    }
    if (max <= this.totalBytesWritten) {
      return ProcessResult.FINISH; // FIXME: Specify reason
      //          controller.requestCrawlStop(CrawlStatus.FINISHED_WRITE_LIMIT);
    }   
    return ProcessResult.PROCEED;
  }


  protected void innerProcess(@SuppressWarnings("unused") ProcessorURI puri) {
    throw new AssertionError();
  }  

  // good to keep at end of source: must run after all per-Key 
  // initialization values are set.
  static {
    KeyManager.addKeys(HBaseWriterProcessor.class);
  }

  public void close() {
    this.pool.close();
  }

  @Override
  protected boolean shouldProcess(ProcessorURI uri) {
    ProcessorURI curi = uri;
    // If failure, or we haven't fetched the resource yet, return
    if (curi.getFetchStatus() <= 0) {
      return false;
    }   

    // If no recorded content at all, don't write record.
    long recordLength = curi.getContentSize();
    if (recordLength <= 0) {
      // getContentSize() should be > 0 if any material (even just
      // HTTP headers with zero-length body is available.
      return false;
    }

    return true;
  }
}