/**
 * HBaseWriterProcessor
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Keying;
import org.apache.log4j.Logger;
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
import org.archive.state.Initializable;
import org.archive.state.Key;
import org.archive.state.KeyManager;
import org.archive.state.StateProvider;
import org.archive.util.IoUtils;

/**
 * An <a href="http://crawler.archive.org">heritrix2</a> processor that writes
 * to <a href="http://hbase.org">Hadoop HBase</a>.
 */
public class HBaseWriterProcessor extends Processor implements Initializable,
		Closeable {
	private static final long serialVersionUID = 7166781798179114353L;

	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	/**
	 * Location of hbase master.
	 */
	@Immutable
	public static final Key<String> MASTER = Key.make(HConstants.DEFAULT_MASTER_ADDRESS);

	/**
	 * HBase tableName to crawl into.
	 */
	@Immutable
	public static final Key<String> TABLE = Key.make("crawl");

	/**
	 * If set to true, then only process urls that are new rowkey records.
	 * Default is false, to collect all urls.
	 * 
	 * Heritrix is good about not hitting the same url twice, so this feature is
	 * to ensure that you can run multiple sessions of the same crawl
	 * configuration and not fetch the same url more than once. You may just
	 * want to crawl a site to see what new urls have been added over time, or
	 * continue where you left off on a terminated crawl.
	 */
	@Immutable
	public static final Key<Boolean> ONLY_NEW_RECORDS = Key.make(false);

	/**
	 * Maximum active files in pool. This setting cannot be varied over the life
	 * of a crawl.
	 */
	@Immutable
	final public static Key<Integer> POOL_MAX_ACTIVE = Key.make(WriterPool.DEFAULT_MAX_ACTIVE);

	/**
	 * Maximum time to wait on pool element (milliseconds). This setting cannot
	 * be varied over the life of a crawl.
	 */
	@Immutable
	final public static Key<Integer> POOL_MAX_WAIT = Key.make(WriterPool.DEFAULT_MAXIMUM_WAIT);

	@Immutable
	final public static Key<ServerCache> SERVER_CACHE = Key.makeAuto(ServerCache.class);

	/**
	 * Maximum allowable content size.
	 */
	@Immutable
	final public static Key<Integer> CONTENT_MAX_SIZE = Key.make(20 * 1024 * 1024);

	/**
	 * Total file bytes to write to disk. Once the size of all files on disk has
	 * exceeded this limit, this processor will stop the crawler. A value of
	 * zero means no upper limit.
	 */
	@Immutable
	@Expert
	final public static Key<Long> TOTAL_BYTES_TO_WRITE = Key.make(0L);

	/**
	 * Reference to pool.
	 */
	private transient WriterPool pool = null;
	private ServerCache serverCache;
	private int maxActive;
	private int maxWait;
	private int maxContentSize;
	private String master;
	private String tableName;
	private boolean onlyWriteNewRecords;

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
		this.tableName = context.get(this, TABLE);
		this.onlyWriteNewRecords = context.get(this, ONLY_NEW_RECORDS).booleanValue();
		this.maxContentSize = context.get(this, CONTENT_MAX_SIZE).intValue();
		setupPool();
	}

	protected String getMaster() {
		return this.master;
	}

	protected String getTable() {
		return this.tableName;
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
			LOG.info("does not write " + curi.toString());
		} catch (IOException e) {
			curi.getNonFatalFailures().add(e);
			LOG.error("Failed write of Record: " + curi.toString(), e);
		} finally {
			IoUtils.close(ris);
		}
		return ProcessResult.PROCEED;
	}

	/**
	 * Return IP address of given URI suitable for recording (as in a classic
	 * ARC 5-field header line).
	 * 
	 * @param curi
	 *            ProcessorURI
	 * @return String of IP address
	 */
	protected String getHostAddress(ProcessorURI curi) {
		// special handling for DNS URIs: want address of DNS server
		if (curi.getUURI().getScheme().toLowerCase().equals("dns")) {
			return (String) curi.getData().get(
					ModuleAttributeConstants.A_DNS_SERVER_IP_LABEL);
		}
		// otherwise, host referenced in URI
		CrawlHost h = ServerCacheUtil.getHostFor(serverCache, curi.getUURI());
		if (h == null) {
			throw new NullPointerException("Crawlhost is null for " + curi + " " + curi.getVia());
		}
		InetAddress a = h.getIP();
		if (a == null) {
			throw new NullPointerException(
					"Address is null for "
							+ curi
							+ " "
							+ curi.getVia()
							+ ". Address "
							+ ((h.getIpFetched() == CrawlHost.IP_NEVER_LOOKED_UP) ? "was never looked up."
									: (System.currentTimeMillis() - h
											.getIpFetched())
											+ " ms ago."));
		}
		return h.getIP().getHostAddress();
	}

	/**
	 * Whether the given ProcessorURI should be written to archive files.
	 * Annotates ProcessorURI with a reason for any negative answer.
	 * 
	 * @param curi
	 *            ProcessorURI
	 * @return true if URI should be written; false otherwise
	 */
	protected boolean shouldWrite(ProcessorURI curi) {
		boolean retVal;
		String scheme = curi.getUURI().getScheme().toLowerCase();
		// determine the return value of the uri request
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

		// If the content exceeds the maxContentSize, then dont write.
		if (curi.getContentSize() > this.maxContentSize) {
			// content size is too large
			curi.getAnnotations().add("unwritten:size");
			LOG.warn("content size for " + curi.getUURI() + " is too large ("
					+ curi.getContentSize() + ") - maximum content size is: "
					+ this.maxContentSize);
			return false;
		}

		// If onlyWriteNewRecords is enabled and the given rowkey has cell data,
		// don't write the record.
		if (this.onlyWriteNewRecords) {
			WriterPoolMember writer;
			try {
				writer = getPool().borrowFile();
			} catch (IOException e1) {
				LOG.error("No writer could be gotten from the pool: " + getPool().toString() 
								+ " - exception is: \n" + e1.getMessage());
				return false;
			}
			HTable ht = ((HBaseWriter) writer).getClient();
			// Here we can generate the rowkey for this uri ...
			String url = curi.toString();
			String row = Keying.createKey(url);
			try {
				// and look it up to see if it already exists...
				if (ht.getRow(row) != null && !ht.getRow(row).isEmpty()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Not Writing "
										+ url
										+ " since rowkey: "
										+ row.toString()
										+ " already exists and onlyWriteNewRecords is enabled.");
					}
					return false;
				}
			} catch (IOException e) {
				LOG.error("Failed to determine if record: "
								+ row.toString()
								+ " should be written or not, deciding not to write the record: \n"
								+ e.getMessage());
				return false;
			} finally {
				try {
					getPool().returnFile(writer);
				} catch (IOException e) {
					LOG.error("Failed to add back writer to the pool after checking for existing rowkey: "
									+ row.toString() + "\n" + e.getMessage());
					return false;
				}
			}
		}
		// all tests pass, return true to write the content locally.
		return true;
	}

	protected ProcessResult write(final ProcessorURI curi, long recordLength, InputStream in, String ip) throws IOException {
		WriterPoolMember writer = getPool().borrowFile();
		long position = writer.getPosition();
		HBaseWriter w = (HBaseWriter) writer;
		try {
			w.write(curi, getHostAddress(curi), curi.getRecorder().getRecordedOutput(), curi.getRecorder().getRecordedInput());
		} finally {
			setTotalBytesWritten(getTotalBytesWritten() + (writer.getPosition() - position));
			getPool().returnFile(writer);
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
			// controller.requestCrawlStop(CrawlStatus.FINISHED_WRITE_LIMIT);
		}
		return ProcessResult.PROCEED;
	}

	protected void innerProcess(ProcessorURI puri) {
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

		// If we make it here, then we passed all our checks and we can assume
		// we should write the record.
		return true;
	}
}
