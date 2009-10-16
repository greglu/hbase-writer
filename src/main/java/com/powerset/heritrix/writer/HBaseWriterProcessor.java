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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
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
public class HBaseWriterProcessor extends Processor implements Initializable, Closeable {
	
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 7166781798179114353L;

	/** The LOG. */
	private static final Logger LOG = Logger.getLogger("HBaseWriterProcessor");

	/** Commas-seperated list of Hostnames in the zookeeper quorum. */
	@Immutable
	public static final Key<String> ZKQUORUM = Key.make("localhost");

	/** The port that clients should connect on to contact their zk quorum hsots. */
	@Immutable
	public static final Key<Integer> ZKCLIENTPORT = Key.make(2181);
	
	/** HBase tableName to crawl into. */
	@Immutable
	public static final Key<String> TABLE = Key.make("crawl");

	/** 
	 * If set to true, then only write urls that are new rowkey records. Default is false, which will write all urls to the HBase table.  
	 * Heritrix is good about not hitting the same url twice, so this feature is to ensure that you can run multiple sessions 
	 * of the same crawl configuration and not write the same url more than once to the same hbase table. You may just want to crawl a site to 
	 * see what new urls have been added over time, or continue where you left off on a terminated crawl.  Heritrix itself does support this
	 * functionalty by supporting "Checkpoints" during a crawl session, so this may not be a necessary option. 
	 */
	@Immutable
	public static final Key<Boolean> WRITE_ONLY_NEW_RECORDS = Key.make(false);
	
	/** 
	 * If set to true, then only process urls that are new rowkey records. Default is false, which will process all urls to the HBase table.  
	 * In this mode, Heritrix wont even fetch and parse the content served at the url if it already exists as a rowkey in the HBase table. 
	 */
	@Immutable
	public static final Key<Boolean> PROCESS_ONLY_NEW_RECORDS = Key.make(false);

	/** Maximum active files in pool. This setting cannot be varied over the life of a crawl. */
	@Immutable
	final public static Key<Integer> POOL_MAX_ACTIVE = Key.make(WriterPool.DEFAULT_MAX_ACTIVE);

	/** Maximum time to wait on pool element (milliseconds). This setting cannot be varied over the life of a crawl. */
	@Immutable
	final public static Key<Integer> POOL_MAX_WAIT = Key.make(WriterPool.DEFAULT_MAXIMUM_WAIT);

	/** The Constant SERVER_CACHE. */
	@Immutable
	final public static Key<ServerCache> SERVER_CACHE = Key.makeAuto(ServerCache.class);

	/** Maximum allowable content size. */
	@Immutable
	final public static Key<Integer> CONTENT_MAX_SIZE = Key.make(20 * 1024 * 1024);

	/** Total file bytes to write to disk. Once the size of all files on disk has exceeded this limit, this processor will stop the crawler. A value of zero means no upper limit. */
	@Immutable
	@Expert
	final public static Key<Long> TOTAL_BYTES_TO_WRITE = Key.make(0L);

	/** Reference to pool. */
	private transient WriterPool pool = null;
	
	/** The server cache. */
	private transient ServerCache serverCache;
	
	/** The max active. */
	private int maxActive;
	
	/** The max wait. */
	private int maxWait;
	
	/** The max content size. */
	private int maxContentSize;
	
	/** The comma seperated string of hostnames that make up the zookeeper quorum. */
	private String zkQuorum;
	
	/** The port that zk clients should connect to for information. */
	private int zkClientPort;
	
	/** The hbase table name. */
	private String tableName;
	
	/** The only write new records turnable. */
	private boolean onlyWriteNewRecords;
	
	/** The only process new records turnable. */
	private boolean onlyProcessNewRecords;

	/** The total bytes written to disk. */
	private long totalBytesWritten = 0;

	static {
		KeyManager.addKeys(HBaseWriterProcessor.class);
	}

	/**
	 * Instantiates a new HBaseWriterProcessor.
	 */
	public HBaseWriterProcessor() {
		super();
	}

	/* (non-Javadoc)
	 * @see org.archive.state.Initializable#initialTasks(org.archive.state.StateProvider)
	 */
	public synchronized void initialTasks(StateProvider context) {
		this.serverCache = context.get(this, SERVER_CACHE);
		this.maxActive = context.get(this, POOL_MAX_ACTIVE).intValue();
		this.maxWait = context.get(this, POOL_MAX_WAIT).intValue();
		this.zkQuorum = context.get(this, ZKQUORUM);
		this.zkClientPort = context.get(this, ZKCLIENTPORT);
		this.tableName = context.get(this, TABLE);
		this.onlyWriteNewRecords = context.get(this, WRITE_ONLY_NEW_RECORDS).booleanValue();
		this.onlyProcessNewRecords = context.get(this, PROCESS_ONLY_NEW_RECORDS).booleanValue();
		this.maxContentSize = context.get(this, CONTENT_MAX_SIZE).intValue();
		setupPool();
	}

	/* (non-Javadoc)
	 * @see org.archive.modules.Processor#innerProcessResult(org.archive.modules.ProcessorURI)
	 */
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
	 * @param curi ProcessorURI
	 * 
	 * @return String of IP address
	 */
	protected String getHostAddress(ProcessorURI curi) {
		// special handling for DNS URIs: want address of DNS server
		if (curi.getUURI().getScheme().equalsIgnoreCase("dns")) {
			return (String) curi.getData().get(
					ModuleAttributeConstants.A_DNS_SERVER_IP_LABEL);
		}
		// otherwise, host referenced in URI
		CrawlHost crawlHost = ServerCacheUtil.getHostFor(serverCache, curi.getUURI());
		if (crawlHost == null) {
			throw new NullPointerException("Crawlhost is null for " + curi + " " + curi.getVia());
		}
		// check if the ip address was looked up.
		if (crawlHost.getIP() == null) {
			throw new NullPointerException(
					"Address is null for "
							+ curi
							+ " "
							+ curi.getVia()
							+ ". Address "
							+ ((crawlHost.getIpFetched() == CrawlHost.IP_NEVER_LOOKED_UP) ? "was never looked up."
									: (System.currentTimeMillis() - crawlHost.getIpFetched()) 
							+ " ms ago."));
		}
		return crawlHost.getIP().getHostAddress();
	}

	/* (non-Javadoc)
	 * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.ProcessorURI)
	 */
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
		
		// If onlyProcessNewRecords is enabled and the given rowkey has cell data,
		// don't write the record.
		if (this.onlyProcessNewRecords) {
			return this.isRecordNew(curi);
		}
		// If we make it here, then we passed all our checks and we can assume
		// we should write the record.
		return true;
	}
	
	/**
	 * Whether the given ProcessorURI should be written to archive files.
	 * Annotates ProcessorURI with a reason for any negative answer.
	 * 
	 * @param curi ProcessorURI
	 * 
	 * @return true if URI should be written; false otherwise
	 */
	protected boolean shouldWrite(ProcessorURI curi) {
		boolean retVal;
		String scheme = curi.getUURI().getScheme();
		// determine the return value of the uri request
		if (scheme.equalsIgnoreCase("dns")) {
			retVal = curi.getFetchStatus() == FetchStatusCodes.S_DNS_SUCCESS;
		} else if (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https")) {
			retVal = curi.getFetchStatus() > 0 && curi.getHttpMethod() != null;
		} else if (scheme.equalsIgnoreCase("ftp")) {
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
			return this.isRecordNew(curi);
		}
		// all tests pass, return true to write the content locally.
		return true;
	}
	
	/**
	 * Determine if the given uri exists as a rowkey in the configured hbase table.
	 * 
	 * @param curi the curi
	 * 
	 * @return true, if checks if is record new
	 */
	private boolean isRecordNew(ProcessorURI curi) {
		WriterPoolMember writerPoolMember;
		try {
			writerPoolMember = getPool().borrowFile();
		} catch (IOException e1) {
			LOG.error("No writer could be borrowed from the pool: " + getPool().toString() 
							+ " - exception is: \n" + e1.getMessage());
			return false;
		}
		HTable hbaseTable = ((HBaseWriter) writerPoolMember).getClient();
		// Here we can generate the rowkey for this uri ...
		String url = curi.toString();
		String row = Keying.createKey(url);
		try {
			// and look it up to see if it already exists...
			Get rowToGet = new Get(Bytes.toBytes(row));
			if (hbaseTable.get(rowToGet) != null && !hbaseTable.get(rowToGet).isEmpty()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Not A NEW Record - Url: "
								+ url
								+ " has the existing rowkey: "
								+ row
								+ " and has cell data.");
				}
				return false;
			}
		} catch (IOException e) {
			LOG.error("Failed to determine if record: "
							+ row
							+ " is a new record due to IOExecption.  Deciding the record is already existing for now. \n"
							+ e.getMessage());
			return false;
		} finally {
			try {
				getPool().returnFile(writerPoolMember);
			} catch (IOException e) {
				LOG.error("Failed to add back writer to the pool after checking if a rowkey is new or existing: "
								+ row + "\n" + e.getMessage());
				return false;
			}
		}
		return true;
	}

	/**
	 * Write.
	 * 
	 * @param curi the curi
	 * @param recordLength the record length
	 * @param in the in
	 * @param ip the ip
	 * 
	 * @return the process result
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected ProcessResult write(final ProcessorURI curi, long recordLength, InputStream in, String ip) throws IOException {
		WriterPoolMember writerPoolMember = getPool().borrowFile();
		long writerPoolMemberPosition = writerPoolMember.getPosition();
		HBaseWriter hbaseWriter = (HBaseWriter) writerPoolMember;
		try {
			hbaseWriter.write(curi, getHostAddress(curi), curi.getRecorder().getRecordedOutput(), curi.getRecorder().getRecordedInput());
		} finally {
			setTotalBytesWritten(getTotalBytesWritten() + (writerPoolMember.getPosition() - writerPoolMemberPosition));
			getPool().returnFile(writerPoolMember);
		}
		return checkBytesWritten(curi);
	}

	/**
	 * Check bytes written.
	 * 
	 * @param context the context
	 * 
	 * @return the process result
	 */
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

	/**
	 * Setup pool.
	 */
	protected void setupPool() {
		setPool(new HBaseWriterPool(getZKQuorum(), getZKClientPort(), getTable(), getMaxActive(), getMaxWait()));
	}

	/**
	 * Gets the zookeeper quorum.
	 * 
	 * @return the zkQuorum
	 */
	protected String getZKQuorum() {
		return this.zkQuorum;
	}

	/**
	 * Gets the zookeeper client port.
	 * 
	 * @return the zlClientPort
	 */
	protected int getZKClientPort() {
		return this.zkClientPort;
	}

	
	/**
	 * Gets the table.
	 * 
	 * @return the table
	 */
	protected String getTable() {
		return this.tableName;
	}

	
	/**
	 * Gets the max active.
	 * 
	 * @return the max active
	 */
	protected int getMaxActive() {
		return maxActive;
	}

	/**
	 * Gets the max wait.
	 * 
	 * @return the max wait
	 */
	protected int getMaxWait() {
		return maxWait;
	}

	/**
	 * Sets the pool.
	 * 
	 * @param pool the new pool
	 */
	protected void setPool(WriterPool pool) {
		this.pool = pool;
	}

	/**
	 * Gets the pool.
	 * 
	 * @return the pool
	 */
	protected WriterPool getPool() {
		return this.pool;
	}

	/**
	 * Gets the total bytes written.
	 * 
	 * @return the total bytes written
	 */
	protected long getTotalBytesWritten() {
		return this.totalBytesWritten;
	}

	/**
	 * Sets the total bytes written.
	 * 
	 * @param b the new total bytes written
	 */
	protected void setTotalBytesWritten(final long b) {
		this.totalBytesWritten = b;
	}
	
	/* (non-Javadoc)
	 * @see org.archive.modules.Processor#innerProcess(org.archive.modules.ProcessorURI)
	 */
	protected void innerProcess(ProcessorURI puri) {
		throw new AssertionError();
	}

	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	public void close() {
		this.pool.close();
	}

}
