package org.archive.modules.writer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Keying;
import org.apache.log4j.Logger;
import org.archive.modules.writer.WriterPoolProcessor;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.io.hbase.HBaseParameters;
import org.archive.io.hbase.HBaseWriter;
import org.archive.io.hbase.HBaseWriterPool;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.util.ArchiveUtils;


/**
 * A <a href="http://crawler.archive.org">Heritrix 3</a> processor that writes
 * to <a href="http://hbase.org">Hadoop HBase</a>.
 *
 * The following example shows how to configure the crawl job configuration.
 *
 * <pre>
 * {@code
 * <!-- DISPOSITION CHAIN -->
 * <bean id="hbaseParameterSettings" class="org.archive.io.hbase.HBaseParameters">
 *   <property name="contentColumnFamily" value="newcontent" />
 *   <!-- Overwrite more options here -->
 * </bean>
 *
 * <bean id="hbaseWriterProcessor" class="org.archive.modules.writer.HBaseWriterProcessor">
 *   <property name="zkQuorum" value="localhost" />
 *   <property name="zkClientPort" value="2181" />
 *   <property name="hbaseTable" value="crawl" />
 *   <property name="hbaseParameters">
 *     <bean ref="hbaseParameterSettings" />
 *   </property>
 * </bean>
 *
 * <bean id="dispositionProcessors" class="org.archive.modules.DispositionChain">
 *   <property name="processors">
 *     <list>
 *     <!-- write to aggregate archival files... -->
 *     <ref bean="hbaseWriterProcessor"/>
 *     <!-- other references -->
 *     </list>
 *   </property>
 * </bean>
 * }
 * </pre>
 *
 * @see org.archive.io.hbase.HBaseParameters {@link org.archive.io.hbase.HBaseParameters}
 *  for defining hbaseParameters
 *
 */
public class HBaseWriterProcessor extends WriterPoolProcessor {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    private static final long serialVersionUID = 7019522841438703184L;

    /** HBase specific attributes **/
    private String zkQuorum;            // shouldn't be defaulted
    private int zkClientPort = 2181;    // Default port for ZK
    private String hbaseTable;          // shouldn't be defaulted

    /**
     * @see org.archive.io.hbase.HBaseParameters
     */
    HBaseParameters hbaseParameters = null;

    /** If set to true, then only write urls that are new rowkey records. 
     *  Default is false, which will write all urls to the HBase table. 
     * Heritrix is good about not hitting the same url twice, so this feature 
     * is to ensure that you can run multiple sessions of the same crawl 
     * configuration and not write the same url more than once to the same 
     * hbase table. You may just want to crawl a site to see what new urls have 
     * been added over time, or continue where you left off on a terminated 
     * crawl.  Heritrix itself does support this functionalty by supporting 
     * "Checkpoints" during a crawl session, so this may not be a necessary 
     * option.
     */
    private boolean onlyWriteNewRecords = false;

    /** If set to true, then only process urls that are new rowkey records. 
     * Default is false, which will process all urls to the HBase table. 
     * In this mode, Heritrix wont even fetch and parse the content served at 
     * the url if it already exists as a rowkey in the HBase table. 
     */
    private boolean onlyProcessNewRecords = false;

    
    /** Getters and setters **/

    public String getZkQuorum() {
        return zkQuorum;
    }
    public void setZkQuorum(String zkQuorum) {
        LOG.info("ZkQuorum: " + zkQuorum);
        this.zkQuorum = zkQuorum;
    }

    public int getZkClientPort() {
        return zkClientPort;
    }
    public void setZkClientPort(int zkClientPort) {
        LOG.info("ZkClientPort: " + zkClientPort);
        this.zkClientPort = zkClientPort;
    }

    public String getHbaseTable() {
        return hbaseTable;
    }
    public void setHbaseTable(String hbaseTable) {
        LOG.info("HBaseTable: " + hbaseTable);
        this.hbaseTable = hbaseTable;
    }

    public synchronized HBaseParameters getHbaseParameters() {
        if (hbaseParameters == null)
            this.hbaseParameters = new HBaseParameters();

        return hbaseParameters;
    }
    public void setHbaseParameters(HBaseParameters options) {
        this.hbaseParameters = options;
    }

    public boolean onlyWriteNewRecords() {
        return onlyWriteNewRecords;
    }
    public void setOnlyWriteNewRecords(boolean onlyWriteNewRecords) {
        this.onlyWriteNewRecords = onlyWriteNewRecords;
    }

    public boolean onlyProcessNewRecords() {
        return onlyProcessNewRecords;
    }
    public void setOnlyProcessNewRecords(boolean onlyProcessNewRecords) {
        this.onlyProcessNewRecords = onlyProcessNewRecords;
    }

    /** End of Getters and Setters **/

    @Override
    long getDefaultMaxFileSize() {
        return (20 * 1024 * 1024);
    }

    @Override
    List<String> getDefaultStorePaths() {
        return new ArrayList<String>();
    }

    @Override
    protected List<String> getMetadata() {
        return new ArrayList<String>();
    }

    @Override
    protected void setupPool(AtomicInteger serial) {
        setPool(new HBaseWriterPool(getZkQuorum(), getZkClientPort(), getHbaseTable(), getHbaseParameters(), getPoolMaxActive(), getPoolMaxWaitMs()));
    }

    @Override
    protected ProcessResult innerProcessResult(CrawlURI uri) {
        CrawlURI curi = uri;
        long recordLength = getRecordedSize(curi);
        ReplayInputStream ris = null;
        try {
            if (shouldWrite(curi)) {
                ris = curi.getRecorder().getRecordedInput().getReplayInputStream();
                return write(curi, recordLength, ris);
            }
            LOG.info("Does not write " + curi.toString());
        } catch (IOException e) {
            curi.getNonFatalFailures().add(e);
            LOG.error("Failed write of Record: " + curi.toString(), e);
        } finally {
            ArchiveUtils.closeQuietly(ris);
        }
        return ProcessResult.PROCEED;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.ProcessorURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        // The old method is still checked, but only continue with the next
        // checks if it returns true.
        if (!super.shouldProcess(curi))
            return false;

        // If onlyProcessNewRecords is enabled and the given rowkey has cell data,
        // don't write the record.
        if (onlyProcessNewRecords()) {
            return isRecordNew(curi);
        }

        // If we make it here, then we passed all our checks and we can assume
        // we should write the record.
        return true;
    }

    /**
     * Whether the given CrawlURI should be written to archive files.
     * Annotates CrawlURI with a reason for any negative answer.
     *
     * @param curi CrawlURI
     *
     * @return true if URI should be written; false otherwise
     */
    protected boolean shouldWrite(CrawlURI curi) {
        // The old method is still checked, but only continue with the next
        // checks if it returns true.
        if (!super.shouldWrite(curi))
            return false;

        // If the content exceeds the maxContentSize, then dont write.
        if (curi.getContentSize() > getMaxFileSizeBytes()) {
            // content size is too large
            curi.getAnnotations().add(ANNOTATION_UNWRITTEN + ":size");
            LOG.warn("Content size for " + curi.getUURI() + " is too large ("
                    + curi.getContentSize() + ") - maximum content size is: "
                    + getMaxFileSizeBytes());
            return false;
        }

        // If onlyWriteNewRecords is enabled and the given rowkey has cell data,
        // don't write the record.
        if (onlyWriteNewRecords()) {
            return isRecordNew(curi);
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
    private boolean isRecordNew(CrawlURI curi) {
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
     * Write to HBase.
     * 
     * @param curi the curi
     * @param recordLength the record length
     * @param in the in
     * 
     * @return the process result
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected ProcessResult write(final CrawlURI curi, long recordLength, InputStream in) throws IOException {
        WriterPoolMember writerPoolMember = getPool().borrowFile();
        long writerPoolMemberPosition = writerPoolMember.getPosition();
        HBaseWriter hbaseWriter = (HBaseWriter) writerPoolMember;
        try {
            hbaseWriter.write(curi, getHostAddress(curi), curi.getRecorder().getRecordedOutput(), curi.getRecorder().getRecordedInput());
        } finally {
            setTotalBytesWritten(getTotalBytesWritten() + (writerPoolMember.getPosition() - writerPoolMemberPosition));
            getPool().returnFile(writerPoolMember);
        }
        return checkBytesWritten();
    }

}