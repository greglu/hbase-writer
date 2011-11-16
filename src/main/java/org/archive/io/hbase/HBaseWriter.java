/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.archive.io.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Keying;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.modules.CrawlURI;

/**
 * HBase implementation.
 *
 */
public class HBaseWriter {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    private HBaseParameters hbaseOptions;
    private final HTable client;

    /**
     * @see org.archive.io.hbase.HBaseParameters
     */
    public HBaseParameters getHbaseOptions() {
        return hbaseOptions;
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
     * Instantiates a new HBaseWriter for the WriterPool to use in heritrix.
     * 
     * @param zkQuorum the zookeeper quorum. The list of hosts that make up you zookeeper quorum.
     * i.e.:  zkHost1,zkHost2,zkHost3
     * @param zkClientPort the zookeeper client port that clients should try to connect on for
     * servers in the zk quorum.  This value is analgous to the hase-site.xml config parameter:
     * hbase.zookeeper.property.clientPort
     * @param tableName the table in hbase to write to.  i.e. : webtable
     * @param parameters an HBaseParameters object consisting of parameters list
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public HBaseWriter(final String zkQuorum, final int zkClientPort, final String tableName, HBaseParameters parameters)  throws IOException{
        this.hbaseOptions = parameters;

        if (tableName == null || tableName.length() <= 0) {
            throw new IllegalArgumentException("Must specify a table name");
        }
        Configuration hbaseConfiguration = HBaseConfiguration.create();

        // set the zk quorum list
        if (zkQuorum != null && zkQuorum.length() > 0) {
            LOG.info("setting zookeeper quorum to : " + zkQuorum);
            hbaseConfiguration.setStrings(HConstants.ZOOKEEPER_QUORUM, zkQuorum.split(","));
        }

        // set the client port
        if (zkClientPort > 0) {
            LOG.info("setting zookeeper client Port to : " + zkClientPort);
            hbaseConfiguration.setInt(getHbaseOptions().getZookeeperClientPort(), zkClientPort);
        }

        // create a crawl table
        initializeCrawlTable(hbaseConfiguration, tableName);
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
    protected void initializeCrawlTable(final Configuration hbaseConfiguration, final String hbaseTableName) throws IOException {
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
                if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(getHbaseOptions().getContentColumnFamily())) {
                    foundContentColumnFamily = true;
                } else if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(getHbaseOptions().getCuriColumnFamily())) {
                    foundCURIColumnFamily = true;
                }
            }

            // modify the table if it's missing any of the column families.
            if (!foundContentColumnFamily || !foundCURIColumnFamily) {
                LOG.info("Disabling table: " + hbaseTableName);
                hbaseAdmin.disableTable(hbaseTableName);

                if (!foundContentColumnFamily) {
                    LOG.info("Adding column to table: " + hbaseTableName + " column: " + getHbaseOptions().getContentColumnFamily());
                    existingHBaseTable.addFamily(new HColumnDescriptor(getHbaseOptions().getContentColumnFamily()));     
                }

                if (!foundCURIColumnFamily) {
                    LOG.info("Adding column to table: " + hbaseTableName + " column: " + getHbaseOptions().getCuriColumnFamily());
                    existingHBaseTable.addFamily(new HColumnDescriptor(getHbaseOptions().getCuriColumnFamily()));    
                }

                hbaseAdmin.modifyTable(Bytes.toBytes(hbaseTableName), existingHBaseTable);

                LOG.info("Enabling table: " + hbaseTableName);
                hbaseAdmin.enableTable(hbaseTableName); 
            }
            LOG.debug("Done checking table: " + hbaseTableName);
        } else {
            // create a new hbase table
            LOG.info("Creating table " + hbaseTableName);
            HTableDescriptor newHBaseTable = new HTableDescriptor(hbaseTableName);
            newHBaseTable.addFamily(new HColumnDescriptor(getHbaseOptions().getContentColumnFamily()));
            newHBaseTable.addFamily(new HColumnDescriptor(getHbaseOptions().getCuriColumnFamily()));

            // create the table
            hbaseAdmin.createTable(newHBaseTable);
            LOG.info("Created table " + newHBaseTable.toString());
        }
    }

    /**
     * Read the ReplayInputStream and write it to the given BatchUpdate with the given column.
     * 
     * @param replayInputStream the ris the cell data as a replay input stream
     * @param streamSize the size
     * 
     * @return the byte array from input stream
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
     * update object. Or even saving the values in other custom hbase tables 
     * or other remote data sources. (a.k.a. anything you want)
     * 
     * @param put the stateful put object containing all the row data to be written.
     * @param replayInputStream the replay input stream containing the raw content gotten by heritrix crawler.
     * @param streamSize the stream size
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected void processContent(Put put, ReplayInputStream replayInputStream, int streamSize) throws IOException {
        // Below is just an example of a typical use case of overriding this method.
        // I.E.: The goal below is to process the raw content array and parse it to a new byte array.....
        // byte[] rowKey = put.getRow();
        // byte[] rawContent = this.getByteArrayFromInputStream(replayInputStream, streamSize)
        // // process rawContent and create output to store in new columns. 
        // byte[] someParsedByteArray = userDefinedMethondToProcessRawContent(rawContent);
        // put.add(Bytes.toBytes("some_column_family"), Bytes.toBytes("a_new_column_name"), someParsedByteArray);
    }

    /**
     * Write the crawled output to the configured HBase table.
     * Write each row key as the url with reverse domain and optionally process any content.
     * 
     * @param curi URI of crawled document
     * @param ip IP of remote machine.
     * @param recordingOutputStream recording input stream that captured the response
     * @param recordingInputStream recording output stream that captured the GET request
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void write(final CrawlURI curi, final String ip, final RecordingOutputStream recordingOutputStream, 
            final RecordingInputStream recordingInputStream) throws IOException {
        // generate the target url of the crawled document
        String url = curi.toString();

        // create the hbase friendly rowkey
        String rowKey = Keying.createKey(url);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Writing " + url + " as " + rowKey);
        }

        // create an hbase updateable object (the put object)
        // Constructor takes the rowkey as the only argument
        Put batchPut = new Put(Bytes.toBytes(rowKey));

        // write the target url to the url column
        batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getUrlColumnName()), curi.getFetchBeginTime(), Bytes.toBytes(url));
        // write the target ip to the ip column
        batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getIpColumnName()), curi.getFetchBeginTime(), Bytes.toBytes(ip));
        // is the url part of the seed url (the initial url(s) used to start the crawl)
        if (curi.isSeed()) {
            batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getIsSeedColumnName()), Bytes.toBytes(Boolean.TRUE.booleanValue()));
            if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0) {
                batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getPathFromSeedColumnName()), Bytes.toBytes(curi.getPathFromSeed().trim()));
            }
        }
        // write the Via string
        String viaStr = (curi.getVia() != null) ? curi.getVia().toString().trim() : null;
        if (viaStr != null && viaStr.length() > 0) {
            batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getViaColumnName()), Bytes.toBytes(viaStr));
        }
        // Write the Crawl Request to the Put object
        if (recordingOutputStream.getSize() > 0) {
            batchPut.add(Bytes.toBytes(getHbaseOptions().getCuriColumnFamily()), Bytes.toBytes(getHbaseOptions().getRequestColumnName()), 
                    getByteArrayFromInputStream(recordingOutputStream.getReplayInputStream(), (int) recordingOutputStream.getSize()));
        }
        // Write the Crawl Response to the Put object
        ReplayInputStream replayInputStream = recordingInputStream.getReplayInputStream();
        try {
            // add the raw content to the table record.
            batchPut.add(Bytes.toBytes(getHbaseOptions().getContentColumnFamily()), Bytes.toBytes(getHbaseOptions().getContentColumnName()),
                    getByteArrayFromInputStream(replayInputStream, (int) recordingInputStream.getSize()));
            // reset the input steam for the content processor.
            replayInputStream = recordingInputStream.getReplayInputStream();
            replayInputStream.setToResponseBodyStart();
            // process the content (optional)
            processContent(batchPut, replayInputStream, (int) recordingInputStream.getSize());
            // TODO: add an option to manually set the timestamp value of the batchPut object
            // Set crawl time as the timestamp to the Put object.
            //batchPut.setTimeStamp(curi.getFetchBeginTime());

            // write the Put object to the HBase table
            getClient().put(batchPut);
        } finally {
            IOUtils.closeStream(replayInputStream);
        }
    }
}