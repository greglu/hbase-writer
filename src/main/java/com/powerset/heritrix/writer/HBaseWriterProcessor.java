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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.HConstants;
import org.archive.crawler.datamodel.CoreAttributeConstants;
import org.archive.crawler.datamodel.CrawlURI;
import org.archive.crawler.datamodel.FetchStatusCodes;
import org.archive.crawler.framework.WriterPoolProcessor;
import org.archive.crawler.settings.SimpleType;
import org.archive.crawler.settings.Type;
import org.archive.io.WriterPoolMember;
import org.archive.io.arc.ARCConstants;


/**
 * HBase writer.
 * @author stack
 */
public class HBaseWriterProcessor extends WriterPoolProcessor
implements HBaseWriterPoolSettings {
    private final Logger logger = Logger.getLogger(this.getClass().getName());
    
    /***
     * HBase master
     */
    public static final String ATTR_MASTER = "hbase-master";
   
    private static final String DEFAULT_MASTER =
        HConstants.DEFAULT_MASTER_ADDRESS;
    
    /***
     * Key for the HBase table
     */
    public static final String ATTR_TABLE = "hbase-table";
    
    private static final String DEFAULT_TABLE = "crawl";

    /**
     * @param name Name of this writer.
     */
    public HBaseWriterProcessor(String name) {
        this(name, "HBaseWriter processor");
    }
      
    /**
     * @param name Name of this processor.
     * @param description Description for this processor.
     */
    public HBaseWriterProcessor(final String name, final String description) {
        super(name, description);
        Type e = addElementToDefinition(new SimpleType(ATTR_MASTER,
            "HBase master node. Specify as host:port", DEFAULT_MASTER));
        e.setOverrideable(false);
        e = addElementToDefinition(new SimpleType(ATTR_TABLE,
            "Table to write crawls to", DEFAULT_TABLE));
        e.setOverrideable(false);
  
        // these do not apply ...
        removeElementFromDefinition(ATTR_PATH);
        removeElementFromDefinition(ATTR_COMPRESS);
        removeElementFromDefinition(ATTR_PREFIX);
        removeElementFromDefinition(ATTR_SUFFIX);
        removeElementFromDefinition(ATTR_MAX_SIZE_BYTES);
    }

    public String getMaster() {
        Object obj = getAttributeUnchecked(ATTR_MASTER);
        return (obj == null)? DEFAULT_MASTER: (String)obj;
    }

    public String getTable() {
        Object obj = getAttributeUnchecked(ATTR_TABLE);
        return (obj == null)? DEFAULT_TABLE: (String)obj;
    }

    protected void setupPool(final AtomicInteger serialNo) {
      setPool(new HBaseWriterPool(serialNo, this, getPoolMaximumActive(),
            getPoolMaximumWait()));
    }
    
    /**
     * Writes a CrawlURI and its associated data to hbase.
     * @param curi CrawlURI to process.
     */
    protected void innerProcess(CrawlURI curi) {
        // If failure, or we haven't fetched the resource yet, return
        if (curi.getFetchStatus() <= 0) {
            return;
        }
        
        // If no content, don't write record.
        int recordLength = (int)curi.getContentSize();
        if (recordLength <= 0) {
        	// Write nothing.
        	return;
        }
        
        try {
          write(curi);
        } catch (IOException e) {
            curi.addLocalizedError(this.getName(), e, "WriteRecord: " +
                curi.toString());
            logger.log(Level.SEVERE, "Failed write of Record: " +
                curi.toString(), e);
        }
    }
    
    protected void write(CrawlURI curi) throws IOException {
        WriterPoolMember writer = getPool().borrowFile();
        long position = writer.getPosition();
        HBaseWriter w = (HBaseWriter)writer;
        try {
            w.write(curi, getHostAddress(curi),
                    curi.getHttpRecorder().getRecordedOutput(),
                    curi.getHttpRecorder().getRecordedInput());
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
        checkBytesWritten();
    }
}