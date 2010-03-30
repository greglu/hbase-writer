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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.archive.io.DefaultWriterPoolSettings;
import org.archive.io.WriterPool;


/**
 * @author stack
 */
public class HBaseWriterPool extends WriterPool {

    /**
     * Constructor.
     * 
     * @param zkQuorum the list of zookeeper quorum servers that serve HBase, comma seperated.
     * e.g.:  zkHost1,zkHost2,zkHost3
     * @param zkClientPort the port that clients should connect to on the given zk quorum servers.
     * e.g.:  2181
     * @param table the table name in HBase
     * @param parameters an HBaseParameters object consisting of parameters list
     * @param poolMaximumActive the maximum number of writers in the writer pool.
     * @param poolMaximumWait the maximum waittime for all writers in the pool.
     */
    public HBaseWriterPool(final String zkQuorum, final int zkClientPort, final String table, final HBaseParameters parameters, final int poolMaximumActive, final int poolMaximumWait) {
        // Below is hard to follow. Its invocation of this classes super
        // constructor passing a serial, an instance of BasePoolable.. that
        // is defined in line, followed by settings, max and wait.
        super(
            // a serial 
            new AtomicInteger(), 
            // Poolable factory
            new BasePoolableObjectFactory() {
                public Object makeObject() throws Exception {
                    return new HBaseWriter(zkQuorum, zkClientPort, table, parameters);
                }

                public void destroyObject(Object hbaseWriter) throws Exception {
                    ((HBaseWriter) hbaseWriter).close();
                    super.destroyObject(hbaseWriter);
                }
            },
            // default writer pool settings...
            new DefaultWriterPoolSettings(), 
            // maximum active writers in the writer pool
            poolMaximumActive,
            // maximum wait time
            poolMaximumWait);
    }
}
