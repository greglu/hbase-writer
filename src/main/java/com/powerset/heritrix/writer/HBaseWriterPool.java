/** HBaseWriterPool
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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.archive.io.DefaultWriterPoolSettings;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;

/**
 * A pool of HBaseWriters.
 */
public class HBaseWriterPool extends WriterPool {
	
	/**
	 * Constructor.
	 * 
	 * @param zkQuorum the list of zookeeper quorum servers that serve HBase, comma seperated.  
	 * 			i.e.:  zkHost1,zkHost2,zkHost3
	 * @param zkClientPort the port that clients should connect to on the given zk quorum servers.   
	 * 			i.e.:  2181
	 * @param table the table name in HBase
	 * @param poolMaximumActive the maximum number of writers in the writer pool.
	 * @param poolMaximumWait the maximum waittime for all writers in the pool.
	 */
	public HBaseWriterPool(final String zkQuorum, final int zkClientPort, final String table, final int poolMaximumActive, final int poolMaximumWait) {
		// Below is hard to follow. Its invocation of this classes super
		// constructor passing a serial, an instance of BasePoolable.. that
		// is defined in line, followed by settings, max and wait.
		super(
			// a serial	
			new AtomicInteger(), 
			// Poolable factory
			new BasePoolableObjectFactory() {
				public Object makeObject() throws Exception {
					return new HBaseWriter(zkQuorum, zkClientPort, table);
				}

				public void destroyObject(Object arcWriter) throws Exception {
					((WriterPoolMember) arcWriter).close();
					super.destroyObject(arcWriter);
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