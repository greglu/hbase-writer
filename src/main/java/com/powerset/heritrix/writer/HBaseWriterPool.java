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

// TODO: Auto-generated Javadoc
/**
 * A pool of HBaseWriters.
 */
public class HBaseWriterPool extends WriterPool {
	
	/**
	 * Constructor.
	 * 
	 * @param poolMaximumActive the pool maximum active
	 * @param poolMaximumWait the pool maximum wait
	 * @param master the master
	 * @param table the table
	 */
	public HBaseWriterPool(final String master, final String table, final int poolMaximumActive, final int poolMaximumWait) {
		// Below is hard to follow. Its invocation of this classes super
		// constructor passing a serial, an instance of BasePoolable.. that
		// is defined in line, followed by settings, max and wait.
		super(new AtomicInteger(), new BasePoolableObjectFactory() {
			public Object makeObject() throws Exception {
				return new HBaseWriter(master, table);
			}

			public void destroyObject(Object arcWriter) throws Exception {
				((WriterPoolMember) arcWriter).close();
				super.destroyObject(arcWriter);
			}
		}, new DefaultWriterPoolSettings(), poolMaximumActive, poolMaximumWait);
	}
}