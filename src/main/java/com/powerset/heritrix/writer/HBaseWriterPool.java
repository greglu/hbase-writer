/* HBaseWriterPool
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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;


/**
 * A pool of HBaseWriters.
 * @author stack
 */
public class HBaseWriterPool extends WriterPool {
    /**
     * Constructor
     *
     * @param settings Settings for this pool.
     * @param poolMaximumActive
     * @param poolMaximumWait
     */
    public HBaseWriterPool(final WriterPoolSettings settings,
            final int poolMaximumActive, final int poolMaximumWait) {
        this(null, settings, poolMaximumActive, poolMaximumWait);
    }

    /**
     * Constructor
     *
     * @param serial  Used to generate unique filename sequences
     * @param settings Settings for this pool.
     * @param poolMaximumActive
     * @param poolMaximumWait
     */
    public HBaseWriterPool(final AtomicInteger serial,
            final WriterPoolSettings settings, final int poolMaximumActive,
            final int poolMaximumWait) {
        // Below is hard to follow.  Its invocation of this classes super
        // constructor passing a serial, an instance of BasePoolable.. that
        // is defined in line, followed by settings, max and wait.
        super(serial, new BasePoolableObjectFactory() {
            public Object makeObject() throws Exception {
                HBaseWriterPoolSettings hbaseSettings =
                    (HBaseWriterPoolSettings)settings;
                return new HBaseWriter(hbaseSettings.getMaster(),
                    hbaseSettings.getTable());
            }

            public void destroyObject(Object arcWriter)
            throws Exception {
                ((WriterPoolMember)arcWriter).close();
                super.destroyObject(arcWriter);
            }
        }, settings, poolMaximumActive, poolMaximumWait);
    }
}
