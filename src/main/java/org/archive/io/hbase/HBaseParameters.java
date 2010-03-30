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

import org.archive.io.ArchiveFileConstants;

/**
 * Configures the values of the column family/qualifier used
 * for the crawl. Also contains a full set of default values that
 * are the same as the previous Heritrix2 implementation.
 *
 * Meant to be configured within the Spring framework either inline
 * of HBaseWriterProcessor or as a named bean and references later on.
 *
 * <pre>
 * {@code
 * <bean id="hbaseParameterSettings" class="org.archive.io.hbase.HBaseParameters">
 *   <property name="contentColumnFamily" value="newcontent" />
 *   <!-- Overwrite more options here -->
 * </bean>
 * }
 * </pre>
 *
 * @see org.archive.modules.writer.HBaseWriterProcessor
 *  {@link org.archive.modules.writer.HBaseWriterProcessor} for a full example
 *
 */
public class HBaseParameters implements ArchiveFileConstants {

    /** DEFAULT OPTIONS **/
    // "content" column family and qualifiers
    public static final String CONTENT_COLUMN_FAMILY = "content";
    public static final String CONTENT_COLUMN_NAME = "raw_data";

    // "curi" column family and qualifiers
    public static final String CURI_COLUMN_FAMILY = "curi";
    public static final String IP_COLUMN_NAME = "ip";
    public static final String PATH_FROM_SEED_COLUMN_NAME = "path-from-seed";
    public static final String IS_SEED_COLUMN_NAME = "is-seed";
    public static final String VIA_COLUMN_NAME = "via";
    public static final String URL_COLUMN_NAME = "url";
    public static final String REQUEST_COLUMN_NAME = "request";

    // the zk client port name, this has to match what is in hbase-site.xml for the clientPort config attribute.
    public static String ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";


    /** ACTUAL OPTIONS INITIALIZED TO DEFAULTS **/
    private String contentColumnFamily = CONTENT_COLUMN_FAMILY;
    private String contentColumnName = CONTENT_COLUMN_NAME;

    private String curiColumnFamily = CURI_COLUMN_FAMILY;
    private String ipColumnName = IP_COLUMN_NAME;
    private String pathFromSeedColumnName = PATH_FROM_SEED_COLUMN_NAME;
    private String isSeedColumnName = IS_SEED_COLUMN_NAME;
    private String viaColumnName = VIA_COLUMN_NAME;
    private String urlColumnName = URL_COLUMN_NAME;
    private String requestColumnName = REQUEST_COLUMN_NAME;


    public String getContentColumnFamily() {
        return contentColumnFamily;
    }
    public void setContentColumnFamily(String contentColumnFamily) {
        this.contentColumnFamily = contentColumnFamily;
    }
    public String getContentColumnName() {
        return contentColumnName;
    }
    public void setContentColumnName(String contentColumnName) {
        this.contentColumnName = contentColumnName;
    }
    public String getCuriColumnFamily() {
        return curiColumnFamily;
    }
    public void setCuriColumnFamily(String curiColumnFamily) {
        this.curiColumnFamily = curiColumnFamily;
    }
    public String getIpColumnName() {
        return ipColumnName;
    }
    public void setIpColumnName(String ipColumnName) {
        this.ipColumnName = ipColumnName;
    }
    public String getPathFromSeedColumnName() {
        return pathFromSeedColumnName;
    }
    public void setPathFromSeedColumnName(String pathFromSeedColumnName) {
        this.pathFromSeedColumnName = pathFromSeedColumnName;
    }
    public String getIsSeedColumnName() {
        return isSeedColumnName;
    }
    public void setIsSeedColumnName(String isSeedColumnName) {
        this.isSeedColumnName = isSeedColumnName;
    }
    public String getViaColumnName() {
        return viaColumnName;
    }
    public void setViaColumnName(String viaColumnName) {
        this.viaColumnName = viaColumnName;
    }
    public String getUrlColumnName() {
        return urlColumnName;
    }
    public void setUrlColumnName(String urlColumnName) {
        this.urlColumnName = urlColumnName;
    }
    public String getRequestColumnName() {
        return requestColumnName;
    }
    public void setRequestColumnName(String requestColumnName) {
        this.requestColumnName = requestColumnName;
    }
    public String getZookeeperClientPort() {
        return ZOOKEEPER_CLIENT_PORT;
    }
}
