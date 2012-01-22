Welcome to HBase-Writer README for Heritrix 3.  

This document can also be found online here:
http://code.google.com/p/hbase-writer/wiki/READMEHeritrix3

Specific versions of HBase-Writer now support different
version combinations of Heritrix and HBase. Please refer to
http://code.google.com/p/hbase-writer/wiki/VERSIONS
for a more detailed list.

Before reading this document, please make sure the
HBase-Writer version you downloaded is meant to work with
Heritrix 3.


= TABLE OF CONTENTS =

* SETUP
* CONFIGURING HERITRIX
* FILE FORMAT
* COMPILING THE SOURCE
* BUILDING THE JAR
* BUILDING THE SITE-REPORT

The hbase-writer is an extension to the Heritrix open
source crawler written by the Internet Archive (http://crawler.archive.org/)
that enables it to store crawled content directly into HBase, the Hadoop
Distributed Database (http://hbase.org/  http://hadoop.apache.org/).  Hadoop implements
the Map/Reduce distributed computation framework on top of HDFS.  
hbase-writer-processor writes crawled content into hbase table records with rowkeys.
Having the crawled data in hbase tables is directly supported by the Map/Reduce framework.  
This facilitates running high-speed, distributed computations over content crawled with Heritrix.


= QUICK SETUP =

1. Start an instance of hbase.

2. Install heritrix-3.x.x

3. Copy the following jar files into the ${HERITRIX_HOME}/lib directory:

  hbase-writer-x.x.x.jar
  
  hbase-x.x.x.jar
  
  zookeeper-x.x.x.jar
  
  hadoop-x.x.x-core.jar
  
  log4j-x.x.x.jar

4. Start Heritrix


= CONFIGURING HERITRIX =

Add the following beans to the disposition chain of your job configuration:

---
{{{
<!-- DISPOSITION CHAIN -->
<bean id="hbaseParameterSettings" class="org.archive.io.hbase.HBaseParameters">
	<!-- These settings are required -->
	<property name="zkQuorum" value="localhost" />
	<property name="hbaseTableName" value="crawl" />

	<!-- This should reflect your installation, but 2181 is the default -->
	<property name="zkPort" value="2181" />

	<!-- All other settings are optional -->
	<property name="onlyProcessNewRecords" value="false" />
	<property name="onlyWriteNewRecords" value="false" />
	<property name="contentColumnFamily" value="newcontent" />
	<!-- 25 * 1024 * 1024 = 26214400 bytes -->
	<property name="defaultMaxFileSizeInBytes" value="26214400" />
	<!-- Overwrite more options here -->
</bean>

<bean id="hbaseWriterProcessor" class="org.archive.modules.writer.HBaseWriterProcessor">
	<property name="hbaseParameters">
		<ref bean="hbaseParameterSettings"/> 
	</property>
</bean>

<bean id="dispositionProcessors" class="org.archive.modules.DispositionChain">
	<property name="processors">
		<list>
			<ref bean="hbaseWriterProcessor"/>
			<!-- other references -->
		</list>
	</property>
</bean>
}}}
---

With the following configurable properties:

org.archive.io.hbase.HBaseParameters properties:

	zkQuorum (required)
	  The zookeeper quroum that serves the hbase master address.  Since hbase-0.20.0, the master server's address is returned by the zookeeper quorum.
	  So this value is a comma seperated list of the zk quorum.
	  e.g. zkHost1,zkHost2,zkHost3

	hbaseTableName (required)
	  Which table in HBase to write the crawl to.  This table will be created automatically if it doesnt exist.
	  e.g. crawl

	zkPort (defaults to 2181, which is the zookeeper default)
	  The zookeeper quroum client port that clients should connect to to get HBase information.
	  e.g. 2181

	contentColumnFamily
	  The column family name for where you want to save the content to. Defaults to "newcontent".

	contentColumnName
	  The column qualifier name for where you want to save the content to. Defaults to "raw_data" which becomes "newcontent:raw_data"

	curiColumnFamily
	  The column family name for storing the Crawl URI related information. Defaults to "curi".

	ipColumnName
	  The column qualifier name for storing the IP address. Defaults to "ip" which becomes "curi:ip".

	pathFromSeedColumnName
	  The column qualifier name for storing the path from seed. Defaults to "path-from-seed" which becomes "curi:path-from-seed".

	isSeedColumnName
	  The column qualifier name for storing whether the crawl is a seed. Defaults to "is-seed" which becomes "curi:is-seed".

	viaColumnName
	  The column qualifier name for storing where it came from. Defaults to "via" which becomes "curi:via".

	urlColumnName
	  The column qualifier name for storing the URL. Defaults to "url" which becomes "curi:url".

	requestColumnName
	  The column qualifier name for storing the request. Defaults to "request" which becomes "curi:request".

	onlyWriteNewRecords
	  Set to "false" by default.  In default mode, heritrix will crawl all urls regardless of existing rowkeys (urls).
	  By setting this to "true" you ensure that only new urls(rowkeys) are written to the crawl table.

	onlyProcessNewRecords
	  Set to "false" by default.  In default mode, heritrix will process (fetch and parse) all urls regardless of existing rowkeys (urls).
	  By setting this to "true" you ensure that only new urls(rowkeys) are processed by heritrix.  Also, if set to "true",
	  heritrix doesnt download any content that is already existing as a record in the hbase table.
	  
	defaultMaxFileSizeInBytes
	  Set to 20MB (20*1024*1024 bytes) by default.  If data item is fetched and it exceeds this amount, the content will not be written to hbase.
