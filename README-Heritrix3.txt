Welcome to HBase-Writer README for Heritrix 3.  

This document can also be found online here:
http://code.google.com/p/hbase-writer/wiki/README-Heritrix3

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
<!-- DISPOSITION CHAIN -->
<bean id="hbaseParameterSettings" class="org.archive.io.hbase.HBaseParameters">
	<property name="contentColumnFamily" value="newcontent" />
	<!-- Overwrite more options here -->
</bean>

<bean id="hbaseWriterProcessor" class="org.archive.modules.writer.HBaseWriterProcessor">
	<property name="zkQuorum" value="localhost" />
	<property name="zkClientPort" value="2181" />
	<property name="hbaseTable" value="crawl" />
	<property name="onlyProcessNewRecords" value="false" />
	<property name="onlyWriteNewRecords" value="false" />
	<property name="hbaseParameters">
		<ref bean="hbaseParameterSettings"/> 
	</property>
</bean>

<bean id="dispositionProcessors" class="org.archive.modules.DispositionChain">
	<property name="processors">
	<list>
		<!-- write to aggregate archival files... -->
		<ref bean="hbaseWriterProcessor"/>
		<!-- other references -->
	</list>
	</property>
</bean>
---

With the following configurable properties:

org.archive.modules.writer.HBaseWriterProcessor properties:

	zkQuorum
	  The zookeeper quroum that serves the hbase master address.  Since hbase-0.20.0, the master server's address is returned by the zookeeper quorum.
	  So this value is a comma seperated list of the zk quorum.
	  e.g. zkHost1,zkHost2,zkHost3

	zkClientPort
	  The zookeeper quroum client port that clients should connect to to get HBase information.
	  e.g. 2181

	hbaseTable
	  Which table in HBase to write the crawl to.  This table will be created automatically if it doesnt exist.
	  e.g. Webtable

	onlyWriteNewRecords
	  Set to "false" by default.  In default mode, heritrix will crawl all urls regardless of existing rowkeys (urls).
	  By setting this to "true" you ensure that only new urls(rowkeys) are written to the crawl table.

	onlyProcessNewRecords
	  Set to "false" by default.  In default mode, heritrix will process (fetch and parse) all urls regardless of existing rowkeys (urls).
	  By setting this to "true" you ensure that only new urls(rowkeys) are processed by heritrix.  Also, if set to "true",
	  heritrix doesnt download any content that is already existing as a record in the hbase table.


org.archive.io.hbase.HBaseParameters properties:

	contentColumnFamily
	  The column family name for where you want to save the content to. Defaults to "content".

	contentColumnName
	  The column qualifier name for where you want to save the content to. Defaults to "raw_data" which becomes "content:raw_data"

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


= COMPILING THE SOURCE =

  mvn clean compile

= BUILDING THE JAR =

  mvn clean package

The hbase-writer-x.x.x.jar should be in the target/ directory.  
You can get the hadoop, hbase and log4j dependency jars from your ${HOME}/.m2/repository/ directory.
For example:
  cp ${HOME}/.m2/repository/org/apache/hadoop/hbase/0.20.1/hbase-0.20.1.jar ${HERITRIX_HOME}/lib/
  cp ${HOME}/.m2/repository/org/apache/hadoop/zookeeper/3.2.1/zookeeper-3.2.1.jar ${HERITRIX_HOME}/lib/
  cp ${HOME}/.m2/repository/org/apache/hadoop/hadoop-core/0.20.1/hadoop-core-0.20.1.jar ${HERITRIX_HOME}/lib/ 
  cp ${HOME}/.m2/repository/log4j/log4j/1.2.16/log4j-1.2.16.jar ${HERITRIX_HOME}/lib/
  
= UPGRADING TO NEW HADOOP/HBASE/HERITRIX VERSIONS =

To build hbase-writer with new versions of hadoop, hbase or heritrix (or any of the dependencies), use a ${HOME}/.m2/settings.xml file.

A sample settings.xml file:
 <?xml version="1.0" encoding="UTF-8"?>
 <settings>
  <profiles>
	<profile>
	  <id>myBuild</id>
	  <properties>
            <heritrix.version>3.1.0</heritrix.version>
            <hbase.version>0.90.3</hbase.version>
            <hadoop.version>0.20.205.0</hadoop.version>
            <zookeeper.version>3.3.2</zookeeper.version>
	  </properties>
	</profile>
  </profiles>
 </settings> 
  
Place this file in your ${HOME}/.m2/ directory and run the maven build command:
 mvn clean package -PmyBuild

= CONTRIBUTING SOURCE =
If you would like to contribute a patch to help improve the source code, please feel free to create an Issue in the Issues section on the project website.  
Just attach your patch to the issue and a committer will merge the changes as soon as possible.  Thank you.
 
= BUILDING THE SITE REPORT =

  mvn clean site

= PING BACK =

Thanks to the Open Source community for all the support for releasing this project.
	
