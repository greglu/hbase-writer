Welcome to HBase-Writer README.  

This document can also be found online here:
http://code.google.com/p/hbase-writer/wiki/README

INTRODUCTION
============
This is a processor for heritrix that writes fetched pages to hbase.

The layout of this contribution is modeled after Doug Judds'
heritrix-hadoop-dfs-processor available off the heritrix home page.

This software is licensed under the LGPL.  See accompanying LICENSE.txt document.

TABLE OF CONTENTS
=================
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

The current version of hbase-writer assumes version
2.0.x of Heritrix and version 0.20.x of Hadoop & HBase.  Newer versions of HBase, Hadoop
and Heritrix may continue to work with this connector as long as the pertinent
APIs have not changed.  Just replace the jar files with the newer versions.


QUCIK SETUP
===========

1. Start an instance of hbase.
2. Install heritrix-2.0.x
3. Copy the following jar files into the ${HERITRIX_HOME}/lib directory:

  hbase-writer-x.x.x.jar
  hbase-x.x.x.jar
  zookeeper-x.x.x.jar
  hadoop-x.x.x-core.jar
  log4j-x.x.x.jar

4. Start Heritrix


CONFIGURING HERITRIX
====================

On the "Settings for sheet 'global'":

- Click on a 'details' link and add new processor com.powerset.heritrix.writer.HBaseWriterProcessor.
- Return to the global sheet and set this as your writer (the page will not draw completely if
did not type in the name of the processor properly -- see heritrix_out.log for errors).
- Set at least the zkquorum and table configuration for HBaseWriterProcessor.

zkquorum
  The zookeeper quroum that serves the hbase master address.  Since hbase-0.20.0, the master server's address is returned by the zookeeper quorum.
  So this value is a comma seperated list of the zk quorum.
  i.e.: zkHost1,zkHost2,zkHost3

zkclientport
  The zookeeper quroum client port that clients should connect to to get HBase information.
  i.e.: 2181

table
  Which table in HBase to write the crawl to.  This table will be created automatically if it doesnt exist.
  i.e.: Webtable
  
write-only-new-records
  Set to "false" by default.  In default mode, heritrix will crawl all urls regardless of existing rowkeys (urls).  
  By setting this to "true" you ensure that only new urls(rowkeys) are written to the crawl table.  

process-only-new-records
  Set to "false" by default.  In default mode, heritrix will process (fetch and parse) all urls regardless of existing rowkeys (urls).  
  By setting this to "true" you ensure that only new urls(rowkeys) are processed by heritrix.  Also, if set to "true", 
  heritrix doesnt download any content that is already existing as a record in the hbase table. 

COMPILING THE SOURCE
====================
  mvn clean compile

BUILDING THE JAR
=====================
  mvn clean package

The hbase-writer-x.x.x.jar should be in the target/ directory.  
You can get the hadoop, hbase and log4j dependency jars from your ${HOME}/.m2/repository/ directory.
For example:
  cp ${HOME}/.m2/repository/org/apache/hadoop/hbase/0.20.1/hbase-0.20.1.jar ${HERITRIX_HOME}/lib/
  cp ${HOME}/.m2/repository/org/apache/hadoop/zookeeper/3.2.1/zookeeper-3.2.1.jar ${HERITRIX_HOME}/lib/
  cp ${HOME}/.m2/repository/org/apache/hadoop/hadoop-core/0.20.1/hadoop-core-0.20.1.jar ${HERITRIX_HOME}/lib/ 
  cp ${HOME}/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar ${HERITRIX_HOME}/lib/
  
UPGRADING TO NEW HADOOP/HBASE/HERITRIX VERSIONS
================================================
To build hbase-writer with new versions of hadoop, hbase or heritrix (or any of the dependencies), use a ${HOME}/.m2/settings.xml file.

A sample settings.xml file:
 <?xml version="1.0" encoding="UTF-8"?>
 <settings>
  <profiles>
	<profile>
	  <id>myBuild</id>
	  <properties>
            <heritrix.version>2.0.2</heritrix.version>
            <hbase.version>0.20.1</hbase.version>
            <hadoop.version>0.20.1</hadoop.version>
            <zookeeper.version>3.2.1</zookeeper.version>
	  </properties>
	</profile>
  </profiles>
 </settings> 
  
Place this file in your ${HOME}/.m2/ directory and run the maven build command:
 mvn clean package -PmyBuild
 
BUILDING THE SITE REPORT
========================
  mvn clean site

	
