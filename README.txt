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

The heritrix-hadoop-dfs-writer-processor is an extension to the Heritrix open
source crawler written by the Internet Archive (http://crawler.archive.org/)
that enables it to store crawled content directly into HDFS, the Hadoop
Distributed FileSystem (http://lucene.apache.org/hadoop/).  Hadoop implements
the Map/Reduce distributed computation framework on top of HDFS.  
heritrix-hadoop-dfs-writer-processor writes crawled content into SequenceFile
format which is directly supported by the Map/Reduce framework and has support
for compression.  This facilitates running high-speed, distributed computations
over content crawled with Heritrix.

The current version of heritrix-hadoop-dfs-writer-processor assumes version
2.0.x of Heritrix and version 0.18.x of Hadoop.  Newer versions of Hadoop
and Heritrix may continue to work with this connector as long as the pertinent
APIs have not changed.  Just replace the jar files with the newer versions.


QUCIK SETUP
===========

1. Start an instance of hbase.
2. Install heritrix-2.0.x
3. Copy the following jar files into the ${HERITRIX_HOME}/lib directory:

  hbase-writer-x.x.x.jar
  hbase-x.x.x.jar
  hadoop-x.x.x-core.jar
  log4j-x.x.x.jar

4. Start Heritrix


CONFIGURING HERITRIX
====================

On the "Settings for sheet 'global'":

- Click on a 'details' link and add new processor com.powerset.heritrix.writer.HBaseWriterProcessor.
- Return to the global sheet and set this as your writer (the page will not draw completely if
did not type in the name of the processor properly -- see heritrix_out.log for errors).
- Set at least the master and table configuration for HBaseWriterProcessor.

master
  The host and port of the hbase master server.

table
  Which table to crawl into.

COMPILING THE SOURCE
====================
  mvn clean compile

BUILDING THE JAR
=====================
  mvn clean package

The hbase-writer-x.x.x.jar should be in the target/ directory.  
You can get the hadoop, hbase and log4j dependency jars from your ${HOME}/.m2/repository/ directory.
For example:
  cp ${HOME}/.m2/repository/org/apache/hadoop/hbase/0.18.0/hbase-0.18.0.jar ${HERITRIX_HOME}/lib/
  cp ${HOME}/.m2/repository/org/apache/hadoop/hadoop-core/0.18.0/hadoop-core-0.18.0.jar ${HERITRIX_HOME}/lib/ 
  cp ${HOME}/.m2/repository/log4j/log4j/1.2.14/log4j-1.2.14.jar ${HERITRIX_HOME}/lib/
  
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
            <hbase.version>0.18.1</hbase.version>
            <hadoop.version>0.18.1</hadoop.version>
	  </properties>
	</profile>
  </profiles>
 </settings> 
  
Place this file in your ${HOME}/.m2/ directory and run the maven build command:
 mvn clean package -PmyBuild
 
BUILDING THE SITE REPORT
========================
  mvn clean site


PING BACK
=========
Thanks to Questio for the time and support for allowing the release and maintenance of this project. (http://questio.com)
	