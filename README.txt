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


SETUP
=====

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
Use maven2.
  mvn clean compile

BUILDING THE JAR
=====================
  mvn clean package
  
BUILDING THE SITE REPORT
========================
  mvn clean site

	