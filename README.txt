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
  hadoop-x.x.x-core.jar
  hadoop-hbase-x.x.x.jar
  log4j-x.x.x.jar

  Get the latter three jars from ${HADOOP_HOME}/lib.

4. Start Heritrix


CONFIGURING HERITRIX
====================

On the Modules Page "Select Writers" section

- Remove the ARCWriterProcessor
- Add the HBaseWriterProcessor in the "Select Writers" section
  (NOTE: it's at the *bottom* of the drop-down menu)

On the Settings page you will find the following configuration items under
write-processors -> HDFSArchiver section:

master
  The host and port of the hbase master server.

table
  Which table to crawl into.

COMPILING THE SOURCE
====================

Run the following commands:

tar xzvf hbase-writer-x.x.x.tar.gz
cd hbase-writer-x.x.x
(Create a build.properties with defines for hadoop.home.dir and
heritrix.home.dir so the hbase build can find the jars it needs)
ant jar

The jar file will end up in the build/ subdirectory.  To see the new hbase
writer processor in eclipse, you need to add the
hbase-writer-x.x.xjar in the 'Java build path -> Libraries' panel.
