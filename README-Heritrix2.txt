Welcome to HBase-Writer README for Heritrix 2.  

This document can also be found online here:
http://code.google.com/p/hbase-writer/wiki/READMEHeritrix2

Specific versions of HBase-Writer now support different
version combinations of Heritrix and HBase. Please refer to
http://code.google.com/p/hbase-writer/wiki/VERSIONS
for a more detailed list.

Before reading this document, please make sure the
HBase-Writer version you downloaded is meant to work with
Heritrix 2.



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

2. Install heritrix-2.x.x

3. Copy the following jar files into the ${HERITRIX_HOME}/lib directory:

  hbase-writer-x.x.x.jar

  hbase-x.x.x.jar

  zookeeper-x.x.x.jar

  hadoop-x.x.x-core.jar

  log4j-x.x.x.jar

4. Start Heritrix


= CONFIGURING HERITRIX = 

On the "Settings for sheet 'global'":

- Click on a 'details' link and add new processor com.powerset.heritrix.writer.HBaseWriterProcessor.
- Return to the global sheet and set this as your writer (the page will not draw completely if
did not type in the name of the processor properly -- see heritrix_out.log for errors).
- Set at least the zkquorum and table configuration for HBaseWriterProcessor.

zkquorum
  The zookeeper quroum that serves the hbase master address.  Since hbase-0.20.0, the master server's address is returned by the zookeeper quorum.
  So this value is a comma seperated list of the hosts listed in your zk quorum.
  i.e.: zkHost1,zkHost2,zkHost3,10.2.34.55,zkhost.example.com

zkclientport
  The zookeeper quroum client port that clients should connect to to get HBase information.
  i.e.: 2181  (this is the default)

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

