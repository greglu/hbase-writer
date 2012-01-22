Welcome to HBase-Writer README.

This document can also be found online here:
http://code.google.com/p/hbase-writer/wiki/README

Specific versions of HBase-Writer now support different
version combinations of Heritrix and HBase. Please refer to
http://code.google.com/p/hbase-writer/wiki/VERSIONS
for a more detailed list.


= INTRODUCTION =

This is a processor for Heritrix that writes fetched pages to HBase.

The layout of this contribution is modeled after Doug Judds'
heritrix-hadoop-dfs-processor available off the heritrix home page.

This software is licensed under the LGPL.  See accompanying LICENSE.txt document.

The HBase-Writer project is an extension to the Heritrix open
source crawler written by the Internet Archive (http://crawler.archive.org/)
that enables it to store crawled content directly into HBase tables (http://hbase.org/) running on the Hadoop Distributed FileSystem (http://lucene.apache.org/hadoop/).
HBase-Writer writes crawled content into a given hbase table as records.
In turn, these tables are directly supported by the Map/Reduce framework via HBase so Map/Reduce jobs can be done on them. 

= GETTING STARTED = 

HBase-Writer now supports Heritrix 2 and 3. Please refer to the corresponding
READMEHeritrix*.txt files for specific instructions.

http://code.google.com/p/hbase-writer/wiki/READMEHeritrix3

http://code.google.com/p/hbase-writer/wiki/READMEHeritrix2

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
{{{
 <?xml version="1.0" encoding="UTF-8"?>
 <settings>
  <profiles>
	<profile>
	  <id>myBuild</id>
	  <properties>
            <heritrix.version>3.1.0</heritrix.version>
            <hbase.version>0.90.5</hbase.version>
            <hadoop.version>0.20.205.0</hadoop.version>
            <zookeeper.version>3.3.2</zookeeper.version>
	  </properties>
	</profile>
  </profiles>
 </settings> 
}}}

Place this file in your ${HOME}/.m2/ directory and run the maven build command:
 mvn clean package -PmyBuild

= CONTRIBUTING SOURCE =
If you would like to contribute a patch to help improve the source code, please feel free to create an Issue in the Issues section on the project website.  
Just attach your patch to the issue and a committer will merge the changes as soon as possible.  Thank you.
 
= BUILDING THE SITE REPORT =

  mvn clean site

= PING BACK =

Thanks to the Open Source community for all the support for releasing this project.

