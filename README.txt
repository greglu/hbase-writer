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
README-Heritrix*.txt files for specific instructions.

http://code.google.com/p/hbase-writer/wiki/READMEHeritrix2

http://code.google.com/p/hbase-writer/wiki/READMEHeritrix3

