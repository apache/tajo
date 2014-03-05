***************
Introduction
***************

The main goal of Apache Tajo project is to build an advanced open source
data warehouse system in Hadoop for processing web-scale data sets. 
Basically, Tajo provides SQL standard as a query language.
Tajo is designed for both interactive and batch queries on data sets
stored on HDFS and other data sources. Without hurting query response
times, Tajo provides fault-tolerance and dynamic load balancing which
are necessary for long-running queries. Tajo employs a cost-based and
progressive query optimization techniques for reoptimizing running
queries in order to avoid the worst query plans.