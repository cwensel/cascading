# Cascading 4.0 Local Mode Extensions

This project contains extensions and examples for use with Cascading 4's local mode planner.

Prior to Cascading 4, the local mode planner was not particularly promoted as production ready. 

The new Tap classes and examples in this project aim to show how Cascading can be used reliably in local mode for batch 
or stream processing, with a few limitations (currently Cascading 4 does not provide any online aggregations).

This is particularly useful for building log and data cleansing, partitioning, and indexing applications.

This project and Cascading 4 are still currently under development. There are a number of enhancements 
that would much improve these classes, but the best way to prioritize them is to request them on the 
Cascading mail-list (see below).

## Provides

### Apache Kafka Tap

A full featured Tap that can both read and write data from, to, and between Apache Kafka topics/queues.

See the `cascading-local-kafka` sub-project for details.

For maven coordinates, see: http://conjars.org/cascading/cascading-local-kafka

### AWS S3 Tap

A full featured Tap that can both read and write data from, to and between AWS S3 buckets. 

In addition, this tap can restart where it left off on a previous fetch, even across JVM executions. 

See the `cascading-local-s3` sub-project for details.

For maven coordinates, see: http://conjars.org/cascading/cascading-local-s3

### Splunk Tap

A full featured Tap that can both read and write data from, to and between Splunk instances. 

See the `cascading-local-splunk` sub-project for details.

For maven coordinates, see: http://conjars.org/cascading/cascading-local-splunk

### S3 Log Parser Example

A trivial example where S3 access logs are both read, parsed, and stored directly into a Kafka queue, and where
the queue is then consumed, parsed, and stored as directory partitioned CSV files.

See the `cascading-local-example` sub-project for details.

## General Information:

For project documentation and community support, visit: [cascading.org](http://cascading.org/)
