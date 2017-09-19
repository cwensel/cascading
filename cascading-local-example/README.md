# Cascading Local Mode Examples

## Amazon S3 Log Parsing w/ Apache Kafka

This is a fabricated example showing the use of the S3Tap and KafkaTap classes provided by this project.

The first Flow reads from Amazon S3, and writes to a Kafka queue. The second Flow simply reads from the Kafka queue
and writes the data as CSV to a directory structure partitioned by the HTTP method and day the request was made.

What's important to note about this example is that it runs continuously, that is, the Kafka flow runs forever until the 
JVM is shutdown or no more data is available in the queue within a given timeout.

Also note that type conversions are inherent to the operations, initially available in Cascading 3 and improved in 4, 
there is no need for an explicit coercion function to convert Strings to other primitive or complex types. 

For example, see the `cascading.tuple.type.DateType` class, it maintains the date/time information as a `long` in the 
Tuple, but honors the date/time format when retrieved as a `String` type.

Also, org.cascading.aws.s3.logs.CleanCoercibleType class can be used to cleanse any data before coercion to its 
canonical representation. In this case the '-' is a common place holder for a `null` value in a field, so when '-'
is encountered, the canonical value is `null` in the tuple.  

To build and run:

`
gradle shadowJar
java -cp cascading-local-example/build/libs/cascading-local-example-shaded-1.0.0-wip-dev.jar cascading.aws.s3.logs.Main [input s3 uri] [kafka host] [output directory] <checkpoint dir>
`

