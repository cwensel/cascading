
Thanks for using Cascading.

General Information:

  Project and contact information: http://www.cascading.org/

  This distribution includes four Cascading jar files:

  cascading-x.y.z.jar      - all relevant Cascading class files and libraries, with a 'lib' folder
  cascading-core-x.y.z.jar - all Cascading Core class files
  cascading-local-x.y.z.jar - all Cascading local mode class files
  cascading-hadoop-x.y.z.jar - all Cascading Hadoop mode class files
  cascading-xml-x.y.z.jar  - all Cascading XML operations class files
  cascadgin-test-x.y.z.jar - all Cascading tests and test utilities

Building:

  To retrieve all immediate dependencies, excluding Hadoop (see below)

  > cd <path to cascading>
  > ant retrieve

  To build Cascading,

  > ant compile

  To make all jars:

  > ant  jar

  To run all tests:

  > ant test

  where <path to cascading> is the directory created after cloning or uncompressing the Cascading
  distribution.

Using:

  To use with Hadoop, we suggest stuffing cascading-core, cascading-hadoop, and optionally cascading-xml jar
  files, and all third-party libs (lib/core and lib/xml) into the 'lib' folder of your job jar and executing via
  'hadoop jar your.jar <your args>'.

  Note you do not need to pub the lib/hadoop jars in your jar as they are already present in your cluster.

  For example, your job jar would look like this (via: jar -t your.jar)

    /<all your class and resource files>
    /lib/cascading-core-x.y.z.jar
    /lib/cascading-hadoop-x.y.z.jar
    /lib/cascading-xml-x.y.z.jar
    /lib/<cascading third-party jar files>

  Hadoop will unpack the jar locally and remotely (in the cluster) and add any libraries in 'lib' to the classpath.
  This is a feature specific to Hadoop.

  The cascading-x.y.z.jar file is typically used with scripting languages and is completely self contained.