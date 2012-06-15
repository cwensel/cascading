# Cascading

Thanks for using Cascading.

## General Information:

Project and contact information: http://www.cascading.org/

This distribution includes six Cascading jar files:

* cascading-x.y.z.jar      - all relevant Cascading class files and libraries, with a 'lib' folder
* cascading-core-x.y.z.jar - all Cascading Core class files
* cascading-local-x.y.z.jar - all Cascading Local mode class files
* cascading-hadoop-x.y.z.jar - all Cascading Hadoop mode class files
* cascading-xml-x.y.z.jar  - all Cascading XML operations class files
* cascading-test-x.y.z.jar - all Cascading tests and test utilities

These jars are all available via www.conjars.org.

Hadoop mode is where the Cascading application should run on a Hadoop cluster.

Local mode is where the Cascading application will run locally in memory without any Hadoop dependenices.

## Building:

To build Cascading, run the following in the shell:

```bash
> git clone https://github.com/cwensel/cascading.git
> cd cascading
> gradle build
```

Cascading currently requires Gradle 1.0 to build.

To use an IDE like IntelliJ, run the following to get all jar dependencies:

```bash
> gradle ideLibs
```

## Using with Apache Hadoop:

To use Cascading with Hadoop, we suggest stuffing `cascading-core`, `cascading-hadoop`, (optionally) `cascading-xml` jarfiles and all third-party libs (optionally retrieved by calling `gradle ideLibs`) into the `lib` folder of your job jar and executing your job via `$HADOOP_HOME/bin/hadoop jar your.jar <your args>`.

Note you do not need to put the lib/hadoop jars in your jar as they are already present in your cluster.

For example, your job jar would look like this (via: `jar -t your.jar`)

```bash
/<all your class and resource files>
/lib/cascading-core-x.y.z.jar
/lib/cascading-hadoop-x.y.z.jar
/lib/cascading-xml-x.y.z.jar
/lib/<cascading third-party jar files>
```

Hadoop will unpack the jar locally and remotely (in the cluster) and add any libraries in `lib` to the classpath. This is a feature specific to Hadoop.

The cascading-x.y.z.jar file is typically used with scripting languages and is completely self contained, but it cannot be added to a jar `lib` folder as Hadoop will not recursively unjar jars.
