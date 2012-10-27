# Cascading

Thanks for using Cascading.

## General Information:

For project documentation and community support, visit: [cascading.org](http://cascading.org/)

This distribution includes five Cascading jar files:

* cascading-core-x.y.z.jar     - all Cascading Core class files
* cascading-xml-x.y.z.jar      - all Cascading XML operations class files
* cascading-local-x.y.z.jar    - all Cascading Local mode class files
* cascading-hadoop-x.y.z.jar   - all Cascading Hadoop mode class files
* cascading-platform-x.y.z.jar - all Cascading common platform tests and test utilities

These class jars, along with source and javadoc jars, are all available via the [Conjars.org](http://conjars.org)
Maven repository.

Hadoop mode is where the Cascading application should run on a Hadoop cluster.

Local mode is where the Cascading application will run locally in memory without any Hadoop dependenices.

The Platform jar only includes tests common across all supported platforms.

## Building:

To build Cascading, run the following in the shell:

```bash
> git clone https://github.com/cascading/cascading.git
> cd cascading
> gradle build
```

Cascading currently requires Gradle 1.0.

To use an IDE like IntelliJ, run the following to get create IntelliJ module files:

```bash
> gradle ideaModule
```

## Using with Apache Hadoop:

To use Cascading with Hadoop, we suggest stuffing `cascading-core`, `cascading-hadoop`, (optionally) `cascading-xml`
jarfiles and all third-party libs into the `lib` folder of your job jar and executing your job via
`$HADOOP_HOME/bin/hadoop jar your.jar <your args>`.

For example, your job jar would look like this (via: `jar -t your.jar`)

```bash
/<all your class and resource files>
/lib/cascading-core-x.y.z.jar
/lib/cascading-hadoop-x.y.z.jar
/lib/cascading-xml-x.y.z.jar
/lib/<cascading third-party jar files>
```

Hadoop will unpack the jar locally and remotely (in the cluster) and add any libraries in `lib` to the classpath. This is a feature specific to Hadoop.