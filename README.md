# Cascading

Thanks for using Cascading.

## General Information:

For project documentation and community support, visit: [cascading.org](http://cascading.org/)

To download a pre-built distribution, visit [http://cascading.org/downloads/](http://cascading.org/downloads/),
or use Maven (described below).

The project includes five Cascading jar files:

* `cascading-core-x.y.z.jar`          - all Cascading Core class files
* `cascading-xml-x.y.z.jar`           - all Cascading XML operations class files
* `cascading-local-x.y.z.jar`         - all Cascading Local in-memory mode class files
* `cascading-hadoop-x.y.z.jar`        - all Cascading Hadoop 1.x MapReduce mode class files
* `cascading-hadoop2-mr1-x.y.z.jar`   - all Cascading Hadoop 2.x MapReduce mode class files

These class jars, along with, tests, source and javadoc jars, are all available via the
[Conjars.org](http://conjars.org) Maven repository.

Hadoop 1.x mode is where the Cascading application should run on a Hadoop cluster. Hadoop 2.x is the same
but for Hadoop 2.x releases.

Local mode is where the Cascading application will run locally in memory without any Hadoop dependencies.

## Extensions, the SDK, and DSLs

There are a number of projects based on Cascading available. Visit the
[Cascading Extensions](http://cascading.org/extensions/) page for a current list.

Or download the [Cascading SDK](http://cascading.org/sdk/) which includes pre-built binaries.

Of note are three top level projects:

* [Fluid](http://cascading.org/fluid/) - An alternative fluent API for Cascading 
* [Lingual](http://cascading.org/lingual/) - ANSI SQL and JDBC with Cascading
* [Pattern](http://cascading.org/pattern/) - Machine Learning and [PMML](http://en.wikipedia.org/wiki/Predictive_Model_Markup_Language) support with Cascading

And new languages:

* [Scalding](http://cascading.org/projects/scalding/) - A Scala based DSL
* [Cascalog](http://cascading.org/projects/cascalog/) - A Clojure based DSL

## Versioning

Cascading stable releases are always of the form `x.y.z`, where `z` is the current maintenance release.

The source and tags for all stable releases can be found here:
[https://github.com/Cascading/cascading](https://github.com/Cascading/cascading)

WIP (work in progress) releases are fully tested builds of code not yet deemed fully stable. On every build by our
continuous integration servers, the WIP build number is increased. Successful builds are then tagged and published.

The WIP releases are always of the form `x.y.z-wip-n`, where `x.y.z` will be the next stable release version the WIP
releases are leading up to. `n` is the current successfully tested build.

The source, working branches, and tags for all WIP releases can be found here:
[https://github.com/cwensel/cascading](https://github.com/cwensel/cascading)

Or downloaded from here:
[http://www.cascading.org/wip/](http://cascading.org/wip/)

When a WIP is deemed stable and ready for production use, it will be published as a `x.y.z` release, and made
available from the [http://cascading.org/downloads/](http://cascading.org/downloads/) page.

## Reporting issues

To report an issue, first ping the [Cascading User mailing list](http://cascading.org/support/) with any questions.

If unresolved, look for a comparable test in the `cascading-platform` sub-project, reproduce your issue, then issue
a pull request with the failing test added to one of the existing suites.

If you wish to allow your test code to be added to the Cascading test suite, please sign and return this
[contributor agreement](http://files.concurrentinc.com/agreements/Concurrent_Contributor_Agreement.doc).

## Using with Maven/Ivy

It is strongly recommended developers pull Cascading from our Maven compatible jar repository
[Conjars.org](http://conjars.org).

You can find the latest public and WIP (work in progress) releases here:

*  http://conjars.org/cascading/cascading-core
*  http://conjars.org/cascading/cascading-local
*  http://conjars.org/cascading/cascading-hadoop
*  http://conjars.org/cascading/cascading-hadoop2-mr1
*  http://conjars.org/cascading/cascading-xml

When creating tests, make sure to add any of the relevant above dependencies to your `test` scope or equivalent
configuration along with the `cascading-platform` dependency.

*  http://conjars.org/cascading/cascading-platform

Note the `cascading-platform` compile dependency has no classes, you must pull the tests dependency with the
`tests` classifier.

See [http://cascading.org/downloads/#maven](http://cascading.org/downloads/#maven) for example Maven pom
dependency settings.

Source and Javadoc artifacts (using the appropriate classifier) are also available through Conjars.

Note that `cascading-hadoop` and `cascading-hadoop2-mr1` has a `provided` dependency on the Hadoop jars so that it
won't get sucked into any application packaging as a dependency, typically.

## Building and IDE Integration

For most cases, building Cascading is unnecessary as it has been pre-built, tested, and published to our Maven
repository (above).

To build Cascading, run the following in the shell:

```bash
> git clone https://github.com/cascading/cascading.git
> cd cascading
> gradle build
```

Cascading requires Gradle 1.x to build.

To use an IDE like IntelliJ, run the following to create IntelliJ project files:

```bash
> gradle idea
```

Similarly for Eclipse:

```bash
> gradle eclipse
```

## Using with Apache Hadoop

First confirm you are using a supported version of Apache Hadoop by checking the
[Compatibility](http://cascading.org/support/compatibility/) page.

To use Cascading with Hadoop, we suggest stuffing `cascading-core`, `cascading-hadoop`, (optionally) `cascading-xml`
jar files and all third-party libs into the `lib` folder of your job jar and executing your job via
`$HADOOP_HOME/bin/hadoop jar your.jar <your args>`.

For example, your job jar would look like this (via: `jar -t your.jar`)

```bash
/<all your class and resource files>
/lib/cascading-core-x.y.z.jar
/lib/cascading-hadoop-x.y.z.jar
/lib/cascading-xml-x.y.z.jar
/lib/<cascading third-party jar files>
```

Hadoop will unpack the jar locally and remotely (in the cluster) and add any libraries in `lib` to the classpath. This
is a feature specific to Hadoop.