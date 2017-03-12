# Cascading

Thanks for using Cascading.

## Cascading 4.0

Cascading 4 includes a few major changes and additions from prior major releases:

* TBD

## General Information:

For project documentation and community support, visit: [cascading.org](http://cascading.org/)

To download a pre-built distribution, visit [http://cascading.org/downloads/](http://cascading.org/downloads/),
or use Maven (described below).

The project includes nine Cascading jar files:

* `cascading-core-x.y.z.jar`              - all Cascading Core class files
* `cascading-xml-x.y.z.jar`               - all Cascading XML operations class files
* `cascading-expression-x.y.z.jar`        - all Cascading Janino expression operations class files
* `cascading-local-x.y.z.jar`             - all Cascading Local in-memory mode class files
* `cascading-hadoop-x.y.z.jar`            - all Cascading Hadoop 1.x MapReduce mode class files
* `cascading-hadoop2-io-x.y.z.jar`        - all Cascading Hadoop 2.x HDFS and IO related class files
* `cascading-hadoop2-mr1-x.y.z.jar`       - all Cascading Hadoop 2.x MapReduce mode class files
* `cascading-hadoop2-tez-x.y.z.jar`       - all Cascading Hadoop 2.x Tez mode class files
* `cascading-hadoop2-tez-stats-x.y.z.jar` - all Cascading Tez YARN timeline server class files

These class jars, along with, tests, source and javadoc jars, are all available via the
[Conjars.org](http://conjars.org) Maven repository.

Hadoop 1.x mode is where the Cascading application should run on a Hadoop *MapReduce* cluster.

Hadoop 2.x MR1 mode is the same as above but for Hadoop 2.x releases.

Hadoop 2.x Tez mode is where the Cascading application should run on an Apache Tez *DAG* cluster.

Local mode is where the Cascading application will run locally in memory without any Hadoop dependencies or
cluster distribution. This implementation has minimal to no robustness in low memory situations, by design.

As of Cascading 4.x, all above jar files are built against Java 1.7. Prior versions of Cascading are built
against Java 1.6.

## Extensions, the SDK, and DSLs

There are a number of projects based on and extensions to Cascading available.

Visit the [Cascading Extensions](http://cascading.org/extensions/) page for a current list.

Or download the [Cascading SDK](http://cascading.org/sdk/) which includes many pre-built binaries.

Of note are three top level projects:

* [Fluid](http://cascading.org/fluid/) - A fluent Java API for Cascading that is compatible with the default API.
* [Lingual](http://cascading.org/lingual/) - ANSI SQL and JDBC on Cascading
* [Pattern](http://cascading.org/pattern/) - Machine Learning scoring and [PMML](http://en.wikipedia.org/wiki/Predictive_Model_Markup_Language) support with Cascading

And alternative languages:

* [Scalding](http://cascading.org/projects/scalding/) - A Scala based DSL
* [Cascalog](http://cascading.org/projects/cascalog/) - A Clojure based DSL
* [PigPen](https://github.com/Netflix/PigPen) - A Clojure based DSL

And a third-party computing platform:

* [Apache Flink](http://www.cascading.org/cascading-flink/) - Faster than MapReduce cluster computing

## Versioning

Cascading stable releases are always of the form `x.y.z`, where `z` is the current maintenance release.

`x.y.z` releases are maintenance releases. No public incompatible API changes will be made, but in an effort to fix
bugs, remediation may entail throwing new Exceptions.

`x.y` releases are minor releases. New features are added. No public incompatible API changes will be made on the
core processing APIs (Pipes, Functions, etc), but in an effort to resolve inconsistencies, minor semantic changes may be
necessary.

It is important to note that *we do reserve to make breaking changes to the new query planner API through the 4.x
releases*. This allows us to respond to bugs and performance issues without issuing new major releases.

The source and tags for all stable releases can be found here:
[https://github.com/Cascading/cascading](https://github.com/Cascading/cascading)

WIP (work in progress) releases are fully tested builds of code not yet deemed fully stable. On every build by our
continuous integration servers, the WIP build number is increased. Successful builds are then tagged and published.

The WIP releases are always of the form `x.y.z-wip-n`, where `x.y.z` will be the next stable release version the WIP
releases are leading up to. `n` is the current successfully tested build.

The source, working branches, and tags for all WIP releases can be found here:
[https://github.com/cwensel/cascading](https://github.com/cwensel/cascading)

Or downloaded from here:
[http://cascading.org/wip/](http://cascading.org/wip/)

When a WIP is deemed stable and ready for production use, it will be published as a `x.y.z` release, and made
available from the [http://cascading.org/downloads/](http://cascading.org/downloads/) page.

## Writing and Running Tests

Comprehensive tests should be written against the `cascading.PlatformTestCase`.

When running tests built against the PlatformTestCase, the local cluster can be disabled (if enabled by the test)
by setting:

    -Dtest.cluster.enabled=false

From Gradle, to run a single test case:

    > gradle :cascading-hadoop2-mr1:platformTest --tests=*.FieldedPipesPlatformTest -i

or a single test method:

    > gradle :cascading-hadoop2-mr1:platformTest --tests=*.FieldedPipesPlatformTest.testNoGroup -i

## Debugging the Planner

When running tests, set the following

    -Dtest.traceplan.enabled=true

If you are on Mac OS X and have installed GraphViz, dot files can be converted to pdf on the fly. To enable, set:

    -Dutil.dot.to.pdf.enabled=true

Optionally, for stand alone applications, statistics and tracing can be enabled selectively with the following
properties:

* `cascading.planner.stats.path` - outputs detailed statistics on time spent by the planner
* `cascading.planner.plan.path` - basic planner information
* `cascading.planner.plan.transforms.path` - detailed information for each rule

## Contributing and Reporting Issues

See __CONTRIBUTING.md__ at https://github.com/Cascading/cascading.

## Using with Maven/Ivy

It is strongly recommended developers pull Cascading from our Maven compatible jar repository
[Conjars.org](http://conjars.org).

You can find the latest public and WIP (work in progress) releases here:

*  http://conjars.org/cascading/cascading-core
*  http://conjars.org/cascading/cascading-local
*  http://conjars.org/cascading/cascading-hadoop
*  http://conjars.org/cascading/cascading-hadoop2-io
*  http://conjars.org/cascading/cascading-hadoop2-mr1
*  http://conjars.org/cascading/cascading-hadoop2-tez
*  http://conjars.org/cascading/cascading-hadoop2-tez-stats
*  http://conjars.org/cascading/cascading-xml
*  http://conjars.org/cascading/cascading-expression

When creating tests, make sure to add any of the relevant above dependencies to your `test` scope or equivalent
configuration along with the `cascading-platform` dependency.

*  http://conjars.org/cascading/cascading-platform

Note the `cascading-platform` compile dependency has no classes, you must pull the tests dependency with the
`tests` classifier.

See [http://cascading.org/downloads/#maven](http://cascading.org/downloads/#maven) for example Maven pom
dependency settings.

Source and Javadoc artifacts (using the appropriate classifier) are also available through Conjars.

Note that `cascading-hadoop`, `cascading-hadoop2-mr1`, and `cascading-hadoop2-tez` have a `provided` dependency on the
Hadoop jars so that it won't get sucked into any application packaging as a dependency, typically.

## Building and IDE Integration

For most cases, building Cascading is unnecessary as it has been pre-built, tested, and published to our Maven
repository (above).

To build Cascading, run the following in the shell:

```bash
> git clone https://github.com/cascading/cascading.git
> cd cascading
> gradle build
```

Cascading requires at least Gradle 2.7 and Java 1.7 to build.

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

To use Cascading with Hadoop, we suggest stuffing `cascading-core` and `cascading-hadoop2-mr1`, jar files and all
third-party libs into the `lib` folder of your job jar and executing your job via
`$HADOOP_HOME/bin/hadoop jar your.jar <your args>`.

For example, your job jar would look like this (via: `jar -t your.jar`)

```bash
/<all your class and resource files>
/lib/cascading-core-x.y.z.jar
/lib/cascading-hadoop2-mr1-x.y.z.jar
/lib/cascading-hadoop2-io-x.y.z.jar
/lib/cascading-expression-x.y.z.jar
/lib/<cascading third-party jar files>
```

Hadoop will unpack the jar locally and remotely (in the cluster) and add any libraries in `lib` to the classpath. This
is a feature specific to Hadoop.