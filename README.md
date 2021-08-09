# Cascading

Thanks for using [Cascading](https://cascading.wensel.net/).

## Cascading 4.1

Cascading 4 includes a few major changes and additions from prior major releases:

* Local mode support improved for production use
* Moved the website to https://cascading.wensel.net/
* Changed the Maven group name to `net.wensel` (from `cascading`)
* Moved to GitHub Packages (WIPs) and Maven Central (releases)
* Added native JSON support via the `cascading-nested-json` sub-project
* Removed `cascading-xml` sub-project
* Removed Apache Hadoop 1.x support

## General Information:

For current WIP releases, go to: https://github.com/cwensel?tab=packages&repo_name=cascading

For project documentation and community support, visit: [cascading.wensel.net](https://cascading.wensel.net/)

The project includes nine Cascading jar files:

* `cascading-core-x.y.z.jar`              - all Cascading Core class files
* `cascading-expression-x.y.z.jar`        - all Cascading Janino expression operations class files
* `cascading-nested-json-x.y.z.jar`       - all Cascading JSON operations
* `cascading-nested-x.y.z.jar`            - all Cascading base classes for nested data-type operations
* `cascading-local-x.y.z.jar`             - all Cascading Local in-memory mode class files
* `cascading-local-kafka-x.y.z.jar`       - all Cascading Local support for Apache Kafka
* `cascading-local-neo4j-x.y.z.jar`       - all Cascading Local support for Neo4j
* `cascading-local-s3-x.y.z.jar`          - all Cascading Local support for AWS S3
* `cascading-local-splunk-x.y.z.jar`      - all Cascading Local support for Splunk
* `cascading-local-hadoop3-io-x.y.z.jar`  - all Cascading Local in-memory mode class files used with Hadoop
* `cascading-hadoop3-common-x.y.z.jar`    - all Cascading Hadoop 2.x common class files
* `cascading-hadoop3-io-x.y.z.jar`        - all Cascading Hadoop 2.x HDFS and IO related class files
* `cascading-hadoop3-mr1-x.y.z.jar`       - all Cascading Hadoop 2.x MapReduce mode class files
* `cascading-hadoop3-tez-x.y.z.jar`       - all Cascading Hadoop 2.x Tez mode class files
* `cascading-hadoop3-tez-stats-x.y.z.jar` - all Cascading Tez YARN timeline server class files

These class jars, along with, tests, source and javadoc jars, are all available via the Maven repository.

Local mode is where the Cascading application will run locally in memory without cluster distribution. This
implementation has minimal to no robustness in low memory situations, by design.

Hadoop 3.x MR1 mode is for running on Hadoop 3.x releases.

Hadoop 3.x Tez mode is where the Cascading application should run on an Apache Tez *DAG* cluster.

As of Cascading 4.x, all above jar files are built against Java 1.8. Prior versions of Cascading are built against Java
1.7 and 1.6.

## Local Mode

Local mode has been much improved for production use in applications that do not need to run distributed across a
cluster. Specifically in applications that trivially parallelize and run within AWS Lambda or Batch applications.

See https://github.com/cwensel/cascading-local for a collection of local mode integrations.

Note this project will merge into Cascading in then next minor release.

## Extensions, the SDK, and DSLs

There are a number of projects based on and extensions to Cascading available.

Visit https://cascading.org/ for links. As these projects are updated to depend on 4.x, we will update the main site.

Note many projects built and released against Cascading 3.x will work without modification with Cascading 4.x.

## Versioning

Cascading stable releases are always of the form `x.y.z`, where `z` is the current maintenance release.

`x.y.z` releases are maintenance releases. No public incompatible API changes will be made, but in an effort to fix
bugs, remediation may entail throwing new Exceptions.

`x.y` releases are minor releases. New features are added. No public incompatible API changes will be made on the core
processing APIs (Pipes, Functions, etc), but in an effort to resolve inconsistencies, minor semantic changes may be
necessary.

It is important to note that *we do reserve to make breaking changes to the new query planner API through the 4.x
releases*. This allows us to respond to bugs and performance issues without issuing new major releases.

All releases will be maintained here:
[https://github.com/cwensel/cascading](https://github.com/cwensel/cascading)

WIP (work in progress) releases are fully tested builds of code not yet deemed fully stable. On every build by our
continuous integration servers, the WIP build number is increased. Successful builds are then tagged and published.

The WIP releases are always of the form `x.y.z-wip-n`, where `x.y.z` will be the next stable release version the WIP
releases are leading up to. `n` is the current successfully tested build.

The source, working branches, and tags for all WIP releases can be found here:
[https://github.com/cwensel/cascading](https://github.com/cwensel/cascading)

When a WIP is deemed stable and ready for production use, it will be published as a `x.y.z` release, and made available
from Maven Central.

## Writing and Running Tests

Comprehensive tests should be written against the `cascading.PlatformTestCase`.

When running tests built against the PlatformTestCase, the local cluster can be disabled (if enabled by the test)
by setting:

    -Dtest.cluster.enabled=false

From Gradle, to run a single test case:

    > gradle :cascading-hadoop3-mr1:platformTest --tests=*.FieldedPipesPlatformTest -i

or a single test method:

    > gradle :cascading-hadoop3-mr1:platformTest --tests=*.FieldedPipesPlatformTest.testNoGroup -i

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

It is strongly recommended developers pull Cascading from Maven Central.

Alternatively, see GitHub Packages for latest WIP releases:

* https://maven.pkg.github.com/cwensel/cascading

When creating tests, make sure to add any of the relevant above dependencies to your `test` scope or equivalent
configuration along with the `cascading-platform` dependency.

Note the `cascading-platform` compile dependency has no classes, you must pull the tests dependency with the
`tests` classifier.

Source and Javadoc artifacts (using the appropriate classifier) are also available through Maven.

Note that `cascading-hadoop3-mr1`, and `cascading-hadoop3-tez` have a `provided` dependency on the Hadoop jars so that
it won't get sucked into any application packaging as a dependency, typically.

## Building and IDE Integration

For most cases, building Cascading is unnecessary as it has been pre-built, tested, and published to our Maven
repository (above).

To build Cascading, run the following in the shell:

```bash
> git clone https://github.com/cwensel/cascading.git
> cd cascading
> ./gradlew build
```

## Using with Apache Hadoop

First confirm you are using a supported version of Apache Hadoop by checking the
[Compatibility](https://cascading.wensel.net/support/compatibility/) page.

To use Cascading with Hadoop, we suggest stuffing `cascading-core` and `cascading-hadoop3-mr1`, jar files and all
third-party libs into the `lib` folder of your job jar and executing your job via
`$HADOOP_HOME/bin/hadoop jar your.jar <your args>`.

For example, your job jar would look like this (via: `jar -t your.jar`)

```bash
/<all your class and resource files>
/lib/cascading-core-x.y.z.jar
/lib/cascading-hadoop3-common-x.y.z.jar
/lib/cascading-hadoop3-mr1-x.y.z.jar
/lib/cascading-hadoop3-io-x.y.z.jar
/lib/cascading-expression-x.y.z.jar
/lib/cascading-nested-json-x.y.z.jar
/lib/<cascading third-party jar files>
```

Hadoop will unpack the jar locally and remotely (in the cluster) and add any libraries in `lib` to the classpath. This
is a feature specific to Hadoop.

## History and Status

Cascading was started in 2007 by Chris K Wensel.

After a series of acquisitions, it was left unsupported and unmaintained by the copyright, domain name, and GitHub
organization owners.

Chris has since continued his noodling with Cascading and has been pushing changes to the original repo.

Cascading remains under the Apache License v2.0.