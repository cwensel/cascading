# Cascading

Thanks for using Cascading.

## Cascading 4.0

Cascading 4 includes a few major changes and additions from prior major releases:

* Moved the website to https://cascading.wensel.net/
* Moved to JCenter and changed the Maven group name to `net.wensel` (from `cascading`)
* Added native JSON support via the `cascading-nested-json` sub-project
* Removed `cascading-xml` sub-project
* Removed Apache Hadoop 1.x support

## General Information:

_Note: Everything is subject to change as we re-imagine Cascading 4 resources._

For project documentation and community support, visit: [cascading.wensel.net](https://cascading.wensel.net/)

The project includes nine Cascading jar files:

* `cascading-core-x.y.z.jar`              - all Cascading Core class files
* `cascading-expression-x.y.z.jar`        - all Cascading Janino expression operations class files
* `cascading-nested-json-x.y.z.jar`       - all Cascading JSON operations
* `cascading-nested-x.y.z.jar`            - all Cascading base classes for nested data-type operations
* `cascading-local-x.y.z.jar`             - all Cascading Local in-memory mode class files
* `cascading-hadoop2-common-x.y.z.jar`    - all Cascading Hadoop 2.x common class files
* `cascading-hadoop2-io-x.y.z.jar`        - all Cascading Hadoop 2.x HDFS and IO related class files
* `cascading-hadoop2-mr1-x.y.z.jar`       - all Cascading Hadoop 2.x MapReduce mode class files
* `cascading-hadoop2-tez-x.y.z.jar`       - all Cascading Hadoop 2.x Tez mode class files
* `cascading-hadoop2-tez-stats-x.y.z.jar` - all Cascading Tez YARN timeline server class files

These class jars, along with, tests, source and javadoc jars, are all available via the
JCenter Maven repository.

Hadoop 2.x MR1 mode is the same as above but for Hadoop 2.x releases.

Hadoop 2.x Tez mode is where the Cascading application should run on an Apache Tez *DAG* cluster.

Local mode is where the Cascading application will run locally in memory without any Hadoop dependencies or
cluster distribution. This implementation has minimal to no robustness in low memory situations, by design.

As of Cascading 4.x, all above jar files are built against Java 1.8. Prior versions of Cascading are built
against Java 1.7 and 1.6.

## Extensions, the SDK, and DSLs

There are a number of projects based on and extensions to Cascading available.

Visit the [Cascading Extensions](https://cascading.wensel.net/extensions/) page for a current list.

Of note are three top level projects:

* [Fluid](https://cascading.wensel.net/fluid/) - A fluent Java API for Cascading that is compatible with the default API.
* [Lingual](https://cascading.wensel.net/lingual/) - ANSI SQL and JDBC on Cascading
* [Pattern](https://cascading.wensel.net/pattern/) - Machine Learning scoring and [PMML](https://en.wikipedia.org/wiki/Predictive_Model_Markup_Language) support with Cascading

And alternative languages:

* [Scalding](https://cascading.wensel.net/projects/scalding/) - A Scala based DSL
* [Cascalog](https://cascading.wensel.net/projects/cascalog/) - A Clojure based DSL
* [PigPen](https://github.com/Netflix/PigPen) - A Clojure based DSL

And a third-party computing platform:

* [Apache Flink](https://cascading.wensel.net/cascading-flink/) - Faster than MapReduce cluster computing

## Versioning

Cascading stable releases are always of the form `x.y.z`, where `z` is the current maintenance release.

`x.y.z` releases are maintenance releases. No public incompatible API changes will be made, but in an effort to fix
bugs, remediation may entail throwing new Exceptions.

`x.y` releases are minor releases. New features are added. No public incompatible API changes will be made on the
core processing APIs (Pipes, Functions, etc), but in an effort to resolve inconsistencies, minor semantic changes may be
necessary.

It is important to note that *we do reserve to make breaking changes to the new query planner API through the 4.x
releases*. This allows us to respond to bugs and performance issues without issuing new major releases.

The source and tags for all prior (to 4.x) stable releases can be found here:
[https://github.com/Cascading/cascading](https://github.com/Cascading/cascading)

All 4.x releases will be maintainted here:
[https://github.com/cwensel/cascading](https://github.com/cwensel/cascading)

WIP (work in progress) releases are fully tested builds of code not yet deemed fully stable. On every build by our
continuous integration servers, the WIP build number is increased. Successful builds are then tagged and published.

The WIP releases are always of the form `x.y.z-wip-n`, where `x.y.z` will be the next stable release version the WIP
releases are leading up to. `n` is the current successfully tested build.

The source, working branches, and tags for all WIP releases can be found here:
[https://github.com/cwensel/cascading](https://github.com/cwensel/cascading)

Or downloaded from here:
[http://cascading.wensel.net/wip/](https://cascading.wensel.net/wip/)

When a WIP is deemed stable and ready for production use, it will be published as a `x.y.z` release, and made
available from the [http://cascading.wensel.net/downloads/](http://cascading.wensel.net/downloads/) page.

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

It is strongly recommended developers pull Cascading from Jcenter.
[jcenter.bintray.com](https://jcenter.bintray.com/).

Alternatively, see bintray for latest wip and final releases: 

* https://bintray.com/wensel/wip
* https://bintray.com/wensel/release

When creating tests, make sure to add any of the relevant above dependencies to your `test` scope or equivalent
configuration along with the `cascading-platform` dependency.

Note the `cascading-platform` compile dependency has no classes, you must pull the tests dependency with the
`tests` classifier.

See [http://cascading.wensel.net/downloads/#maven](https://cascading.wensel.net/downloads/#maven) for example Maven pom
dependency settings.

Source and Javadoc artifacts (using the appropriate classifier) are also available through Conjars.

Note that `cascading-hadoop2-mr1`, and `cascading-hadoop2-tez` have a `provided` dependency on the
Hadoop jars so that it won't get sucked into any application packaging as a dependency, typically.

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

To use Cascading with Hadoop, we suggest stuffing `cascading-core` and `cascading-hadoop2-mr1`, jar files and all
third-party libs into the `lib` folder of your job jar and executing your job via
`$HADOOP_HOME/bin/hadoop jar your.jar <your args>`.

For example, your job jar would look like this (via: `jar -t your.jar`)

```bash
/<all your class and resource files>
/lib/cascading-core-x.y.z.jar
/lib/cascading-hadoop2-common-x.y.z.jar
/lib/cascading-hadoop2-mr1-x.y.z.jar
/lib/cascading-hadoop2-io-x.y.z.jar
/lib/cascading-expression-x.y.z.jar
/lib/cascading-nested-json-x.y.z.jar
/lib/<cascading third-party jar files>
```

Hadoop will unpack the jar locally and remotely (in the cluster) and add any libraries in `lib` to the classpath. This
is a feature specific to Hadoop.