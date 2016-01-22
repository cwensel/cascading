# Cascading on Apache Tez

Support for Cascading on Apache Tez is now a part of Cascading 3.x.

## Using

To use Tez as an alternative platform:

* change your maven dependencies to `cascading-hadoop2-tez`
* add any necessary Apache Tez dependencies as `provided`
* use the `c.f.t.Hadoop2TezFlowConnector` instead of the `c.f.h.Hadoop2MR1FlowConnector`.

You cannot have both `cascading-hadoop2-mr1` and `cascading-hadoop2-tez` in your project without dependency issues.

Both c.t.Tap and c.s.Scheme implementations will work with both MapReduce and Tez platforms, though custom Tap/Scheme
implementations may need to be updated to work against Cascading 3.0.

This release has been tested and built against Tez 0.8.2. Currently all applicable tests pass against the Tez platform.

See below for notes and issues against this release.

## Sample Applications

For a few sample applications, checkout the
[Cascading Samples](https://github.com/Cascading/cascading.samples/tree/wip-3.0) project.

This project also has a sample Gradle build file to help bootstrap new projects.

## Running Tests and Debugging

To run a single test case

    > gradle :cascading-hadoop2-tez:platformTest --tests=*.FieldedPipesPlatformTest -i 2>&1 | tee output.log

or test method

    > gradle :cascading-hadoop2-tez:platformTest --tests=*.FieldedPipesPlatformTest.testNoGroup -i 2>&1 | tee output.log

To enable a remote debugger,

    -Dtest.debug.node=[ordinal or source name] -Dtest.debug.address=localhost:5005

To enable Java Flight Recorder,

    -Dtest.profile.node=[ordinal or source name] -Dtest.profile.path=/tmp/jfr/

Note that the ordinal value is consistent across runs if there are no changes to the assembly graph. See the node/vertex
logs for the node ordinal value.

And source name can be the name of a CoGroup or GroupBy that acts as the boundary between two nodes, or the bound source
Tap name provided to the FlowDef.

## Tez Binaries

Unfortunately Apache Tez does not release with downloadable binaries. And as of 0.6.0, the set of binaries that work
on a given Hadoop revision are slightly different.

As a convenience, we are providing the build for each supported version of Tez. Of which can be easily copied down
onto HDFS for use by YARN.

| Tez Version | Hadoop Version | Bucket + Path                                                               | Stable |
|-------------|----------------|-----------------------------------------------------------------------------|--------|
| 0.5.0       | 2.4.x          | files.cascading.org/third-party/yarn/apps/tez-0.5.0/                        |        |
| 0.5.1       | 2.4.x          | files.cascading.org/third-party/yarn/apps/tez-0.5.1/                        |        |
| 0.5.3       | 2.4.x          | files.cascading.org/third-party/yarn/apps/tez-0.5.3/                        |        |
| 0.6.1       | 2.4.x          | files.cascading.org/third-party/yarn/apps/tez-0.6.1-minimal-hadoop24.tar.gz |   *    |
| 0.6.1       | 2.6.x          | files.cascading.org/third-party/yarn/apps/tez-0.6.1-minimal-hadoop26.tar.gz |   *    |
| 0.6.2       | 2.4.x          | files.cascading.org/third-party/yarn/apps/tez-0.6.2-minimal-hadoop24.tar.gz |   X    |
| 0.6.2       | 2.6.x          | files.cascading.org/third-party/yarn/apps/tez-0.6.2-minimal-hadoop26.tar.gz |   X    |
| 0.7.0       | 2.4.x          | files.cascading.org/third-party/yarn/apps/tez-0.7.0-minimal-hadoop24.tar.gz |   *    |
| 0.7.0       | 2.6.x          | files.cascading.org/third-party/yarn/apps/tez-0.7.0-minimal-hadoop26.tar.gz |   *    |
| 0.8.2       | 2.6.x          | files.cascading.org/third-party/yarn/apps/tez-0.8.2-minimal-hadoop26.tar.gz |   X *   |

* See known issues below.

For up to date notes on the available YARN apps, see: http://files.cascading.org/third-party/yarn/apps/NOTES.txt

To copy Tez 0.8.2 to HDFS, execute:

    hdfs dfs -cp s3n://files.cascading.org/third-party/yarn/apps/tez-0.6.2-minimal-hadoop24.tar.gz /apps/

Since the _minimal_ jar is being use, ensure 'tez.use.cluster.hadoop-libs=true'.

If using Hadoop 2.4.x, you must also set 'tez.allow.disabled.timeline-domains=true'.

## Running on Amazon EMR 4.x

To use Apache Tez on Amazon EMR 4.x, these Hadoop configuration options will be useful.

Note this will require starting your Hadoop cluster with specific settings, and changing your execution environment.

Before getting started, these project may help:

  * [BASH EMR](https://github.com/cwensel/bash-emr)
  * [AWS Client](http://aws.amazon.com/cli/)

First, re-configure Hadoop via this bootstrap action (these are the EMR client commandline args necessary):

    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
    --args -y,yarn.log-aggregation-enable=true,-y,yarn.log-aggregation.retain-seconds=-1,-y,yarn.log-aggregation.retain-check-interval-seconds=3000,-y,yarn.nodemanager.remote-app-log-dir=s3://your-bucket/emr/yarn-logs

Note these setting help debugging failed applications.

The next section is also applicable.

## Running a Tez Application

If on Amazon EMR 4.x, see above before booting your cluster.

Next shell into where you are running your Hadoop jobs, then:

    hdfs dfs -mkdir -p /user/
    hdfs dfs -chmod -R 777 /user/
    hdfs dfs -mkdir -p /apps/
    hdfs dfs -chmod -R 777 /apps/

    hdfs dfs -copyToLocal s3n://files.cascading.org/third-party/yarn/apps/tez-0.8.2-minimal-hadoop26.tar.gz
    hdfs dfs -cp s3n://files.cascading.org/third-party/yarn/apps/tez-0.8.2-minimal-hadoop26.tar.gz /apps/

    mkdir tez-0.8.2
    tar -xzf tez-0.8.2-minimal-hadoop26.tar.gz -C tez-0.8.2

    export HADOOP_CLASSPATH=~/tez-0.8.2/*:~/tez-0.8.2/lib/*:$HADOOP_CLASSPATH

See above for other available Tez versions.

You also need to start the YARN History/Timeline Server (not on by default in EMR):

If `/usr/sbin/yarn-daemon.sh` does not exist, create it:

    sudo cat > /usr/sbin/yarn-daemon.sh << EOF
    #!/bin/bash
    . /usr/lib/bigtop-utils/bigtop-detect-javahome

    export YARN_LOG_DIR=/mnt/var/log/hadoop-yarn
    export HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec

    exec /usr/lib/hadoop-yarn/sbin/yarn-daemon.sh "$@"
    EOF

    sudo chmod a+x /usr/sbin/yarn-daemon.sh

Now start the timeline server:

    yarn-daemon.sh start timelineserver

Then to execute your application you nee to make sure these properties are set on the `Hadoop2TezFlowConnector` or
`tez-site.xml`:

     tez.lib.uris=${fs.default.name}/apps/tez-0.8.2-minimal-hadoop26.tar.gz
     tez.use.cluster.hadoop-libs=true
     yarn.timeline-service.hostname=$HOSTNAME
     io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec
     mapred.output.committer.class=org.apache.hadoop.mapred.FileOutputCommitter

The last two lines provide missing EMR parameters. May be unnecessary on a local cluster.

## Notes and Known Issues

Some notes and issues with running Cascading on Apache Tez. JIRA issues will be noted when created.

* Tez 0.8.2, see https://issues.apache.org/jira/browse/TEZ-3113, you may need to set `tez.runtime.sorter.class=LEGACY`
  above

* Tez 0.6.1 has known deadlock issues with reasonably large dags, 0.6.2 resolves these issues.

* Tez 0.7.0 is unstable causing both random hangs in tests, and the default sorter fails on large partitions, see
  TEZ-2475 & TEZ-2505

* YARN timeline server (ATS) is required for task level counters, this will be required when Driven is supported.

* Kill hanging processes (before tests)

    > jps | grep DAGApp | cut -f1 -d" " | xargs kill -9; jps | grep TezChild | cut -f1 -d" " | xargs kill -9

* Contrary to GroupVertex, there is no obvious API for binding multiple vertices as a single output. Subsequently, some
  splits are written twice.

* Does not provide a plan supporting `JoinFieldedPipesPlatformTest#testJoinMergeGroupBy`. See
  `DualStreamedAccumulatedMergeNodeAssert` rule.

* Restartable Checkpointed Flows are unsupported though the tests will pass

* Remote classpath support via FlowDef is not working in Tez local mode, tests in mini-cluster mode will pass

* System.exit(0) must be called when running in Tez local mode, there are non-daemon threads not properly managed by Tez

* There is no Vertex 'parallelization' default in Tez, FlowRuntimeProps must be called per application (see sample
  applications above).

* Currently no way to algorithmically set node parallelization during runtime.