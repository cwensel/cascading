# Cascading on Apache Tez

Support for Cascading on Apache Tez is part of the Cascading 3.0 WIP (work in progress) releases. 

We are making this release available so interested parties can begin testing Tez deployments against existing 
Cascading applications.

WIP builds can be downloaded directly from http://www.cascading.org/wip/

Alternately, all binaries are available through the [Conjars.org](http://conjars.org) Maven repository.

## Using

To use Tez as an alternative platform:

* change your maven dependencies to `cascading-hadoop2-tez`
* add any necessary Apache Tez dependencies as `provided`
* use the c.f.t.Hadoop2TezFlowConnector instead of the c.f.h.Hadoop2MR1Planner.

You cannot have both `cascading-hadoop2-mr1` and `cascading-hadoop2-tez` in your project without dependency issues. 

Both c.t.Tap and c.s.Scheme implementations will work with both MapReduce and Tez platforms, though custom Tap/Scheme
implementations may need to be updated to work against Cascading 3.0.

This release has been tested and built against Tez 0.5.1. Currently all applicable tests pass against the Tez platform.

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
    
    -Dtest.debug.node=[ordinal or source name]

## Notes and Known Issues

Some notes and issues with running Cascading on Apache Tez. JIRA issues will be noted when created.

* Kill hanging processes (before tests)

    > jps | grep DAGApp | cut -f1 -d" " | xargs kill -9; jps | grep TezChild | cut -f1 -d" " | xargs kill -9
    
* To prevent deadlocks in local mode, "tez.am.inline.task.execution.max-tasks" is set to 2 contrary to comments.

* Contrary to GroupVertex, there is no obvious API for binding multiple vertices as a single output. Subsequently, some
  splits are written twice.
    
* Does not provide a plan supporting `JoinFieldedPipesPlatformTest#testJoinMergeGroupBy`. See 
  `DualStreamedAccumulatedMergeNodeAssert` rule.

* Restartable Checkpointed Flows are unsupported though the tests will pass

* Remote classpath support via FlowDef is not working in Tez local mode, tests in mini-cluster mode will pass

* System.exit(0) must be called when running in Tez local mode, there are non-daemon threads not properly managed by Tez

* There is no Vertex 'parallelization' default in Tez, FlowRuntimeProps must be called per application (see sample 
  applications above).

* Currently no way to algorithmically set node parallelization. Look for FlowNodeStrategy in future wip releases.

* Tests ignore parallelization settings, otherwise both local mode and mini cluster modes deadlock.
    