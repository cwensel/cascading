/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.platform.tez;

import java.io.File;
import java.io.IOException;
import java.security.Permission;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProps;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.FlowSession;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.flow.tez.Hadoop2TezFlowProcess;
import cascading.flow.tez.planner.Hadoop2TezPlanner;
import cascading.platform.hadoop.BaseHadoopPlatform;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.test.MiniTezCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class Hadoop2Platform is automatically loaded and injected into a {@link cascading.PlatformTestCase} instance
 * so that all *PlatformTest classes can be tested against Apache Hadoop 2.x.
 */
public class Hadoop2TezPlatform extends BaseHadoopPlatform<TezConfiguration>
  {
  private static final Logger LOG = LoggerFactory.getLogger( Hadoop2TezPlatform.class );
  private transient static MiniDFSCluster miniDFSCluster;
  private transient static MiniTezCluster miniTezCluster;
  private transient static SecurityManager securityManager;

  public Hadoop2TezPlatform()
    {
    this.numGatherPartitions = 4;
    }

  @Override
  public String getName()
    {
    return "hadoop2-tez";
    }

  @Override
  public FlowConnector getFlowConnector( Map<Object, Object> properties )
    {
    return new Hadoop2TezFlowConnector( properties );
    }

  @Override
  public void setNumGatherPartitionTasks( Map<Object, Object> properties, int numGatherPartitions )
    {
    properties.put( FlowRuntimeProps.GATHER_PARTITIONS, Integer.toString( numGatherPartitions ) );
    }

  @Override
  public Integer getNumGatherPartitionTasks( Map<Object, Object> properties )
    {
    if( properties.get( FlowRuntimeProps.GATHER_PARTITIONS ) == null )
      return null;

    return Integer.parseInt( properties.get( FlowRuntimeProps.GATHER_PARTITIONS ).toString() );
    }

  public TezConfiguration getConfiguration()
    {
    return new TezConfiguration( configuration );
    }

  @Override
  public FlowProcess getFlowProcess()
    {
    return new Hadoop2TezFlowProcess( FlowSession.NULL, null, getConfiguration() );
    }

  @Override
  public boolean isMapReduce()
    {
    return false;
    }

  @Override
  public boolean isDAG()
    {
    return true;
    }

  @Override
  public synchronized void setUp() throws IOException
    {
    if( configuration != null )
      return;

    if( !isUseCluster() )
      {
      // Current usage requirements:
      // 1. Clients need to set "tez.local.mode" to true when creating a TezClient instance. (For the examples this can be done via -Dtez.local.mode=true)
      // 2. fs.defaultFS must be set to "file:///"
      // 2.1 If running examples - this must be set in tez-site.xml (so that it's picked up by the client, as well as the conf instances used to configure the Inputs / Outputs).
      // 2.2 If using programatically (without a tez-site.xml present). All configuration instances used (to crate the client / configure Inputs / Outputs) - must have this property set.
      // 3. tez.runtime.optimize.local.fetch needs to be set to true (either via tez-site.xml or in all configurations used to create the job (similar to fs.defaultFS in step 2))
      // 4. tez.staging-dir must be set (either programatically or via tez-site.xml).
      // Until TEZ-1337 goes in - the staging-dir for the job is effectively the root of the filesystem (and where inputs are read from / written to if relative paths are used).

      LOG.info( "not using cluster" );
      configuration = new Configuration();

      configuration.setInt( FlowRuntimeProps.GATHER_PARTITIONS, 1 ); // deadlocks if larger than 1

//      configuration.set( "fs.default.name", "file:///" );
//      configuration.set( "mapred.job.tracker", "local" );
//      configuration.set( "mapreduce.jobtracker.staging.root.dir", "build/tmp/cascading/staging" );

      configuration.set( TezConfiguration.TEZ_LOCAL_MODE, "true" );
      configuration.set( "fs.defaultFS", "file:///" );
      configuration.set( "tez.runtime.optimize.local.fetch", "true" );

      // hack to prevent deadlocks where downstream processors are scheduled before upstream
//      configuration.setInt( "tez.am.inline.task.execution.max-tasks", 1 );
      configuration.setInt( "tez.am.inline.task.execution.max-tasks", 2 );
//      configuration.setInt( "tez.am.inline.task.execution.max-tasks", 3 );

      configuration.set( TezConfiguration.TEZ_IGNORE_LIB_URIS, "true" ); // in local mode, use local classpath
      configuration.setInt( YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, -1 );
      configuration.set( TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS, "true" );

      configuration.set( "tez.am.mode.session", "true" ); // allows multiple TezClient instances to be used in a single jvm

      if( !Util.isEmpty( System.getProperty( "hadoop.tmp.dir" ) ) )
        configuration.set( "hadoop.tmp.dir", System.getProperty( "hadoop.tmp.dir" ) );
      else
        configuration.set( "hadoop.tmp.dir", "build/test/tmp" );

      fileSys = FileSystem.get( configuration );
      }
    else
      {
      LOG.info( "using cluster" );

      if( Util.isEmpty( System.getProperty( "hadoop.log.dir" ) ) )
        System.setProperty( "hadoop.log.dir", "build/test/log" );

      if( Util.isEmpty( System.getProperty( "hadoop.tmp.dir" ) ) )
        System.setProperty( "hadoop.tmp.dir", "build/test/tmp" );

      new File( System.getProperty( "hadoop.log.dir" ) ).mkdirs(); // ignored
      new File( System.getProperty( "hadoop.tmp.dir" ) ).mkdirs(); // ignored

      Configuration defaultConf = new Configuration();

//      defaultConf.setInt( FlowRuntimeProps.GATHER_PARTITIONS, getNumGatherPartitions() );
      defaultConf.setInt( FlowRuntimeProps.GATHER_PARTITIONS, 1 ); // deadlocks if larger than 1

      defaultConf.setInt( YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, -1 );

//      defaultConf.set( TezConfiguration.TEZ_AM_LOG_LEVEL, "DEBUG" );
//      defaultConf.set( TezConfiguration.TEZ_TASK_LOG_LEVEL, "DEBUG" );

//      defaultConf.set( TezConfiguration.TEZ_PROFILE_CONTAINER_LIST, "2" );
//      defaultConf.set( TezConfiguration.TEZ_PROFILE_JVM_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005" );
//      defaultConf.set( TezConfiguration.TEZ_PROFILE_JVM_OPTS, "-agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005,suspend=y" );

      defaultConf.setInt( YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1 );
      defaultConf.setBoolean( TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false );
      defaultConf.set( MiniDFSCluster.HDFS_MINIDFS_BASEDIR, System.getProperty( "hadoop.tmp.dir" ) );

      miniDFSCluster = new MiniDFSCluster.Builder( defaultConf )
        .numDataNodes( 4 )
        .format( true )
        .racks( null )
        .build();

      fileSys = miniDFSCluster.getFileSystem();

      Configuration tezConf = new Configuration( defaultConf );
      tezConf.set( "fs.defaultFS", fileSys.getUri().toString() ); // use HDFS
      tezConf.set( MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir" );

      miniTezCluster = new MiniTezCluster( getClass().getName(), 1, 4, 4 ); // todo: set to 4
      miniTezCluster.init( tezConf );
      miniTezCluster.start();

      configuration = miniTezCluster.getConfig();

      ///////////////// other
/*
      JobConf conf = new JobConf();

      if( !Util.isEmpty( System.getProperty( "mapred.jar" ) ) )
        {
        LOG.info( "using a remote cluster with jar: {}", System.getProperty( "mapred.jar" ) );
        jobConf = conf;

        ( (JobConf) jobConf ).setJar( System.getProperty( "mapred.jar" ) );

        if( !Util.isEmpty( System.getProperty( "fs.default.name" ) ) )
          {
          LOG.info( "using {}={}", "fs.default.name", System.getProperty( "fs.default.name" ) );
          jobConf.set( "fs.default.name", System.getProperty( "fs.default.name" ) );
          }

        if( !Util.isEmpty( System.getProperty( "mapred.job.tracker" ) ) )
          {
          LOG.info( "using {}={}", "mapred.job.tracker", System.getProperty( "mapred.job.tracker" ) );
          jobConf.set( "mapred.job.tracker", System.getProperty( "mapred.job.tracker" ) );
          }

        jobConf.set( "mapreduce.user.classpath.first", "true" ); // use test dependencies
        fileSys = FileSystem.get( jobConf );

        }
      else
        {
        conf.setBoolean( "yarn.is.minicluster", true );
//      conf.setInt( "yarn.nodemanager.delete.debug-delay-sec", -1 );
//      conf.set( "yarn.scheduler.capacity.root.queues", "default" );
//      conf.set( "yarn.scheduler.capacity.root.default.capacity", "100" );
        // disable blacklisting hosts not to fail localhost during unit tests
        conf.setBoolean( "yarn.app.mapreduce.am.job.node-blacklisting.enable", false );

        dfs = new MiniDFSCluster( conf, 4, true, null );
        fileSys = dfs.getFileSystem();

        FileSystem.setDefaultUri( conf, fileSys.getUri() );

        mr = MiniMRClientClusterFactory.create( this.getClass(), 4, conf );

        jobConf = mr.getConfig();
        }


*/
//      configuration.set( "mapred.child.java.opts", "-Xmx512m" );
//      configuration.setInt( "mapreduce.job.jvm.numtasks", -1 );
//      configuration.setInt( "mapreduce.client.completion.pollinterval", 50 );
//      configuration.setInt( "mapreduce.client.progressmonitor.pollinterval", 50 );
//      configuration.setBoolean( "mapreduce.map.speculative", false );
//      configuration.setBoolean( "mapreduce.reduce.speculative", false );
      }

//    configuration.setInt( "mapreduce.job.maps", numMapTasks );
//    configuration.setInt( "mapreduce.job.reduces", numReduceTasks );

    configuration.setInt( TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 1 );
    configuration.setInt( TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 1 );
    configuration.setInt( TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 1 );

    Map<Object, Object> globalProperties = getGlobalProperties();

    if( logger != null )
      globalProperties.put( "log4j.logger", logger );

    FlowProps.setJobPollingInterval( globalProperties, 10 ); // should speed up tests

    Hadoop2TezPlanner.copyProperties( configuration, globalProperties ); // copy any external properties

    Hadoop2TezPlanner.copyConfiguration( properties, configuration ); // put all properties on the jobconf

    ExitUtil.disableSystemExit();

//    forbidSystemExitCall();
    }

  private static class ExitTrappedException extends SecurityException
    {
    }

  private static void forbidSystemExitCall()
    {
    if( securityManager != null )
      return;

    securityManager = new SecurityManager()
    {
    public void checkPermission( Permission permission )
      {
      if( !"exitVM".equals( permission.getName() ) )
        return;

      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

      for( StackTraceElement stackTraceElement : stackTrace )
        LOG.warn( "exit vm trace: {}", stackTraceElement );

      throw new ExitTrappedException();
      }
    };

    System.setSecurityManager( securityManager );
    }

  private static void enableSystemExitCall()
    {
    securityManager = null;
    System.setSecurityManager( null );
    }
  }
