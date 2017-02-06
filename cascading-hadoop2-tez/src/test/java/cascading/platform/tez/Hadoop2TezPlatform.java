/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

import cascading.CascadingException;
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
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService;
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
  private transient ApplicationHistoryServer yarnHistoryServer;

  public Hadoop2TezPlatform()
    {
    this.numGatherPartitions = 1;
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

      configuration.setInt( FlowRuntimeProps.GATHER_PARTITIONS, getNumGatherPartitions() );
//      configuration.setInt( FlowRuntimeProps.GATHER_PARTITIONS, 1 ); // deadlocks if larger than 1

      configuration.set( TezConfiguration.TEZ_LOCAL_MODE, "true" );
      configuration.set( "fs.defaultFS", "file:///" );
      configuration.set( "tez.runtime.optimize.local.fetch", "true" );

      // hack to prevent deadlocks where downstream processors are scheduled before upstream
      configuration.setInt( "tez.am.inline.task.execution.max-tasks", 3 ); // testHashJoinMergeIntoHashJoinAccumulatedAccumulatedMerge fails if set to 2

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

      defaultConf.setInt( FlowRuntimeProps.GATHER_PARTITIONS, getNumGatherPartitions() );

      defaultConf.setInt( YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, -1 );

//      defaultConf.set( TezConfiguration.TEZ_AM_LOG_LEVEL, "DEBUG" );
//      defaultConf.set( TezConfiguration.TEZ_TASK_LOG_LEVEL, "DEBUG" );

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

      // see MiniTezClusterWithTimeline as alternate
      miniTezCluster = new MiniTezCluster( getClass().getName(), 4, 1, 1 ); // todo: set to 4
      miniTezCluster.init( tezConf );
      miniTezCluster.start();

      configuration = miniTezCluster.getConfig();

      // stats won't work after completion unless ATS is used
      if( setTimelineStore( configuration ) ) // true if ats can be loaded and configured for this hadoop version
        {
        configuration.set( TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, ATSHistoryLoggingService.class.getName() );
        configuration.setBoolean( YarnConfiguration.TIMELINE_SERVICE_ENABLED, true );
        configuration.set( YarnConfiguration.TIMELINE_SERVICE_ADDRESS, "localhost:10200" );
        configuration.set( YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS, "localhost:8188" );
        configuration.set( YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS, "localhost:8190" );

        yarnHistoryServer = new ApplicationHistoryServer();
        yarnHistoryServer.init( configuration );
        yarnHistoryServer.start();
        }
      }

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

  protected boolean setTimelineStore( Configuration configuration )
    {
    try
      {
      // try hadoop 2.6
      Class<?> target = Util.loadClass( "org.apache.hadoop.yarn.server.timeline.TimelineStore" );
      Class<?> type = Util.loadClass( "org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore" );

      configuration.setClass( YarnConfiguration.TIMELINE_SERVICE_STORE, type, target );

      try
        {
        // hadoop 2.5 has the above classes, but this one is also necessary for the timeline service with acls to function.
        Util.loadClass( "org.apache.hadoop.yarn.api.records.timeline.TimelineDomain" );
        }
      catch( CascadingException exception )
        {
        configuration.setBoolean( TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS, true );
        }

      return true;
      }
    catch( CascadingException exception )
      {
      try
        {
        // try hadoop 2.4
        Class<?> target = Util.loadClass( "org.apache.hadoop.yarn.server.applicationhistoryservice.timeline.TimelineStore" );
        Class<?> type = Util.loadClass( "org.apache.hadoop.yarn.server.applicationhistoryservice.timeline.MemoryTimelineStore" );

        configuration.setClass( YarnConfiguration.TIMELINE_SERVICE_STORE, type, target );
        configuration.setBoolean( TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS, true );

        return true;
        }
      catch( CascadingException ignore )
        {
        return false;
        }
      }
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
