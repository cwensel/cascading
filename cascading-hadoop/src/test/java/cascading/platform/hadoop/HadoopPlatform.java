/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.platform.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProps;
import cascading.flow.FlowSession;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class HadoopPlatform is automatically loaded and injected into a {@link cascading.PlatformTestCase} instance
 * so that all *PlatformTest classes can be tested against Apache Hadoop.
 * <p/>
 * This platform works in three modes.
 * <p/>
 * Hadoop standalone mode is when Hadoop is NOT run as a cluster, and all
 * child tasks are in process and in memory of the "client" side code.
 * <p/>
 * Hadoop mini cluster mode where a cluster is created on demand using the Hadoop MiniDFSCluster and MiniMRCluster
 * utilities. When a PlatformTestCase requests to use a cluster, this is the default cluster. All properties are
 * pulled from the current CLASSPATH via the JobConf.
 * <p/>
 * Lastly remote cluster mode is enabled when the System property "mapred.jar" is set. This is a Hadoop property
 * specifying the Hadoop "job jar" to be used cluster side. This MUST be the Cascading test suite and dependencies
 * packaged in a Hadoop compatible way. This is left to be implemented by the framework using this mode. Additionally
 * these properties may optionally be set if not already in the CLASSPATH; fs.default.name and mapred.job.tracker.
 */
public class HadoopPlatform extends BaseHadoopPlatform<JobConf>
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopPlatform.class );

  public transient static MiniDFSCluster dfs;
  public transient static MiniMRCluster mr;

  public HadoopPlatform()
    {
    }

  @Override
  public FlowConnector getFlowConnector( Map<Object, Object> properties )
    {
    return new HadoopFlowConnector( properties );
    }

  @Override
  public void setNumMapTasks( Map<Object, Object> properties, int numMapTasks )
    {
    properties.put( "mapred.map.tasks", Integer.toString( numMapTasks ) );
    }

  @Override
  public void setNumReduceTasks( Map<Object, Object> properties, int numReduceTasks )
    {
    properties.put( "mapred.reduce.tasks", Integer.toString( numReduceTasks ) );
    }

  @Override
  public Integer getNumMapTasks( Map<Object, Object> properties )
    {
    if( properties.get( "mapred.map.tasks" ) == null )
      return null;

    return Integer.parseInt( properties.get( "mapred.map.tasks" ).toString() );
    }

  @Override
  public Integer getNumReduceTasks( Map<Object, Object> properties )
    {
    if( properties.get( "mapred.reduce.tasks" ) == null )
      return null;

    return Integer.parseInt( properties.get( "mapred.reduce.tasks" ).toString() );
    }

  @Override
  public JobConf getConfiguration()
    {
    return new JobConf( configuration );
    }

  @Override
  public FlowProcess getFlowProcess()
    {
    return new HadoopFlowProcess( FlowSession.NULL, (JobConf) getConfiguration(), true );
    }

  @Override
  public synchronized void setUp() throws IOException
    {
    if( configuration != null )
      return;

    if( !isUseCluster() )
      {
      LOG.info( "not using cluster" );
      configuration = new JobConf();

      // enforce the local file system in local mode
      configuration.set( "fs.default.name", "file:///" );
      configuration.set( "mapred.job.tracker", "local" );
      configuration.set( "mapreduce.jobtracker.staging.root.dir", System.getProperty( "user.dir" ) + "/build/tmp/cascading/staging" );

      String stagingDir = configuration.get( "mapreduce.jobtracker.staging.root.dir" );

      if( Util.isEmpty( stagingDir ) )
        configuration.set( "mapreduce.jobtracker.staging.root.dir", System.getProperty( "user.dir" ) + "/build/tmp/cascading/staging" );

      fileSys = FileSystem.get( configuration );
      }
    else
      {
      LOG.info( "using cluster" );

      if( Util.isEmpty( System.getProperty( "hadoop.log.dir" ) ) )
        System.setProperty( "hadoop.log.dir", "cascading-hadoop/build/test/log" );

      if( Util.isEmpty( System.getProperty( "hadoop.tmp.dir" ) ) )
        System.setProperty( "hadoop.tmp.dir", "cascading-hadoop/build/test/tmp" );

      new File( System.getProperty( "hadoop.log.dir" ) ).mkdirs(); // ignored

      JobConf conf = new JobConf();

      if( getApplicationJar() != null )
        {
        LOG.info( "using a remote cluster with jar: {}", getApplicationJar() );
        configuration = conf;

        ( (JobConf) configuration ).setJar( getApplicationJar() );

        if( !Util.isEmpty( System.getProperty( "fs.default.name" ) ) )
          {
          LOG.info( "using {}={}", "fs.default.name", System.getProperty( "fs.default.name" ) );
          configuration.set( "fs.default.name", System.getProperty( "fs.default.name" ) );
          }

        if( !Util.isEmpty( System.getProperty( "mapred.job.tracker" ) ) )
          {
          LOG.info( "using {}={}", "mapred.job.tracker", System.getProperty( "mapred.job.tracker" ) );
          configuration.set( "mapred.job.tracker", System.getProperty( "mapred.job.tracker" ) );
          }

        configuration.set( "mapreduce.user.classpath.first", "true" ); // use test dependencies
        fileSys = FileSystem.get( configuration );
        }
      else
        {
        dfs = new MiniDFSCluster( conf, 4, true, null );
        fileSys = dfs.getFileSystem();
        mr = new MiniMRCluster( 4, fileSys.getUri().toString(), 1, null, null, conf );

        configuration = mr.createJobConf();
        }

//      jobConf.set( "mapred.map.max.attempts", "1" );
//      jobConf.set( "mapred.reduce.max.attempts", "1" );
      configuration.set( "mapred.child.java.opts", "-Xmx512m" );
      configuration.setInt( "mapred.job.reuse.jvm.num.tasks", -1 );
      configuration.setInt( "jobclient.completion.poll.interval", 50 );
      configuration.setInt( "jobclient.progress.monitor.poll.interval", 50 );
      ( (JobConf) configuration ).setMapSpeculativeExecution( false );
      ( (JobConf) configuration ).setReduceSpeculativeExecution( false );
      }

    ( (JobConf) configuration ).setNumMapTasks( numMappers );
    ( (JobConf) configuration ).setNumReduceTasks( numReducers );

    Map<Object, Object> globalProperties = getGlobalProperties();

    if( logger != null )
      globalProperties.put( "log4j.logger", logger );

    FlowProps.setJobPollingInterval( globalProperties, 10 ); // should speed up tests

    HadoopPlanner.copyProperties( (JobConf) configuration, globalProperties ); // copy any external properties

    HadoopPlanner.copyJobConf( properties, (JobConf) configuration ); // put all properties on the jobconf
    }
  }
