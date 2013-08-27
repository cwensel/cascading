/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.hadoop.platform;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProps;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.platform.hadoop.BaseHadoopPlatform;
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
 */
public class HadoopPlatform extends BaseHadoopPlatform
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
  public synchronized void setUp() throws IOException
    {
    if( jobConf != null )
      return;

    if( !isUseCluster() )
      {
      LOG.info( "not using cluster" );
      jobConf = new JobConf();
      fileSys = FileSystem.get( jobConf );
      }
    else
      {
      LOG.info( "using cluster" );

      if( Util.isEmpty( System.getProperty( "hadoop.log.dir" ) ) )
        System.setProperty( "hadoop.log.dir", "build/test/log" );

      if( Util.isEmpty( System.getProperty( "hadoop.tmp.dir" ) ) )
        System.setProperty( "hadoop.tmp.dir", "build/test/tmp" );

      new File( System.getProperty( "hadoop.log.dir" ) ).mkdirs();

      JobConf conf = new JobConf();

      conf.setInt( "mapred.job.reuse.jvm.num.tasks", -1 );

      dfs = new MiniDFSCluster( conf, 4, true, null );
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster( 4, fileSys.getUri().toString(), 1, null, null, conf );

      jobConf = mr.createJobConf();

      jobConf.set( "mapred.child.java.opts", "-Xmx512m" );
      jobConf.setInt( "mapred.job.reuse.jvm.num.tasks", -1 );
      jobConf.setInt( "jobclient.completion.poll.interval", 50 );
      jobConf.setInt( "jobclient.progress.monitor.poll.interval", 50 );
      ( (JobConf) jobConf ).setMapSpeculativeExecution( false );
      ( (JobConf) jobConf ).setReduceSpeculativeExecution( false );
      }

    ( (JobConf) jobConf ).setNumMapTasks( numMapTasks );
    ( (JobConf) jobConf ).setNumReduceTasks( numReduceTasks );

    Map<Object, Object> globalProperties = getGlobalProperties();

    if( logger != null )
      globalProperties.put( "log4j.logger", logger );

    FlowProps.setJobPollingInterval( globalProperties, 10 ); // should speed up tests

    HadoopPlanner.copyProperties( (JobConf) jobConf, globalProperties ); // copy any external properties

    HadoopPlanner.copyJobConf( properties, (JobConf) jobConf ); // put all properties on the jobconf
    }
  }