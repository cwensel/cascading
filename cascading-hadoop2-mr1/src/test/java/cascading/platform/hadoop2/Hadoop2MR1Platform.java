/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.platform.hadoop2;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProps;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1Planner;
import cascading.platform.hadoop.BaseHadoopPlatform;
import cascading.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class Hadoop2Platform is automatically loaded and injected into a {@link cascading.PlatformTestCase} instance
 * so that all *PlatformTest classes can be tested against Apache Hadoop 2.x.
 */
public class Hadoop2MR1Platform extends BaseHadoopPlatform
  {
  private static final Logger LOG = LoggerFactory.getLogger( Hadoop2MR1Platform.class );
  private transient static MiniDFSCluster dfs;
  private transient static MiniMRClientCluster mr;

  public Hadoop2MR1Platform()
    {
    }

  @Override
  public String getName()
    {
    return "hadoop2-mr1";
    }

  @Override
  public FlowConnector getFlowConnector( Map<Object, Object> properties )
    {
    return new Hadoop2MR1FlowConnector( properties );
    }

  @Override
  public void setNumMapTasks( Map<Object, Object> properties, int numMapTasks )
    {
    properties.put( "mapreduce.job.maps", Integer.toString( numMapTasks ) );
    }

  @Override
  public void setNumReduceTasks( Map<Object, Object> properties, int numReduceTasks )
    {
    properties.put( "mapreduce.job.reduces", Integer.toString( numReduceTasks ) );
    }

  @Override
  public Integer getNumMapTasks( Map<Object, Object> properties )
    {
    if( properties.get( "mapreduce.job.maps" ) == null )
      return null;

    return Integer.parseInt( properties.get( "mapreduce.job.maps" ).toString() );
    }

  @Override
  public Integer getNumReduceTasks( Map<Object, Object> properties )
    {
    if( properties.get( "mapreduce.job.reduces" ) == null )
      return null;

    return Integer.parseInt( properties.get( "mapreduce.job.reduces" ).toString() );
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

      // enforce settings to make local mode behave the same across distributions
      jobConf.set( "fs.defaultFS", "file:///" );
      jobConf.set( "mapreduce.framework.name", "local" );
      jobConf.set( "mapreduce.jobtracker.staging.root.dir", System.getProperty( "user.dir" ) + "/" + "build/tmp/cascading/staging" );

      fileSys = FileSystem.get( jobConf );
      }
    else
      {
      LOG.info( "using cluster" );

      if( Util.isEmpty( System.getProperty( "hadoop.log.dir" ) ) )
        System.setProperty( "hadoop.log.dir", "build/test/log" );

      if( Util.isEmpty( System.getProperty( "hadoop.tmp.dir" ) ) )
        System.setProperty( "hadoop.tmp.dir", "build/test/tmp" );

      new File( System.getProperty( "hadoop.log.dir" ) ).mkdirs(); // ignored

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

        if( !Util.isEmpty( System.getProperty( "fs.defaultFS" ) ) )
          {
          LOG.info( "using {}={}", "fs.defaultFS", System.getProperty( "fs.defaultFS" ) );
          jobConf.set( "fs.defaultFS", System.getProperty( "fs.defaultFS" ) );
          }

        if( !Util.isEmpty( System.getProperty( "yarn.resourcemanager.address" ) ) )
          {
          LOG.info( "using {}={}", "yarn.resourcemanager.address", System.getProperty( "yarn.resourcemanager.address" ) );
          jobConf.set( "yarn.resourcemanager.address", System.getProperty( "yarn.resourcemanager.address" ) );
          }

        if( !Util.isEmpty( System.getProperty( "mapreduce.jobhistory.address" ) ) )
          {
          LOG.info( "using {}={}", "mapreduce.jobhistory.address", System.getProperty( "mapreduce.jobhistory.address" ) );
          jobConf.set( "mapreduce.jobhistory.address", System.getProperty( "mapreduce.jobhistory.address" ) );
          }

        jobConf.set( "mapreduce.user.classpath.first", "true" ); // use test dependencies
        jobConf.set( "mapreduce.framework.name", "yarn" );
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

      jobConf.set( "mapred.child.java.opts", "-Xmx512m" );
      jobConf.setInt( "mapreduce.job.jvm.numtasks", -1 );
      jobConf.setInt( "mapreduce.client.completion.pollinterval", 50 );
      jobConf.setInt( "mapreduce.client.progressmonitor.pollinterval", 50 );
      jobConf.setBoolean( "mapreduce.map.speculative", false );
      jobConf.setBoolean( "mapreduce.reduce.speculative", false );
      }

    jobConf.setInt( "mapreduce.job.maps", numMapTasks );
    jobConf.setInt( "mapreduce.job.reduces", numReduceTasks );

    Map<Object, Object> globalProperties = getGlobalProperties();

    if( logger != null )
      globalProperties.put( "log4j.logger", logger );

    FlowProps.setJobPollingInterval( globalProperties, 10 ); // should speed up tests

    Hadoop2MR1Planner.copyProperties( jobConf, globalProperties ); // copy any external properties

    Hadoop2MR1Planner.copyConfiguration( properties, jobConf ); // put all properties on the jobconf
    }
  }
