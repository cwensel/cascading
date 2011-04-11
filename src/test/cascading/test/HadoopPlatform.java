/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopPlanner;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

public class HadoopPlatform extends TestPlatform
  {

  public transient static MiniDFSCluster dfs;
  public transient static FileSystem fileSys;
  public transient static MiniMRCluster mr;
  public transient static JobConf jobConf;
  public transient static Map<Object, Object> properties = new HashMap<Object, Object>();

  public int numMapTasks = 4;
  public int numReduceTasks = 1;

  private String logger;

  public HadoopPlatform()
    {
    logger = System.getProperty( "log4j.logger" );
    }

  public void setNumMapTasks( int numMapTasks )
    {
    this.numMapTasks = numMapTasks;
    }

  public void setNumReduceTasks( int numReduceTasks )
    {
    this.numReduceTasks = numReduceTasks;
    }

  @Override
  public void setUp() throws IOException
    {
    if( jobConf != null )
      return;

    if( !isUseCluster() )
      {
      jobConf = new JobConf();
      }
    else
      {
      System.setProperty( "test.build.data", "build" );
      new File( "build/test/log" ).mkdirs();
      System.setProperty( "hadoop.log.dir", "build/test/log" );
      Configuration conf = new Configuration();

      dfs = new MiniDFSCluster( conf, 4, true, null );
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster( 4, fileSys.getUri().toString(), 1 );

      jobConf = mr.createJobConf();

      jobConf.set( "mapred.child.java.opts", "-Xmx512m" );
      jobConf.setMapSpeculativeExecution( false );
      jobConf.setReduceSpeculativeExecution( false );
      }

    jobConf.setNumMapTasks( numMapTasks );
    jobConf.setNumReduceTasks( numReduceTasks );

    if( logger != null )
      properties.put( "log4j.logger", logger );

    HadoopFlow.setJobPollingInterval( properties, 500 ); // should speed up tests
    HadoopPlanner.setJobConf( properties, jobConf );
    }

  @Override
  public Map<Object, Object> getProperties()
    {
    return new HashMap<Object, Object>( properties );
    }

  @Override
  public void tearDown()
    {
    }

  public JobConf getJobConf()
    {
    return new JobConf( jobConf );
    }

  @Override
  public FlowConnector getFlowConnector( Map<Object, Object> properties )
    {
    return new HadoopFlowConnector( properties );
    }

  @Override
  public FlowProcess getFlowProcess()
    {
    return new HadoopFlowProcess( FlowSession.NULL, getJobConf(), true );
    }

  @Override
  public void copyFromLocal( String inputFile ) throws IOException
    {
    if( !new File( inputFile ).exists() )
      throw new FileNotFoundException( "data file not found: " + inputFile );

    if( !isUseCluster() )
      return;

    Path path = new Path( inputFile );

    if( !fileSys.exists( path ) )
      FileUtil.copy( new File( inputFile ), fileSys, path, false, jobConf );
    }

  @Override
  public void copyToLocal( String outputFile ) throws IOException
    {
    if( !isUseCluster() )
      return;

    Path path = new Path( outputFile );

    if( !fileSys.exists( path ) )
      throw new FileNotFoundException( "data file not found: " + outputFile );

    if( new File( outputFile ).exists() )
      new File( outputFile ).delete();

    FileUtil.copy( fileSys, path, new File( outputFile ), false, jobConf );
    }

  @Override
  public Tap getTextFile( Fields sourceFields, Fields sinkFields, String filename, SinkMode mode )
    {
    if( sourceFields == null )
      return new Hfs( new TextLine(), filename, mode );

    return new Hfs( new TextLine( sourceFields, sinkFields ), filename, mode );
    }

  @Override
  public Tap getDelimitedFile( Fields fields, boolean skipHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode )
    {
    return new Hfs( new TextDelimited( fields, skipHeader, delimiter, quote, types ), filename, mode );
    }
  }