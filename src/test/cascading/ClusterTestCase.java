/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.MultiMapReducePlanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 *
 */
public class ClusterTestCase extends CascadingTestCase
  {
  public static final String CLUSTER_TESTING_PROPERTY = "test.cluster.enabled";

  transient private MiniDFSCluster dfs;
  transient private FileSystem fileSys;
  transient private MiniMRCluster mr;
  transient private JobConf jobConf;
  transient private boolean enableCluster;
  transient private Map<Object, Object> properties = new HashMap<Object, Object>();

  int numMapTasks = 4;
  int numReduceTasks = 1;

  private String logger;

  public ClusterTestCase( String string, boolean enableCluster )
    {
    super( string );
    this.enableCluster = Boolean.parseBoolean( System.getProperty( CLUSTER_TESTING_PROPERTY, Boolean.toString( enableCluster ) ) );
    this.logger = System.getProperty( "log4j.logger" );
    }

  public ClusterTestCase( String string, boolean enableCluster, int numMapTasks, int numReduceTasks )
    {
    this( string, enableCluster );
    this.numMapTasks = numMapTasks;
    this.numReduceTasks = numReduceTasks;
    }

  public ClusterTestCase( String string )
    {
    super( string );
    }

  public ClusterTestCase()
    {
    }

  public void setUp() throws IOException
    {
    if( !enableCluster )
      {
      jobConf = new JobConf();
      }
    else
      {
      System.setProperty( "test.build.data", "build" );
      System.setProperty( "hadoop.log.dir", "build/test/log" );
      Configuration conf = new Configuration();

      dfs = new MiniDFSCluster( conf, 4, true, null );
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster( 4, fileSys.getUri().toString(), 1 );

      jobConf = mr.createJobConf();

      jobConf.set( "mapred.child.java.opts", "-Xmx512m" );
      jobConf.set( "mapred.map.tasks.speculative.execution", "false" );
      jobConf.set( "mapred.reduce.tasks.speculative.execution", "false" );
      }

    jobConf.setNumMapTasks( numMapTasks );
    jobConf.setNumReduceTasks( numReduceTasks );

    if( logger != null )
      properties.put( "log4j.logger", logger );

    MultiMapReducePlanner.setJobConf( properties, jobConf );
    }

  public Map<Object, Object> getProperties()
    {
    return new HashMap<Object, Object>( properties );
    }

  protected void copyFromLocal( String inputFile ) throws IOException
    {
    if( !enableCluster )
      return;

    Path path = new Path( inputFile );

    if( !fileSys.exists( path ) )
      FileUtil.copy( new File( inputFile ), fileSys, path, false, jobConf );
    }

  public void tearDown() throws IOException
    {
    try
      {
      if( fileSys != null )
        fileSys.close();
      }
    catch( IOException exception )
      {
      }

    try
      {
      if( dfs != null )
        dfs.shutdown();
      }
    catch( Exception exception )
      {
      }

    try
      {
      if( mr != null )
        mr.shutdown();
      }
    catch( Exception exception )
      {
      }
    }
  }
