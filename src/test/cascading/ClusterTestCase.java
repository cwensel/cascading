/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 *
 */
public class ClusterTestCase extends CascadingTestCase
  {
  transient private MiniDFSCluster dfs;
  transient private FileSystem fileSys;
  transient private MiniMRCluster mr;
  transient protected JobConf jobConf;
  transient private boolean enableCluster;

  public ClusterTestCase( String string, boolean enableCluster )
    {
    super( string );
    this.enableCluster = enableCluster;
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
      return;
      }

    System.setProperty( "test.build.data", "build" );
    System.setProperty( "hadoop.log.dir", "build/test/log" );
    Configuration conf = new Configuration();

    dfs = new MiniDFSCluster( conf, 4, true, null );
    fileSys = dfs.getFileSystem();
    mr = new MiniMRCluster( 4, fileSys.getName(), 1 );

    jobConf = mr.createJobConf();
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
    if( fileSys != null )
      fileSys.close();

    if( dfs != null )
      dfs.shutdown();

    if( mr != null )
      mr.shutdown();
    }
  }
