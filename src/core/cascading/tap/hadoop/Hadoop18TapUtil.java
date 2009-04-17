/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tap.hadoop;

import cascading.tap.Tap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** This class embodies the dependencies between Hadoop versions. It is copyright and licensed under Apache */
public class Hadoop18TapUtil
  {
  private static final Logger LOG = Logger.getLogger( Hadoop18TapUtil.class );
  private static final String TEMPORARY_PATH = "_temporary";

  private static Map<String, AtomicInteger> pathCounts = new HashMap<String, AtomicInteger>();

  /**
   * should only be called if not in a Flow
   *
   * @param conf
   * @throws IOException
   */
  public static void setupJob( JobConf conf ) throws IOException
    {
    if( FileOutputFormat.getOutputPath( conf ) == null )
      return;

    if( conf.get( "mapred.task.id" ) == null ) // need to stuff a fake id
      {
      String mapper = conf.getBoolean( "mapred.task.is.map", true ) ? "m" : "r";
      conf.set( "mapred.task.id", String.format( "attempt_%012d_0000_%s_000000_0", (int) Math.rint( System.currentTimeMillis() ), mapper ) );
      }

    makeTempPath( conf );

    // "mapred.work.output.dir"
    Path taskOutputPath = getTaskOutputPath( conf );
    setWorkOutputPath( conf, taskOutputPath );
    }

  static synchronized void setupTask( JobConf conf ) throws IOException
    {
    String workpath = conf.get( "mapred.work.output.dir" );

    if( workpath == null )
      return;

    AtomicInteger integer = pathCounts.get( workpath );

    if( integer == null )
      {
      integer = new AtomicInteger();
      pathCounts.put( workpath, integer );
      }

    integer.incrementAndGet();
    }

  public static boolean needsTaskCommit( JobConf conf ) throws IOException
    {
    Path taskOutputPath = getTaskOutputPath( conf );

    if( taskOutputPath != null )
      {
      FileSystem fs = taskOutputPath.getFileSystem( conf );

      if( fs.exists( taskOutputPath ) )
        return true;
      }

    return false;
    }

  /**
   * copies all files from the taskoutputpath to the outputpath
   *
   * @param conf
   */
  public static void commitTask( JobConf conf ) throws IOException
    {
    Path taskOutputPath = getTaskOutputPath( conf );

    AtomicInteger integer = pathCounts.get( taskOutputPath.toString() );

    if( integer.decrementAndGet() != 0 )
      return;

    String taskId = conf.get( "mapred.task.id" );

    if( taskOutputPath != null )
      {
      FileSystem fs = taskOutputPath.getFileSystem( conf );

      if( fs.exists( taskOutputPath ) )
        {
        Path jobOutputPath = taskOutputPath.getParent().getParent();
        // Move the task outputs to their final place
        moveTaskOutputs( conf, fs, jobOutputPath, taskOutputPath );

        // Delete the temporary task-specific output directory
        if( !fs.delete( taskOutputPath, true ) )
          {
          LOG.info( "Failed to delete the temporary output" + " directory of task: " + taskId + " - " + taskOutputPath );
          }

        LOG.info( "Saved output of task '" + taskId + "' to " + jobOutputPath );
        }
      }
    }

  /**
   * Called from flow step to remove temp dirs
   *
   * @param conf
   * @throws IOException
   */
  public static void cleanupTap( JobConf conf, Tap tap ) throws IOException
    {
    cleanTempPath( conf, tap.getPath() );
    }

  /**
   * May only be called once. should only be called if not in a flow
   *
   * @param conf
   */
  static void cleanupJob( JobConf conf ) throws IOException
    {
    if( isInflow( conf ) )
      return;

    Path outputPath = FileOutputFormat.getOutputPath( conf );

    cleanTempPath( conf, outputPath );
    }

  private static void cleanTempPath( JobConf conf, Path outputPath ) throws IOException
    {
    // do the clean up of temporary directory

    if( outputPath != null )
      {
      Path tmpDir = new Path( outputPath, TEMPORARY_PATH );

      FileSystem fileSys = tmpDir.getFileSystem( conf );

      if( fileSys.exists( tmpDir ) )
        fileSys.delete( tmpDir, true );
      }
    }

  static boolean isInflow( JobConf conf )
    {
    return conf.get( "cascading.flow.step" ) != null;
    }

  private static Path getTaskOutputPath( JobConf conf )
    {
    String taskId = conf.get( "mapred.task.id" );

    Path p = new Path( FileOutputFormat.getOutputPath( conf ), TEMPORARY_PATH + Path.SEPARATOR + "_" + taskId );

    try
      {
      FileSystem fs = p.getFileSystem( conf );
      return p.makeQualified( fs );
      }
    catch( IOException ie )
      {
      return p;
      }
    }

  static void setWorkOutputPath( JobConf conf, Path outputDir )
    {
    outputDir = new Path( conf.getWorkingDirectory(), outputDir );
    conf.set( "mapred.work.output.dir", outputDir.toString() );
    }

  public static void makeTempPath( JobConf conf ) throws IOException
    {
    // create job specific temporary directory in output path
    Path outputPath = FileOutputFormat.getOutputPath( conf );

    if( outputPath != null )
      {
      Path tmpDir = new Path( outputPath, TEMPORARY_PATH );
      FileSystem fileSys = tmpDir.getFileSystem( conf );

      if( !fileSys.exists( tmpDir ) && !fileSys.mkdirs( tmpDir ) )
        {
        LOG.error( "Mkdirs failed to create " + tmpDir.toString() );
        }
      }
    }

  private static void moveTaskOutputs( JobConf conf, FileSystem fs, Path jobOutputDir, Path taskOutput ) throws IOException
    {
    String taskId = conf.get( "mapred.task.id" );

    if( fs.isFile( taskOutput ) )
      {
      Path finalOutputPath = getFinalPath( jobOutputDir, taskOutput, getTaskOutputPath( conf ) );
      if( !fs.rename( taskOutput, finalOutputPath ) )
        {
        if( !fs.delete( finalOutputPath, true ) )
          {
          throw new IOException( "Failed to delete earlier output of task: " + taskId );
          }
        if( !fs.rename( taskOutput, finalOutputPath ) )
          {
          throw new IOException( "Failed to save output of task: " + taskId );
          }
        }
      LOG.debug( "Moved " + taskOutput + " to " + finalOutputPath );
      }
    else if( fs.getFileStatus( taskOutput ).isDir() )
      {
      FileStatus[] paths = fs.listStatus( taskOutput );
      Path finalOutputPath = getFinalPath( jobOutputDir, taskOutput, getTaskOutputPath( conf ) );
      fs.mkdirs( finalOutputPath );
      if( paths != null )
        {
        for( FileStatus path : paths )
          {
          moveTaskOutputs( conf, fs, jobOutputDir, path.getPath() );
          }
        }
      }
    }

  private static Path getFinalPath( Path jobOutputDir, Path taskOutput, Path taskOutputPath ) throws IOException
    {
    URI taskOutputUri = taskOutput.toUri();
    URI relativePath = taskOutputPath.toUri().relativize( taskOutputUri );
    if( taskOutputUri == relativePath )
      {//taskOutputPath is not a parent of taskOutput
      throw new IOException( "Can not get the relative path: base = " + taskOutputPath + " child = " + taskOutput );
      }
    if( relativePath.getPath().length() > 0 )
      {
      return new Path( jobOutputDir, relativePath.getPath() );
      }
    else
      {
      return jobOutputDir;
      }
    }
  }
