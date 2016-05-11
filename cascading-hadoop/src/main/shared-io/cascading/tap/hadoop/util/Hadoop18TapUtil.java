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

package cascading.tap.hadoop.util;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.Tap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.hadoop.util.HadoopUtil.asJobConfInstance;

public class Hadoop18TapUtil
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( Hadoop18TapUtil.class );

  /** The Hadoop temporary path used to prevent collisions */
  public static final String TEMPORARY_PATH = "_temporary";

  private static final Map<String, AtomicInteger> pathCounts = new HashMap<String, AtomicInteger>();

  /**
   * should only be called if not in a Flow
   *
   * @param conf
   * @throws IOException
   */
  public static void setupJob( Configuration conf ) throws IOException
    {
    Path outputPath = FileOutputFormat.getOutputPath( asJobConfInstance( conf ) );

    if( outputPath == null )
      return;

    if( getFSSafe( conf, outputPath ) == null )
      return;

    String taskID = conf.get( "mapred.task.id", conf.get( "mapreduce.task.id" ) );

    if( taskID == null ) // need to stuff a fake id
      {
      String mapper = conf.getBoolean( "mapred.task.is.map", conf.getBoolean( "mapreduce.task.is.map", true ) ) ? "m" : "r";
      String value = String.format( "attempt_%012d_0000_%s_000000_0", (int) Math.rint( System.currentTimeMillis() ), mapper );
      conf.set( "mapred.task.id", value );
      conf.set( "mapreduce.task.id", value );
      }

    makeTempPath( conf );

    if( writeDirectlyToWorkingPath( conf, outputPath ) )
      {
      LOG.info( "writing directly to output path: {}", outputPath );
      setWorkOutputPath( conf, outputPath );
      return;
      }

    // "mapred.work.output.dir"
    Path taskOutputPath = getTaskOutputPath( conf );
    setWorkOutputPath( conf, taskOutputPath );
    }

  public static synchronized void setupTask( Configuration conf ) throws IOException
    {
    String workpath = conf.get( "mapred.work.output.dir" );

    if( workpath == null )
      return;

    FileSystem fs = getFSSafe( conf, new Path( workpath ) );

    if( fs == null )
      return;

    String taskId = conf.get( "mapred.task.id", conf.get( "mapreduce.task.id" ) );

    LOG.info( "setting up task: '{}' - {}", taskId, workpath );

    AtomicInteger integer = pathCounts.get( workpath );

    if( integer == null )
      {
      integer = new AtomicInteger();
      pathCounts.put( workpath, integer );
      }

    integer.incrementAndGet();
    }

  public static boolean needsTaskCommit( Configuration conf ) throws IOException
    {
    String workpath = conf.get( "mapred.work.output.dir" );

    if( workpath == null )
      return false;

    Path taskOutputPath = new Path( workpath );

    if( taskOutputPath != null )
      {
      FileSystem fs = getFSSafe( conf, taskOutputPath );

      if( fs == null )
        return false;

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
  public static void commitTask( Configuration conf ) throws IOException
    {
    Path taskOutputPath = new Path( conf.get( "mapred.work.output.dir" ) );

    FileSystem fs = getFSSafe( conf, taskOutputPath );

    if( fs == null )
      return;

    AtomicInteger integer = pathCounts.get( taskOutputPath.toString() );

    if( integer.decrementAndGet() != 0 )
      return;

    String taskId = conf.get( "mapred.task.id", conf.get( "mapreduce.task.id" ) );

    LOG.info( "committing task: '{}' - {}", taskId, taskOutputPath );

    if( taskOutputPath != null )
      {
      if( writeDirectlyToWorkingPath( conf, taskOutputPath ) )
        return;

      if( fs.exists( taskOutputPath ) )
        {
        Path jobOutputPath = taskOutputPath.getParent().getParent();
        // Move the task outputs to their final place
        moveTaskOutputs( conf, fs, jobOutputPath, taskOutputPath );

        // Delete the temporary task-specific output directory
        if( !fs.delete( taskOutputPath, true ) )
          LOG.info( "failed to delete the temporary output directory of task: '{}' - {}", taskId, taskOutputPath );

        LOG.info( "saved output of task '{}' to {}", taskId, jobOutputPath );
        }
      }
    }

  /**
   * Called from flow step to remove temp dirs
   *
   * @param conf
   * @throws IOException
   */
  public static void cleanupTapMetaData( Configuration conf, Tap tap ) throws IOException
    {
    cleanTempPath( conf, new Path( tap.getIdentifier() ) );
    }

  /**
   * May only be called once. should only be called if not in a flow
   *
   * @param conf
   */
  public static void cleanupJob( Configuration conf ) throws IOException
    {
    if( HadoopUtil.isInflow( conf ) )
      return;

    Path outputPath = FileOutputFormat.getOutputPath( asJobConfInstance( conf ) );

    cleanTempPath( conf, outputPath );
    }

  private static synchronized void cleanTempPath( Configuration conf, Path outputPath ) throws IOException
    {
    // do the clean up of temporary directory

    if( outputPath != null )
      {
      FileSystem fileSys = getFSSafe( conf, outputPath );

      if( fileSys == null )
        return;

      if( !fileSys.exists( outputPath ) )
        return;

      Path tmpDir = new Path( outputPath, TEMPORARY_PATH );

      LOG.info( "deleting temp path {}", tmpDir );

      if( fileSys.exists( tmpDir ) )
        fileSys.delete( tmpDir, true );
      }
    }

  private static FileSystem getFSSafe( Configuration conf, Path tmpDir )
    {
    try
      {
      return tmpDir.getFileSystem( conf );
      }
    catch( IOException e )
      {
      // ignore
      }

    return null;
    }

  private static Path getTaskOutputPath( Configuration conf )
    {
    String taskId = conf.get( "mapred.task.id", conf.get( "mapreduce.task.id" ) );

    Path p = new Path( FileOutputFormat.getOutputPath( asJobConfInstance( conf ) ), TEMPORARY_PATH + Path.SEPARATOR + "_" + taskId );

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

  static void setWorkOutputPath( Configuration conf, Path outputDir )
    {
    outputDir = new Path( asJobConfInstance( conf ).getWorkingDirectory(), outputDir );
    conf.set( "mapred.work.output.dir", outputDir.toString() );
    }

  public static void makeTempPath( Configuration conf ) throws IOException
    {
    // create job specific temporary directory in output path
    Path outputPath = FileOutputFormat.getOutputPath( asJobConfInstance( conf ) );

    if( outputPath != null )
      {
      Path tmpDir = new Path( outputPath, TEMPORARY_PATH );
      FileSystem fileSys = tmpDir.getFileSystem( conf );

      if( !fileSys.exists( tmpDir ) && !fileSys.mkdirs( tmpDir ) )
        LOG.error( "mkdirs failed to create {}", tmpDir );
      }
    }

  private static void moveTaskOutputs( Configuration conf, FileSystem fs, Path jobOutputDir, Path taskOutput ) throws IOException
    {
    String taskId = conf.get( "mapred.task.id", conf.get( "mapreduce.task.id" ) );

    if( fs.isFile( taskOutput ) )
      {
      Path finalOutputPath = getFinalPath( jobOutputDir, taskOutput, getTaskOutputPath( conf ) );
      if( !fs.rename( taskOutput, finalOutputPath ) )
        {
        if( !fs.delete( finalOutputPath, true ) )
          throw new IOException( "Failed to delete earlier output of task: " + taskId );

        if( !fs.rename( taskOutput, finalOutputPath ) )
          throw new IOException( "Failed to save output of task: " + taskId );
        }

      LOG.debug( "Moved {} to {}", taskOutput, finalOutputPath );
      }
    else if( fs.getFileStatus( taskOutput ).isDir() )
      {
      FileStatus[] paths = fs.listStatus( taskOutput );
      Path finalOutputPath = getFinalPath( jobOutputDir, taskOutput, getTaskOutputPath( conf ) );
      fs.mkdirs( finalOutputPath );
      if( paths != null )
        {
        for( FileStatus path : paths )
          moveTaskOutputs( conf, fs, jobOutputDir, path.getPath() );
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

  /** used in AWS EMR to disable temp paths on some file systems, s3. */
  private static boolean writeDirectlyToWorkingPath( Configuration conf, Path path )
    {
    FileSystem fs = getFSSafe( conf, path );

    if( fs == null )
      return false;

    boolean result = conf.getBoolean( "mapred.output.direct." + fs.getClass().getSimpleName(), false );

    if( result )
      LOG.info( "output direct is enabled for this fs: " + fs.getName() );

    return result;
    }
  }
