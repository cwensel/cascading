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

package cascading.tap.hadoop;

import java.io.IOException;

import cascading.tap.Tap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 *
 */
public class Hadoop19TapUtil
  {

  static TaskAttemptContext getAttemptContext( Configuration conf )
    {
    TaskAttemptID taskAttemptID = getTaskAttemptId( conf );

    return new TaskAttemptContext( conf, taskAttemptID );
    }

  static TaskAttemptID getTaskAttemptId( Configuration conf )
    {
    if( conf.get( "mapred.task.id" ) == null ) // need to stuff a fake id
      {
      String mapper = conf.getBoolean( "mapred.task.is.map", true ) ? "m" : "r";
      conf.set( "mapred.task.id", String.format( "attempt_%012d_0000_%s_000000_0", (int) Math.rint( System.currentTimeMillis() ), mapper ) );
      }

    return TaskAttemptID.forName( conf.get( "mapred.task.id" ) );
    }

  /**
   * should only be called if not in a Flow
   *
   * @param job
   * @throws IOException
   */
  public static void setupJob( Job job ) throws IOException
    {

//    OutputCommitter outputCommitter = job.getOutputCommitter();
//
//    if( !( outputCommitter instanceof FlowOutputCommitter ) )
//      outputCommitter.setupJob( getAttemptContext( job.getConfiguration() ) );
    }


  static synchronized void setupTask( Job conf ) throws IOException
    {
    }

  public static boolean needsTaskCommit( Job conf ) throws IOException
    {
    return false;
    }

  /**
   * copies all files from the taskoutputpath to the outputpath
   *
   * @param conf
   */
  public static void commitTask( Job conf ) throws IOException
    {
    }

  /**
   * Called from flow step to remove temp dirs
   *
   * @param conf
   * @throws IOException
   */
  public static void cleanupTap( Job conf, Tap tap ) throws IOException
    {
    }

  /**
   * May only be called once. should only be called if not in a flow
   *
   * @param conf
   */
  static void cleanupJob( Job conf ) throws IOException
    {
    // only execute if not inside an executing flow
//    OutputCommitter outputCommitter = conf.getOutputCommitter();
//
//    if( !( outputCommitter instanceof FlowOutputCommitter ) )
//      {
//      TaskAttemptContext taskAttemptContext = getAttemptContext( conf );
//
//      if( outputCommitter.needsTaskCommit( taskAttemptContext ) )
//        {
//        outputCommitter.commitTask( taskAttemptContext );
//        outputCommitter.cleanupJob( taskAttemptContext );
//        }
//      }
    }

  }
