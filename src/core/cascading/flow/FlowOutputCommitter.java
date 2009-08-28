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

package cascading.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 *
 */
public class FlowOutputCommitter extends FileOutputCommitter
  {
  public static String[] getBypassOutputPaths( JobContext context, boolean isMapper )
    {
    String name = "cascading.bypass.output.paths." + ( isMapper ? "mapper" : "reducer" );
    String[] list = context.getConfiguration().getStrings( name );

    if( list == null )
      return new String[0];

    String[] result = new String[list.length];

    for( int i = 0; i < list.length; i++ )
      result[ i ] = StringUtils.unEscapeString( list[ i ] );

    return result;
    }

  public static void addBypassOutputPaths( JobContext context, boolean isMapper, String path )
    {
    path = StringUtils.escapeString( path );

    String name = "cascading.bypass.output.paths." + ( isMapper ? "mapper" : "reducer" );
    String paths = context.getConfiguration().get( name );

    context.getConfiguration().set( name, paths == null ? path : path + StringUtils.COMMA_STR + paths );
    }

  /**
   * Create a file output committer
   *
   * @param outputPath the job's output path
   * @param context    the task's context
   * @throws java.io.IOException
   */
  public FlowOutputCommitter( Path outputPath, TaskAttemptContext context )
    throws IOException
    {
    super( outputPath, context );
    }

  @Override
  public void setupJob( final JobContext jobContext ) throws IOException
    {
    invoke( jobContext, true, new Predicate()
    {
    @Override
    public void invoke( OutputCommitter outputCommitter ) throws IOException
      {
      outputCommitter.setupJob( jobContext );
      }
    } );
    }

  @Override
  public void cleanupJob( final JobContext jobContext ) throws IOException
    {
    invoke( jobContext, true, new Predicate()
    {
    @Override
    public void invoke( OutputCommitter outputCommitter ) throws IOException
      {
      outputCommitter.cleanupJob( jobContext );
      }
    } );
    }

  @Override
  public void setupTask( final TaskAttemptContext taskContext ) throws IOException
    {
    invoke( taskContext, false, new Predicate()
    {
    @Override
    public void invoke( OutputCommitter outputCommitter ) throws IOException
      {
      outputCommitter.setupTask( taskContext );
      }
    } );
    }

  @Override
  public boolean needsTaskCommit( TaskAttemptContext taskContext ) throws IOException
    {
    Configuration jobConf = taskContext.getConfiguration();
    boolean result = newInstance( jobConf ).needsTaskCommit( taskContext );

    return result || getBypassOutputPaths( taskContext, jobConf.getBoolean( "mapred.task.is.map", true ) ).length != 0;
    }

  @Override
  public Path getWorkPath() throws IOException
    {
    return super.getWorkPath();
    }

  @Override
  public void commitTask( final TaskAttemptContext taskContext ) throws IOException
    {
    invoke( taskContext, false, new Predicate()
    {
    @Override
    public void invoke( OutputCommitter outputCommitter ) throws IOException
      {
      outputCommitter.commitTask( taskContext );
      }
    } );
    }

  @Override
  public void abortTask( final TaskAttemptContext taskContext )
    {
    try
      {
      invoke( taskContext, false, new Predicate()
      {
      @Override
      public void invoke( OutputCommitter outputCommitter ) throws IOException
        {
        outputCommitter.abortTask( taskContext );
        }
      } );
      }
    catch( IOException exception )
      {
      // do nothing, never thrown
      }
    }

  public interface Predicate
    {
    void invoke( OutputCommitter outputCommitter ) throws IOException;
    }

  private void invoke( JobContext jobContext, boolean bypassAll, Predicate predicate ) throws IOException
    {
    Job job = new Job( jobContext.getConfiguration() );
    Path originalPath = FileOutputFormat.getOutputPath( jobContext );

    if( originalPath == null )
      return;

    predicate.invoke( newInstance( jobContext.getConfiguration() ) );

    if( bypassAll )
      {
      invokeOn( job, predicate, getBypassOutputPaths( job, true ) );
      invokeOn( job, predicate, getBypassOutputPaths( job, false ) );
      }
    else
      {
      invokeOn( job, predicate, getBypassOutputPaths( job, job.getConfiguration().getBoolean( "mapred.task.is.map", true ) ) );
      }
    }

  private void invokeOn( Job jobConf, Predicate predicate, String[] outputPaths ) throws IOException
    {
    for( int i = 0; i < outputPaths.length; i++ )
      {
      FileOutputFormat.setOutputPath( jobConf, new Path( outputPaths[ i ] ) );

      predicate.invoke( newInstance( jobConf.getConfiguration() ) );
      }
    }

  private FileOutputCommitter newInstance( Configuration jobConf )
    {
    return ReflectionUtils.newInstance( FileOutputCommitter.class, jobConf );
    }
  }
