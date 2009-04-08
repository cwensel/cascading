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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 *
 */
public class FlowOutputCommitter extends FileOutputCommitter
  {
  public static String[] getBypassOutputPaths( JobConf jobConf, boolean isMapper )
    {
    String name = "cascading.bypass.output.paths." + ( isMapper ? "mapper" : "reducer" );
    String[] list = jobConf.getStrings( name );

    if( list == null )
      return new String[0];

    String[] result = new String[list.length];

    for( int i = 0; i < list.length; i++ )
      result[ i ] = StringUtils.unEscapeString( list[ i ] );

    return result;
    }

  public static void addBypassOutputPaths( JobConf jobConf, boolean isMapper, String path )
    {
    path = StringUtils.escapeString( path );

    String name = "cascading.bypass.output.paths." + ( isMapper ? "mapper" : "reducer" );
    String paths = jobConf.get( name );

    jobConf.set( name, paths == null ? path : path + StringUtils.COMMA_STR + paths );
    }

  @Override
  public void setupJob( final JobContext jobContext ) throws IOException
    {
    invoke( jobContext.getJobConf(), true, new Predicate()
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
    invoke( jobContext.getJobConf(), true, new Predicate()
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
    invoke( taskContext.getJobConf(), false, new Predicate()
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
    JobConf jobConf = taskContext.getJobConf();
    boolean result = newInstance( jobConf ).needsTaskCommit( taskContext );

    return result || getBypassOutputPaths( jobConf, jobConf.getBoolean( "mapred.task.is.map", true ) ).length != 0;
    }

  @Override
  public void commitTask( final TaskAttemptContext taskContext ) throws IOException
    {
    invoke( taskContext.getJobConf(), false, new Predicate()
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
      invoke( taskContext.getJobConf(), false, new Predicate()
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

  private void invoke( JobConf jobConf, boolean bypassAll, Predicate predicate ) throws IOException
    {
    Path originalPath = FileOutputFormat.getOutputPath( jobConf );

    if( originalPath == null )
      return;

    predicate.invoke( newInstance( jobConf ) );

    try
      {
      if( bypassAll )
        {
        invokeOn( jobConf, predicate, getBypassOutputPaths( jobConf, true ) );
        invokeOn( jobConf, predicate, getBypassOutputPaths( jobConf, false ) );
        }
      else
        {
        invokeOn( jobConf, predicate, getBypassOutputPaths( jobConf, jobConf.getBoolean( "mapred.task.is.map", true ) ) );
        }
      }
    finally
      {
      FileOutputFormat.setOutputPath( jobConf, originalPath );
      }
    }

  private void invokeOn( JobConf jobConf, Predicate predicate, String[] outputPaths ) throws IOException
    {
    for( int i = 0; i < outputPaths.length; i++ )
      {
      FileOutputFormat.setOutputPath( jobConf, new Path( outputPaths[ i ] ) );

      predicate.invoke( newInstance( jobConf ) );
      }
    }

  private FileOutputCommitter newInstance( JobConf jobConf )
    {
    return ReflectionUtils.newInstance( FileOutputCommitter.class, jobConf );
    }
  }
