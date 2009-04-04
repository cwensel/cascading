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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 *
 */
public class FlowOutputCommitter extends OutputCommitter
  {
  public static String[] getBypassOutputPaths( JobConf jobConf )
    {
    String[] list = jobConf.getStrings( "cascading.bypass.output.paths" );
    String[] result = new String[list.length];

    for( int i = 0; i < list.length; i++ )
      result[ i ] = StringUtils.unEscapeString( list[ i ] );

    return result;
    }

  public static void addBypassOutputPaths( JobConf jobConf, String path )
    {
    path = StringUtils.escapeString( path );

    String paths = jobConf.get( "cascading.bypass.output.paths" );
    jobConf.set( "cascading.bypass.output.paths", paths == null ? path : path + StringUtils.COMMA_STR + paths );
    }

  @Override
  public void setupJob( final JobContext jobContext ) throws IOException
    {
    invoke( jobContext.getJobConf(), new Predicate()
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
    invoke( jobContext.getJobConf(), new Predicate()
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
    invoke( taskContext.getJobConf(), new Predicate()
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
    return newInstance( taskContext.getJobConf() ).needsTaskCommit( taskContext );
    }

  @Override
  public void commitTask( final TaskAttemptContext taskContext ) throws IOException
    {
    invoke( taskContext.getJobConf(), new Predicate()
    {
    @Override
    public void invoke( OutputCommitter outputCommitter ) throws IOException
      {
      outputCommitter.commitTask( taskContext );
      }
    } );
    }

  @Override
  public void abortTask( final TaskAttemptContext taskContext ) throws IOException
    {
    invoke( taskContext.getJobConf(), new Predicate()
    {
    @Override
    public void invoke( OutputCommitter outputCommitter ) throws IOException
      {
      outputCommitter.abortTask( taskContext );
      }
    } );
    }

  public interface Predicate
    {
    void invoke( OutputCommitter outputCommitter ) throws IOException;
    }

  private void invoke( JobConf jobConf, Predicate predicate ) throws IOException
    {
    predicate.invoke( newInstance( jobConf ) );

    Path originalPath = FileOutputFormat.getOutputPath( jobConf );

    try
      {
      String[] paths = getBypassOutputPaths( jobConf );

      for( int i = 0; i < paths.length; i++ )
        {
        FileOutputFormat.setOutputPath( jobConf, new Path( paths[ i ] ) );

        predicate.invoke( newInstance( jobConf ) );
        }
      }
    finally
      {
      FileOutputFormat.setOutputPath( jobConf, originalPath );
      }
    }

  private FileOutputCommitter newInstance( JobConf jobConf )
    {
    return ReflectionUtils.newInstance( FileOutputCommitter.class, jobConf );
    }
  }
