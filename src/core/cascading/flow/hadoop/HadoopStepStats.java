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

package cascading.flow.hadoop;

import cascading.flow.FlowException;
import cascading.stats.StepStats;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Class HadoopStepStats ... */
public abstract class HadoopStepStats extends StepStats
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( HadoopStepStats.class );

  /** Field numMapTasks */
  int numMapTasks;
  /** Field numReducerTasks */
  int numReducerTasks;
  /** Field taskStats */
  List<HadoopTaskStats> taskStats;

  /** Class HadoopTaskStats ... */
  public static class HadoopTaskStats
    {
    /** Field isMapper */
    boolean isMapper;
    /** Field duration */
    int duration;
    /** Field status */
    String status;

    HadoopTaskStats( TaskCompletionEvent taskCompletionEvent )
      {
      isMapper = taskCompletionEvent.isMapTask();
      duration = taskCompletionEvent.getTaskRunTime();
      status = taskCompletionEvent.getTaskStatus().toString();
      }
    }

  public List<HadoopTaskStats> getTaskStats()
    {
    if( taskStats == null )
      taskStats = new ArrayList<HadoopTaskStats>();

    return taskStats;
    }

  public void addTaskStats( TaskCompletionEvent[] taskCompletionEvents )
    {
    for( TaskCompletionEvent taskCompletionEvent : taskCompletionEvents )
      getTaskStats().add( new HadoopTaskStats( taskCompletionEvent ) );
    }

  public int getNumMapTasks()
    {
    return numMapTasks;
    }

  public void setNumMapTasks( int numMapTasks )
    {
    this.numMapTasks = numMapTasks;
    }

  public int getNumReducerTasks()
    {
    return numReducerTasks;
    }

  public void setNumReducerTasks( int numReducerTasks )
    {
    this.numReducerTasks = numReducerTasks;
    }

  protected abstract RunningJob getRunningJob();

  @Override
  public long getCounter( Enum counter )
    {
    try
      {
      return getRunningJob().getCounters().getCounter( counter );
      }
    catch( IOException e )
      {
      throw new FlowException( "unable to get counter values" );
      }
    }

  @Override
  public void captureDetail()
    {
    try
      {
      getTaskStats().clear();

      int count = 0;

      while( true )
        {
        TaskCompletionEvent[] events = getRunningJob().getTaskCompletionEvents( count );

        if( events.length == 0 )
          break;

        addTaskStats( events );
        count += 10;
        }
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to get completion events", exception );
      }
    }
  }
