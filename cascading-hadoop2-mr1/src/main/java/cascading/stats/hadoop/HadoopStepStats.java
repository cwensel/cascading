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

package cascading.stats.hadoop;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HadoopStepStats is a hadoop2 (YARN) specific sub-class for fetching TaskReports in an efficient way.
 */
public abstract class HadoopStepStats extends BaseHadoopStepStats
  {
  /** logger. */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopStepStats.class );

  private Map<TaskID, String> idCache = new HashMap<TaskID, String>( 4999 ); // nearest prime, caching for ids

  protected HadoopStepStats( FlowStep<JobConf> flowStep, ClientState clientState )
    {
    super( flowStep, clientState );
    }

  @Override
  protected void addTaskStats( Map<String, HadoopSliceStats> taskStats, HadoopSliceStats.Kind kind, boolean skipLast ) throws IOException
    {
    TaskReport[] taskReports = retrieveTaskReports( kind );

    for( int i = 0; i < taskReports.length - ( skipLast ? 1 : 0 ); i++ )
      {
      TaskReport taskReport = taskReports[ i ];

      if( taskReport == null )
        {
        LOG.warn( "found empty task report" );
        continue;
        }

      String id = getIDFor( taskReport.getTaskID() );
      taskStats.put( id, new HadoopSliceStats( id, getStatus(), kind, stepHasReducers(), taskReport ) );

      incrementKind( kind );
      }
    }

  /**
   * Retrieves the TaskReports via the mapreduce API.
   *
   * @param kind The kind of TaskReport to retrieve.
   * @return An array of TaskReports, but never <code>nul</code>.
   * @throws IOException
   */
  private TaskReport[] retrieveTaskReports( HadoopSliceStats.Kind kind ) throws IOException
    {
    Job job = findJob();

    if( job == null )
      return new TaskReport[ 0 ];

    try
      {
      switch( kind )
        {
        case MAPPER:
          return job.getTaskReports( TaskType.MAP );
        case REDUCER:
          return job.getTaskReports( TaskType.REDUCE );
        case SETUP:
          return job.getTaskReports( TaskType.JOB_SETUP );
        case CLEANUP:
          return job.getTaskReports( TaskType.JOB_CLEANUP );
        default:
          return new TaskReport[ 0 ];
        }
      }
    catch( InterruptedException exception )
      {
      throw new CascadingException( exception );
      }
    }

  /**
   * Method extracts the {@link org.apache.hadoop.mapreduce.Job} from the RunningJob via reflection.
   *
   * @return A {@link Job} instance.
   */
  private Job findJob()
    {
    // fetches the TaskReports via the mapreduce API to avoid memory pressure due to large array copies within the
    // mapred api in Hadoop 2.x.
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return null;

    Job job = Util.returnInstanceFieldIfExistsSafe( runningJob, "job" );

    if( job == null )
      {
      LOG.warn( "unable to get underlying org.apache.hadoop.mapreduce.Job from org.apache.hadoop.mapred.RunningJob, task level task counter will be unavailable" );
      return null;
      }

    return job;
    }

  @Override
  protected void addAttemptsToTaskStats( Map<String, HadoopSliceStats> taskStats, boolean captureAttempts )
    {
    Job job = findJob();

    if( job == null )
      return;

    int count = 0;

    while( captureAttempts )
      {
      try
        {
        TaskCompletionEvent[] events = job.getTaskCompletionEvents( count );

        if( events.length == 0 )
          break;

        for( TaskCompletionEvent event : events )
          {
          if( event == null )
            {
            LOG.warn( "found empty completion event" );
            continue;
            }

          // this will return a housekeeping task, which we are not tracking
          HadoopSliceStats stats = taskStats.get( getIDFor( event.getTaskAttemptId().getTaskID() ) );

          if( stats != null )
            stats.addAttempt( event );
          }

        count += events.length;
        }
      catch( IOException exception )
        {
        throw new CascadingException( exception );
        }
      }
    }

  private String getIDFor( TaskID taskID )
    {
    // using taskID instance as #toString is quite painful
    String id = idCache.get( taskID );

    if( id == null )
      {
      id = Util.createUniqueID();
      idCache.put( taskID, id );
      }

    return id;
    }
  }
