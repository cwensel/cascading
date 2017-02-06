/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.stats.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowNode;
import cascading.management.state.ClientState;
import cascading.stats.BaseCachedNodeStats;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowSliceStats;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskReport;

import static cascading.util.Util.formatDurationFromMillis;

/**
 *
 */
public class HadoopNodeStats extends BaseCachedNodeStats<Configuration, FlowNodeStats, Map<String, Map<String, Long>>>
  {
  private Map<TaskID, String> sliceIDCache = new HashMap<TaskID, String>( 4999 ); // caching for ids

  private HadoopStepStats parentStepStats;
  private HadoopSliceStats.Kind kind;

  /**
   * Constructor CascadingStats creates a new CascadingStats instance.
   *
   * @param parentStepStats
   * @param configuration
   * @param kind
   * @param flowNode
   * @param clientState
   */
  protected HadoopNodeStats( final HadoopStepStats parentStepStats, Configuration configuration, HadoopSliceStats.Kind kind, FlowNode flowNode, ClientState clientState )
    {
    super( flowNode, clientState );
    this.parentStepStats = parentStepStats;
    this.kind = kind;

    this.counterCache = new HadoopNodeCounterCache( this, configuration );
    }

  @Override
  public String getKind()
    {
    if( kind == null )
      return null;

    return kind.name();
    }

  private Status getParentStatus()
    {
    return parentStepStats.getStatus();
    }

  @Override
  protected boolean captureChildDetailInternal()
    {
    if( allChildrenFinished )
      return true;

    JobClient jobClient = parentStepStats.getJobClient();
    RunningJob runningJob = parentStepStats.getJobStatusClient();

    if( jobClient == null || runningJob == null )
      return false;

    try
      {
      TaskReport[] taskReports; // todo: use Job task reports

      if( kind == HadoopSliceStats.Kind.MAPPER )
        taskReports = jobClient.getMapTaskReports( runningJob.getID() );
      else
        taskReports = jobClient.getReduceTaskReports( runningJob.getID() );

      if( taskReports.length == 0 )
        return false;

      addTaskStats( taskReports, false );

      return true;
      }
    catch( IOException exception )
      {
      logWarn( "unable to retrieve slice stats via task reports", exception );
      }

    return false;
    }

  protected void addTaskStats( TaskReport[] taskReports, boolean skipLast )
    {
    logInfo( "retrieved task reports: {}", taskReports.length );

    long lastFetch = System.currentTimeMillis();
    boolean fetchedAreFinished = true;

    synchronized( sliceStatsMap )
      {
      int added = 0;
      int updated = 0;

      for( int i = 0; i < taskReports.length - ( skipLast ? 1 : 0 ); i++ )
        {
        TaskReport taskReport = taskReports[ i ];

        if( taskReport == null )
          {
          logWarn( "found empty task report" );
          continue;
          }

        String id = getSliceIDFor( taskReport.getTaskID() );
        HadoopSliceStats sliceStats = new HadoopSliceStats( id, getParentStatus(), kind, taskReport, lastFetch );

        if( sliceStatsMap.put( id, sliceStats ) != null )
          updated++;
        else
          added++;

        if( !sliceStats.getStatus().isFinished() )
          fetchedAreFinished = false;
        }

      int total = sliceStatsMap.size();
      String duration = formatDurationFromMillis( System.currentTimeMillis() - lastFetch );

      logInfo( "added {}, updated: {} slices, with duration: {}, total fetched: {}", added, updated, duration, total );
      }

    allChildrenFinished = taskReports.length != 0 && fetchedAreFinished;
    }

  protected void addAttempt( TaskCompletionEvent event )
    {
    // the event could be a housekeeping task, which we are not tracking
    String sliceID = sliceIDCache.get( event.getTaskAttemptId().getTaskID() );

    if( sliceID == null )
      return;

    FlowSliceStats stats;

    synchronized( sliceStatsMap )
      {
      stats = sliceStatsMap.get( sliceID );
      }

    if( stats == null )
      return;

    ( (HadoopSliceStats) stats ).addAttempt( event );
    }

  private String getSliceIDFor( TaskID taskID )
    {
    // using taskID instance as #toString is quite painful
    String id = sliceIDCache.get( taskID );

    if( id == null )
      {
      id = Util.createUniqueID();
      sliceIDCache.put( taskID, id );
      }

    return id;
    }
  }
