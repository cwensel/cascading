/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.stats.tez;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowNode;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.stream.annotations.StreamMode;
import cascading.management.state.ClientState;
import cascading.property.PropertyUtil;
import cascading.stats.FlowSliceStats;
import cascading.stats.hadoop.BaseHadoopNodeStats;
import cascading.stats.tez.util.TaskStatus;
import cascading.stats.tez.util.TimelineClient;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.stats.tez.util.TezStatsUtil.STATUS_GET_COUNTERS;
import static cascading.util.Util.formatDurationFromMillis;

/**
 *
 */
public class TezNodeStats extends BaseHadoopNodeStats<DAGClient, TezCounters>
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezNodeStats.class );

  public static final String TIMELINE_FETCH_LIMIT = "cascading.stats.timeline.fetch.limit";
  public static final int DEFAULT_FETCH_LIMIT = 500;

  public static enum Kind
    {
      SPLIT, PARTITIONED
    }

  private TezStepStats parentStepStats;
  private Kind kind;
  private final int fetchLimit;

  private String vertexID;
  private int totalTaskCount;
  private int succeededTaskCount;
  private int failedTaskCount;
  private int killedTaskCount;
  private int runningTaskCount;
  private boolean allTasksAreFinished;

  protected TezNodeStats( final TezStepStats parentStepStats, FlowNode flowNode, ClientState clientState, Configuration configuration )
    {
    super( flowNode, clientState );

    this.parentStepStats = parentStepStats;
    this.kind = flowNode.getSourceElements( StreamMode.Streamed ).isEmpty() ? Kind.PARTITIONED : Kind.SPLIT;
    this.fetchLimit = PropertyUtil.getProperty( HadoopUtil.createProperties( configuration ), TIMELINE_FETCH_LIMIT, DEFAULT_FETCH_LIMIT );
    this.counterCache = new TezCounterCache<DAGClient>( this, configuration )
    {
    @Override
    protected DAGClient getJobStatusClient()
      {
      return parentStepStats.getJobStatusClient();
      }

    protected TezCounters getCounters( DAGClient dagClient ) throws IOException
      {
      VertexStatus vertexStatus = updateProgress( dagClient, STATUS_GET_COUNTERS );

      if( vertexStatus == null )
        return null;

      TezCounters vertexCounters = vertexStatus.getVertexCounters();

      if( vertexCounters == null )
        LOG.warn( "could not retrieve vertex counters for: {}, in stats status: {}, and vertex state: {}", getID(), getStatus(), vertexStatus.getState() );

      return vertexCounters;
      }
    };
    }

  private String retrieveVertexID( DAGClient dagClient )
    {
    if( vertexID != null || !( dagClient instanceof TimelineClient ) )
      return vertexID;

    try
      {
      vertexID = ( (TimelineClient) dagClient ).getVertexID( getID() );
      }
    catch( IOException | TezException exception )
      {
      LOG.warn( "unable to get vertex id for: {}", getID(), exception );
      }

    return vertexID;
    }

  public int getTotalTaskCount()
    {
    return totalTaskCount;
    }

  public int getSucceededTaskCount()
    {
    return succeededTaskCount;
    }

  public int getFailedTaskCount()
    {
    return failedTaskCount;
    }

  public int getKilledTaskCount()
    {
    return killedTaskCount;
    }

  public int getRunningTaskCount()
    {
    return runningTaskCount;
    }

  @Override
  protected boolean captureChildDetailInternal()
    {
    DAGClient dagClient = parentStepStats.getJobStatusClient();

    if( dagClient == null )
      return false;

    // we cannot get task counters without the timeline server running
    if( dagClient instanceof TimelineClient )
      return withTimelineServer( (TimelineClient) dagClient );

    // these are just placeholders without counters, otherwise the order would be reversed as a failover mechanism
    return withoutTimelineServer( dagClient );
    }

  private boolean withTimelineServer( TimelineClient timelineClient )
    {
    updateProgress( (DAGClient) timelineClient, null ); // get latest task counts

    if( sliceStatsMap.size() == getTotalTaskCount() )
      return updateAllTasks( timelineClient );

    return fetchAllTasks( timelineClient );
    }

  private boolean updateAllTasks( TimelineClient timelineClient )
    {
    if( allTasksAreFinished )
      return true;

    long startTime = System.currentTimeMillis();

    int count = 0;

    for( FlowSliceStats sliceStats : sliceStatsMap.values() )
      {
      if( sliceStats.getStatus().isFinished() )
        continue;

      TaskStatus taskStatus = getTaskStatusFor( timelineClient, sliceStats.getProcessSliceID() );

      updateSliceWith( (TezSliceStats) sliceStats, taskStatus );

      count++;
      }

    if( count == 0 )
      allTasksAreFinished = true;

    LOG.info( "updated {} slices in: {}", count, formatDurationFromMillis( System.currentTimeMillis() - startTime ) );

    return sliceStatsMap.size() == getTotalTaskCount();
    }

  private boolean fetchAllTasks( TimelineClient timelineClient )
    {
    long startTime = System.currentTimeMillis();
    String fromTaskId = null;
    int startSize = sliceStatsMap.size();
    int iteration = 0;
    boolean continueIterating = true;

    while( continueIterating && sliceStatsMap.size() != getTotalTaskCount() )
      {
      // we will see the same tasks twice as we paginate
      Iterator<TaskStatus> vertexChildren = getTaskStatusIterator( timelineClient, fromTaskId );

      if( vertexChildren == null )
        return false;

      int added = 0;
      int updated = 0;

      while( vertexChildren.hasNext() )
        {
        TaskStatus taskStatus = vertexChildren.next();

        fromTaskId = taskStatus.getTaskID();

        TezSliceStats sliceStats = (TezSliceStats) sliceStatsMap.get( fromTaskId );

        if( sliceStats == null )
          {
          added++;

          sliceStats = new TezSliceStats( Util.createUniqueID(), kind, this.getStatus(), fromTaskId );

          sliceStatsMap.put( sliceStats.getProcessSliceID(), sliceStats );
          }
        else
          {
          updated++;
          }

        updateSliceWith( sliceStats, taskStatus );
        }

      continueIterating = added + updated != 0;

      if( !continueIterating )
        LOG.info( "no slices retrieved in iteration: {}, with fetch size: {}, ending fetch", ++iteration, fetchLimit );
      else
        LOG.info( "added new {}, updated {} slices in iteration: {}, with fetch size: {}", added, updated, ++iteration, fetchLimit );
      }

    int total = sliceStatsMap.size();
    LOG.info( "added {} new slices, total fetched: {}, with missing: {} in: {}", total - startSize, total, getTotalTaskCount() - total, formatDurationFromMillis( System.currentTimeMillis() - startTime ) );

    return total == getTotalTaskCount();
    }

  private void updateSliceWith( TezSliceStats sliceStats, TaskStatus taskStatus )
    {
    if( taskStatus == null )
      return;

    sliceStats.setStatus( getStatusForTaskStatus( taskStatus.getStatus() ) );
    sliceStats.setCounters( taskStatus.getCounters() );
    }

  private TaskStatus getTaskStatusFor( TimelineClient timelineClient, String taskID )
    {
    try
      {
      return timelineClient.getVertexChild( taskID );
      }
    catch( TezException exception )
      {
      LOG.warn( "unable to get slice stat from timeline server for id: {}", taskID, exception );
      }

    return null;
    }

  private Iterator<TaskStatus> getTaskStatusIterator( TimelineClient timelineClient, String startTaskID )
    {
    try
      {
      String vertexID = retrieveVertexID( (DAGClient) timelineClient );

      if( vertexID == null )
        {
        LOG.warn( "unable to get slice stats from timeline server, did not retrieve valid vertex id for vertex name: {}", getID() );
        return null;
        }

      return timelineClient.getVertexChildren( vertexID, fetchLimit, startTaskID );
      }
    catch( IOException | TezException exception )
      {
      LOG.warn( "unable to get slice stats from timeline server", exception );
      }

    return null;
    }

  private boolean withoutTimelineServer( DAGClient dagClient )
    {
    VertexStatus vertexStatus = updateProgress( dagClient, STATUS_GET_COUNTERS );

    if( vertexStatus == null )
      return false;

    int total = sliceStatsMap.size();

    if( total == 0 ) // yet to be initialized
      LOG.warn( "'" + YarnConfiguration.TIMELINE_SERVICE_ENABLED + "' is disabled, task level counters cannot be retrieved" );

    for( int i = total; i < totalTaskCount; i++ )
      {
      TezSliceStats sliceStats = new TezSliceStats( Util.createUniqueID(), kind, this.getStatus(), null );

      // we don't have the taskId, so we are using the id as the key
      sliceStatsMap.put( sliceStats.getID(), sliceStats );
      }

    // a placeholder to simulate actual slice stats for now
    Iterator<FlowSliceStats> iterator = sliceStatsMap.values().iterator();

    for( int i = 0; i < runningTaskCount && iterator.hasNext(); i++ )
      ( (TezSliceStats) iterator.next() ).setStatus( Status.RUNNING );

    for( int i = 0; i < succeededTaskCount && iterator.hasNext(); i++ )
      ( (TezSliceStats) iterator.next() ).setStatus( Status.SUCCESSFUL );

    for( int i = 0; i < failedTaskCount && iterator.hasNext(); i++ )
      ( (TezSliceStats) iterator.next() ).setStatus( Status.FAILED );

    for( int i = 0; i < killedTaskCount && iterator.hasNext(); i++ )
      ( (TezSliceStats) iterator.next() ).setStatus( Status.STOPPED );

    List<String> diagnostics = vertexStatus.getDiagnostics();

    for( String diagnostic : diagnostics )
      LOG.info( "vertex diagnostics: {}", diagnostic );

    return true;
    }

  private Status getStatusForTaskStatus( String status )
    {
    TaskState state = TaskState.valueOf( status );

    switch( state )
      {
      case NEW:
        return Status.PENDING;
      case SCHEDULED:
        return Status.SUBMITTED;
      case RUNNING:
        return Status.RUNNING;
      case SUCCEEDED:
        return Status.SUCCESSFUL;
      case FAILED:
        return Status.FAILED;
      case KILLED:
        return Status.STOPPED;
      }

    return null;
    }

  private VertexStatus updateProgress( DAGClient dagClient, Set<StatusGetOpts> statusGetOpts )
    {
    VertexStatus vertexStatus = null;

    try
      {
      vertexStatus = dagClient.getVertexStatus( getID(), statusGetOpts );
      }
    catch( IOException | TezException exception )
      {
      LOG.warn( "unable to get vertex status", exception );
      }

    if( vertexStatus == null )
      return null;

    Progress progress = vertexStatus.getProgress();

    totalTaskCount = progress.getTotalTaskCount();
    runningTaskCount = progress.getRunningTaskCount();
    succeededTaskCount = progress.getSucceededTaskCount();
    failedTaskCount = progress.getFailedTaskCount();
    killedTaskCount = progress.getKilledTaskCount();

    return vertexStatus;
    }
  }
