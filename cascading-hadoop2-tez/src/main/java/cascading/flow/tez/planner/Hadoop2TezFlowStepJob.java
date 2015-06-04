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

package cascading.flow.tez.planner;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import cascading.CascadingException;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.tez.Hadoop2TezFlow;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import cascading.stats.tez.TezStepStats;
import cascading.stats.tez.util.TezStatsUtil;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;

import static cascading.flow.FlowProps.JOB_POLLING_INTERVAL;
import static cascading.stats.CascadingStats.STATS_STORE_INTERVAL;

/**
 *
 */
public class Hadoop2TezFlowStepJob extends FlowStepJob<TezConfiguration>
  {
  private static final Set<StatusGetOpts> STATUS_GET_OPTS = EnumSet.of( StatusGetOpts.GET_COUNTERS );

  private DAG dag;

  private TezClient tezClient;
  private DAGClient dagClient;

  private String dagId;

  private static long getStoreInterval( Configuration configuration )
    {
    return configuration.getLong( STATS_STORE_INTERVAL, 60 * 1000 );
    }

  public static long getJobPollingInterval( Configuration configuration )
    {
    return configuration.getLong( JOB_POLLING_INTERVAL, 5000 );
    }

  public Hadoop2TezFlowStepJob( ClientState clientState, BaseFlowStep<TezConfiguration> flowStep, TezConfiguration currentConf, DAG dag )
    {
    super( clientState, currentConf, flowStep, getJobPollingInterval( currentConf ), getStoreInterval( currentConf ) );
    this.dag = dag;

    if( flowStep.isDebugEnabled() )
      flowStep.logDebug( "using polling interval: " + pollingInterval );
    }

  @Override
  protected FlowStepStats createStepStats( ClientState clientState )
    {
    return new TezStepStats( flowStep, clientState )
    {
    DAGClient timelineClient = null;

    @Override
    public synchronized DAGClient getJobStatusClient()
      {
      if( timelineClient != null )
        return timelineClient;

      if( isTimelineServiceEnabled( jobConfiguration ) )
        timelineClient = TezStatsUtil.createTimelineClient( dagClient ); // may return null

      if( timelineClient == null )
        timelineClient = dagClient;

      return timelineClient;
      }

    @Override
    public String getProcessStepID()
      {
      return dagId;
      }
    };
    }

  protected void internalNonBlockingStart() throws IOException
    {
    try
      {
      if( !isTimelineServiceEnabled( jobConfiguration ) )
        flowStep.logWarn( "'" + YarnConfiguration.TIMELINE_SERVICE_ENABLED + "' is disabled, please enable to capture detailed metrics of completed flows, this may require starting the YARN timeline server daemon" );

      TezConfiguration workingConf = new TezConfiguration( jobConfiguration );

      // this could be problematic
      flowStep.logInfo( "tez session mode enabled: " + workingConf.getBoolean( TezConfiguration.TEZ_AM_SESSION_MODE, TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT ) );

      prepareEnsureStagingDir( workingConf );

      tezClient = TezClient.create( flowStep.getName(), workingConf );

      tezClient.start();

      dagClient = tezClient.submitDAG( dag );

      dagId = Util.returnInstanceFieldIfExistsSafe( dagClient, "dagId" );

      flowStep.logInfo( "submitted tez dag to app master: {}, with dag id: {}", tezClient.getAppMasterApplicationId(), dagId );
      }
    catch( TezException exception )
      {
      throw new CascadingException( exception );
      }
    }

  private boolean isTimelineServiceEnabled( TezConfiguration workingConf )
    {
    return workingConf.getBoolean( YarnConfiguration.TIMELINE_SERVICE_ENABLED, YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED );
    }

  @Override
  protected void updateNodeStatus( FlowNodeStats flowNodeStats )
    {
    if( dagClient == null )
      return;

    try
      {
      VertexStatus vertexStatus = dagClient.getVertexStatus( flowNodeStats.getID(), null ); // no counters

      if( vertexStatus == null )
        return;

      VertexStatus.State state = vertexStatus.getState();

      if( state == null )
        return;

      switch( state )
        {
        case NEW:
          break;

        case INITIALIZING:
          break;

        case INITED:
          break;

        case RUNNING:
          flowNodeStats.markRunning();
          break;

        case SUCCEEDED:
          if( !flowNodeStats.isRunning() )
            flowNodeStats.markRunning();

          flowNodeStats.markSuccessful();
          break;

        case FAILED:
          if( !flowNodeStats.isRunning() )
            flowNodeStats.markRunning();

          flowNodeStats.markFailed( null ); // todo: lookup failure
          break;

        case KILLED:
          if( !flowNodeStats.isRunning() )
            flowNodeStats.markRunning();

          flowNodeStats.markStopped();
          break;

        case ERROR:
          if( !flowNodeStats.isRunning() )
            flowNodeStats.markRunning();

          flowNodeStats.markFailed( null ); // todo: lookup failure
          break;

        case TERMINATING:
          break;
        }
      }
    catch( IOException | TezException exception )
      {
      flowStep.logError( "failed setting node status", throwable );
      }
    }

  private Path prepareEnsureStagingDir( TezConfiguration workingConf ) throws IOException
    {
    String stepStagingPath = createStepStagingPath();

    workingConf.set( TezConfiguration.TEZ_AM_STAGING_DIR, stepStagingPath );

    Path stagingDir = new Path( stepStagingPath );
    FileSystem fileSystem = FileSystem.get( workingConf );

    stagingDir = fileSystem.makeQualified( stagingDir );

    TokenCache.obtainTokensForNamenodes( new Credentials(), new Path[]{stagingDir}, workingConf );

    TezClientUtils.ensureStagingDirExists( workingConf, stagingDir );

    if( fileSystem.getScheme().startsWith( "file:/" ) )
      new File( stagingDir.toUri() ).mkdirs();

    return stagingDir;
    }

  String createStepStagingPath()
    {
    String result = "";

    if( HadoopUtil.isLocal( jobConfiguration ) )
      result = jobConfiguration.get( "hadoop.tmp.dir" ) + Path.SEPARATOR;

    String flowStagingPath = ( (Hadoop2TezFlow) flowStep.getFlow() ).getFlowStagingPath();

    return result + flowStagingPath + Path.SEPARATOR + flowStep.getID();
    }

  private DAGStatus.State getDagStatusState()
    {
    DAGStatus dagStatus = getDagStatus();

    if( dagStatus == null )
      {
      flowStep.logWarn( "getDagStatus returned null" );

      return null;
      }

    DAGStatus.State state = dagStatus.getState();

    if( state == null )
      flowStep.logWarn( "dagStatus#getState returned null" );

    return state;
    }

  private boolean isDagStatusComplete()
    {
    DAGStatus dagStatus = getDagStatus();

    if( dagStatus == null )
      flowStep.logWarn( "getDagStatus returned null" );

    return dagStatus != null && dagStatus.isCompleted();
    }

  private DAGStatus getDagStatus()
    {
    if( dagClient == null )
      return null;

    try
      {
      return dagClient.getDAGStatus( null );
      }
    catch( NullPointerException exception )
      {
      flowStep.logWarn( "NPE thrown by getDAGStatus, known issue" );

      return null;
      }
    catch( IOException | TezException exception )
      {
      throw new CascadingException( exception );
      }
    }

  private DAGStatus getDagStatusWithCounters()
    {
    if( dagClient == null )
      return null;

    try
      {
      return dagClient.getDAGStatus( STATUS_GET_OPTS );
      }
    catch( IOException | TezException exception )
      {
      throw new CascadingException( "unable to get counters from dag client", exception );
      }
    }

  protected void internalBlockOnStop() throws IOException
    {
    if( isDagStatusComplete() )
      return;

    try
      {
      if( dagClient != null )
        dagClient.tryKillDAG(); // sometimes throws an NPE
      }
    catch( Exception exception )
      {
      flowStep.logWarn( "exception during attempt to kill dag", exception );
      }

    stopDAGClient();
    stopTezClient();
    }

  @Override
  protected void internalCleanup()
    {
    stopDAGClient();
    stopTezClient();
    }

  private void stopDAGClient()
    {
    try
      {
      if( dagClient != null )
        dagClient.close(); // may throw an NPE
      }
    catch( Exception exception )
      {
      flowStep.logWarn( "exception during attempt to cleanup client", exception );
      }
    }

  private void stopTezClient()
    {
    try
      {
      if( tezClient == null )
        return;

      if( isRemoteExecution() )
        {
        tezClient.stop(); // will shutdown the session
        return;
        }

      // the Tez LocalClient will frequently hang on #stop(), this causes tests to never complete
      Boolean result = Util.submitWithTimeout( new Callable<Boolean>()
      {
      @Override
      public Boolean call() throws Exception
        {
        tezClient.stop();
        return true;
        }
      }, 5, TimeUnit.MINUTES );

      if( result == null || !result )
        flowStep.logWarn( "tezClient#stop() timed out after 5 minutes, cancelling call, continuing" );
      }
    catch( Exception exception )
      {
      flowStep.logWarn( "exception during attempt to cleanup client", exception );
      }
    }

  protected boolean internalNonBlockingIsSuccessful() throws IOException
    {
    return isDagStatusComplete() && getDagStatusState() == DAGStatus.State.SUCCEEDED;
    }

  @Override
  protected boolean isRemoteExecution()
    {
    return !HadoopUtil.isLocal( jobConfiguration );
    }

  @Override
  protected Throwable getThrowable()
    {
    return null;
    }

  protected String internalJobId()
    {
    return dagClient.getExecutionContext();
    }

  protected boolean internalNonBlockingIsComplete() throws IOException
    {
    return isDagStatusComplete();
    }

  protected void dumpDebugInfo()
    {
//    try
//      {
//      if( dagStatus == null )
//        return;

//      flowStep.logWarn( "hadoop job " + runningJob.getID() + " state at " + JobStatus.getJobRunState( runningJob.getJobState() ) );
//      flowStep.logWarn( "failure info: " + runningJob.getFailureInfo() );

//      TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( 0 );
//      flowStep.logWarn( "task completion events identify failed tasks" );
//      flowStep.logWarn( "task completion events count: " + events.length );
//
//      for( TaskCompletionEvent event : events )
//        flowStep.logWarn( "event = " + event );
//      }
//    catch( IOException exception )
//      {
//      flowStep.logError( "failed reading task completion events", exception );
//      }
    }

  protected boolean internalIsStartedRunning()
    {
    // this is an alternative, seems to be set in tests sooner
    // but unsure if the tasks are actually engaged
    return getDagStatusState() == DAGStatus.State.RUNNING || isDagStatusComplete();
/*
    DAGStatus dagStatus = getDagStatus();

    if( dagStatus == null )
      return false;

    Progress dagProgress = dagStatus.getDAGProgress();

    // not strictly true
    if( dagProgress == null )
      return false;

    // same as showing progress in map/reduce
    int completed = dagProgress.getRunningTaskCount()
      + dagProgress.getFailedTaskCount()
      + dagProgress.getKilledTaskCount()
      + dagProgress.getSucceededTaskCount();

    return completed > 0;
*/
    }
  }
