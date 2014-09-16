/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.tez.Hadoop2TezFlow;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;
import cascading.stats.tez.Hadoop2TezStepStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;

import static cascading.flow.FlowProps.JOB_POLLING_INTERVAL;
import static cascading.stats.CascadingStats.STATS_STORE_INTERVAL;

/**
 *
 */
public class Hadoop2TezFlowStepJob extends FlowStepJob<TezConfiguration>
  {
  /** Field currentConf */
  private final TezConfiguration currentConf;
  private DAG dag;

  private Credentials credentials = new Credentials();

  private TezClient tezClient;
  private DAGClient dagClient;

  private static long getStoreInterval( Configuration configuration )
    {
    return configuration.getLong( STATS_STORE_INTERVAL, 60 * 1000 );
    }

  public static long getJobPollingInterval( Configuration configuration )
    {
    return configuration.getLong( JOB_POLLING_INTERVAL, 5000 );
    }

  public Hadoop2TezFlowStepJob( ClientState clientState, BaseFlowStep flowStep, TezConfiguration currentConf, DAG dag )
    {
    super( clientState, flowStep, getJobPollingInterval( currentConf ), getStoreInterval( currentConf ) );
    this.currentConf = currentConf;
    this.dag = dag;

    if( flowStep.isDebugEnabled() )
      flowStep.logDebug( "using polling interval: " + pollingInterval );
    }

  @Override
  public TezConfiguration getConfig()
    {
    return currentConf;
    }

  @Override
  protected FlowStepStats createStepStats( ClientState clientState )
    {
    return new Hadoop2TezStepStats( flowStep, clientState )
    {
//    @Override
//    public JobClient getJobClient()
//      {
//      return jobClient;
//      }
//
//    @Override
//    public RunningJob getRunningJob()
//      {
//      return runningJob;
//      }
    };
    }

  protected void internalNonBlockingStart() throws IOException
    {
    try
      {
      // copy
      TezConfiguration workingConf = new TezConfiguration( currentConf );

      prepareEnsureStagingDir( workingConf );

      tezClient = TezClient.create( flowStep.getName(), workingConf, Collections.<String, LocalResource>emptyMap(), credentials );

      tezClient.start(); // todo: need a reciprocal stop

      dagClient = tezClient.submitDAG( dag ); // is not submitted

      flowStep.logInfo( "submitted tez dag: " + dagClient.getExecutionContext() );

//      if( dagClient.getApplicationReport() != null && dagClient.getApplicationReport().getTrackingUrl() != null )
//        flowStep.logInfo( "tracking url: " + dagClient.getApplicationReport().getTrackingUrl() );
      }
    catch( TezException exception )
      {
      throw new CascadingException( exception );
      }
    }

  private Path prepareEnsureStagingDir( TezConfiguration workingConf ) throws IOException
    {
    String stepStagingPath = createStepStagingPath();

    workingConf.set( TezConfiguration.TEZ_AM_STAGING_DIR, stepStagingPath );

    Path stagingDir = new Path( stepStagingPath );
    FileSystem fileSystem = FileSystem.get( workingConf );

    stagingDir = fileSystem.makeQualified( stagingDir );

    TokenCache.obtainTokensForNamenodes( credentials, new Path[]{stagingDir}, workingConf );

    TezClientUtils.ensureStagingDirExists( workingConf, stagingDir );

    if( fileSystem.getScheme().startsWith( "file:/" ) )
      new File( stagingDir.toUri() ).mkdirs();

    return stagingDir;
    }

  String createStepStagingPath()
    {
    String result = "";

    if( HadoopUtil.isLocal( currentConf ) )
      result = currentConf.get( "hadoop.tmp.dir" ) + Path.SEPARATOR;

    String flowStagingPath = ( (Hadoop2TezFlow) flowStep.getFlow() ).getFlowStagingPath();

    return result + flowStagingPath + Path.SEPARATOR + flowStep.getID();
    }

  Set<StatusGetOpts> statusGetOpts = EnumSet.of( StatusGetOpts.GET_COUNTERS );

  private DAGStatus.State getDagStatusState()
    {
    DAGStatus dagStatus = getDagStatus();

    if( dagStatus == null )
      return null;

    return dagStatus.getState();
    }

  private boolean isDagStatusComplete()
    {
    DAGStatus dagStatus = getDagStatus();

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
      return dagClient.getDAGStatus( statusGetOpts );
      }
    catch( IOException | TezException exception )
      {
      throw new CascadingException( exception );
      }
    }

  protected void internalBlockOnStop() throws IOException
    {
    if( isDagStatusComplete() )
      return;

    try
      {
      if( dagClient != null )
        dagClient.tryKillDAG();

      if( tezClient != null )
        tezClient.stop(); // will shutdown the session
      }
    catch( TezException exception )
      {
      flowStep.logWarn( "exception during attempt to kill dag", exception );
      }
    }

  @Override
  protected void internalCleanup()
    {
    flowStep.logInfo( "closing tez clients" );

    try
      {
      if( dagClient != null )
        dagClient.close();

      if( tezClient != null )
        tezClient.stop(); // will shutdown the session
      }
    catch( TezException | IOException exception )
      {
      flowStep.logWarn( "exception during attempt to cleanup client", exception );
      }
    }

  protected boolean internalNonBlockingIsSuccessful() throws IOException
    {
    return getDagStatusState() == DAGStatus.State.SUCCEEDED;
    }

  @Override
  protected boolean isRemoteExecution()
    {
    return HadoopUtil.isLocal( currentConf );
    }

  @Override
  protected Throwable getThrowable()
    {
    return null;
    }

  protected String internalJobId()
    {
    return dagClient.getExecutionContext().toString();
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
    // return getDagStatusState() == DAGStatus.State.RUNNING || isDagStatusComplete();

    if( getDagStatus() == null )
      return false;

    Progress dagProgress = getDagStatus().getDAGProgress();

    // not strictly true
    if( dagProgress == null )
      return false;

    // same as showing progress in map/reduce
    int completed = dagProgress.getRunningTaskCount()
      + dagProgress.getFailedTaskCount()
      + dagProgress.getKilledTaskCount()
      + dagProgress.getSucceededTaskCount();

    return completed > 0;
    }
  }
