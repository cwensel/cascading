/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import cascading.flow.FlowException;
import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;
import cascading.util.Util;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class HadoopStepStats provides Hadoop specific statistics and methods to underlying Hadoop facilities. */
public abstract class HadoopStepStats extends FlowStepStats
  {
  public static final String COUNTER_TIMEOUT_PROPERTY = "cascading.step.counter.timeout";

  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopStepStats.class );
  public static final int TIMEOUT_MAX = 3;

  private Map<TaskID, String> idCache = new HashMap<TaskID, String>( 4999 ); // nearest prime, caching for ids

  /** Field numMapTasks */
  int numMapTasks;
  /** Field numReducerTasks */
  int numReduceTasks;

  /** Fields counters */
  private Counters cachedCounters = null;

  /** Fields timeouts */
  private int timeouts;

  /** Field taskStats */
  Map<String, HadoopSliceStats> taskStats = (Map<String, HadoopSliceStats>) Collections.EMPTY_MAP;

  protected HadoopStepStats( FlowStep<JobConf> flowStep, ClientState clientState )
    {
    super( flowStep, clientState );
    }

  /**
   * Method getTaskStats returns the taskStats of this HadoopStepStats object.
   *
   * @return the taskStats (type ArrayList<HadoopTaskStats>) of this HadoopStepStats object.
   */
  public Map<String, HadoopSliceStats> getTaskStats()
    {
    return taskStats;
    }

  protected void setTaskStats( Map<String, HadoopSliceStats> taskStats )
    {
    this.taskStats = taskStats;
    }

  /**
   * Method getNumMapTasks returns the numMapTasks from the Hadoop job file.
   *
   * @return the numMapTasks (type int) of this HadoopStepStats object.
   */
  public int getNumMapTasks()
    {
    return numMapTasks;
    }

  void setNumMapTasks( int numMapTasks )
    {
    this.numMapTasks = numMapTasks;
    }

  /**
   * Method getNumReduceTasks returns the numReducerTasks from the Hadoop job file.
   *
   * @return the numReducerTasks (type int) of this HadoopStepStats object.
   */
  public int getNumReduceTasks()
    {
    return numReduceTasks;
    }

  void setNumReduceTasks( int numReduceTasks )
    {
    this.numReduceTasks = numReduceTasks;
    }

  /**
   * Method getJobID returns the Hadoop running job JobID.
   *
   * @return the jobID (type String) of this HadoopStepStats object.
   */
  public String getJobID()
    {
    if( getRunningJob() == null )
      return null;

    return getRunningJob().getJobID();
    }

  /**
   * Method getJobClient returns the Hadoop {@link JobClient} managing this Hadoop job.
   *
   * @return the jobClient (type JobClient) of this HadoopStepStats object.
   */
  public abstract JobClient getJobClient();

  /**
   * Method getRunningJob returns the Hadoop {@link RunningJob} managing this Hadoop job.
   *
   * @return the runningJob (type RunningJob) of this HadoopStepStats object.
   */
  public abstract RunningJob getRunningJob();

  /**
   * Method getCounterGroups returns all of the Hadoop counter groups.
   *
   * @return the counterGroups (type Collection<String>) of this HadoopStepStats object.
   */
  @Override
  public Collection<String> getCounterGroups()
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return Collections.emptySet();

    return Collections.unmodifiableCollection( counters.getGroupNames() );
    }

  /**
   * Method getCounterGroupsMatching returns all the Hadoop counter groups that match the give regex pattern.
   *
   * @param regex of String
   * @return Collection<String>
   */
  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return Collections.emptySet();

    Set<String> results = new HashSet<String>();

    for( String counter : counters.getGroupNames() )
      {
      if( counter.matches( regex ) )
        results.add( counter );
      }

    return Collections.unmodifiableCollection( results );
    }

  /**
   * Method getCountersFor returns the Hadoop counters for the given group.
   *
   * @param group of String
   * @return Collection<String>
   */
  @Override
  public Collection<String> getCountersFor( String group )
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return Collections.emptySet();

    Set<String> results = new HashSet<String>();

    for( Counters.Counter counter : counters.getGroup( group ) )
      results.add( counter.getName() );

    return Collections.unmodifiableCollection( results );
    }

  /**
   * Method getCounterValue returns the Hadoop counter value for the given counter enum.
   *
   * @param counter of Enum
   * @return long
   */
  @Override
  public long getCounterValue( Enum counter )
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return 0;

    return counters.getCounter( counter );
    }

  /**
   * Method getCounterValue returns the Hadoop counter value for the given group and counter name.
   *
   * @param group   of String
   * @param counter of String
   * @return long
   */
  @Override
  public long getCounterValue( String group, String counter )
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return 0;

    Counters.Group counterGroup = counters.getGroup( group );

    if( group == null )
      return 0;

    // geCounter actually searches the display name, wtf
    // in theory this is lazily created if does not exist, but don't rely on it
    Counters.Counter counterValue = counterGroup.getCounterForName( counter );

    if( counter == null )
      return 0;

    return counterValue.getValue();
    }

  protected Counters cachedCounters()
    {
    return cachedCounters( false );
    }

  protected synchronized Counters cachedCounters( boolean force )
    {
    if( !force && ( isFinished() || timeouts >= TIMEOUT_MAX ) )
      return cachedCounters;

    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return cachedCounters;

    Future<Counters> future = runFuture( runningJob );

    int timeout = ( (JobConf) getFlowStep().getConfig() ).getInt( COUNTER_TIMEOUT_PROPERTY, 5 );

    try
      {
      Counters fetched = future.get( timeout, TimeUnit.SECONDS );

      if( fetched != null )
        cachedCounters = fetched;
      }
    catch( InterruptedException exception )
      {
      LOG.warn( "fetching counters was interrupted" );
      }
    catch( ExecutionException exception )
      {
      if( cachedCounters != null )
        {
        LOG.error( "unable to get remote counters, returning cached values", exception.getCause() );

        return cachedCounters;
        }

      LOG.error( "unable to get remote counters, no cached values, throwing exception", exception.getCause() );

      if( exception.getCause() instanceof FlowException )
        throw (FlowException) exception.getCause();

      throw new FlowException( exception.getCause() );
      }
    catch( TimeoutException exception )
      {
      timeouts++;

      if( timeouts >= TIMEOUT_MAX )
        LOG.warn( "fetching counters timed out after: {} seconds, final attempt: {}", timeout, timeouts );
      else
        LOG.warn( "fetching counters timed out after: {} seconds, attempts: {}", timeout, timeouts );
      }

    return cachedCounters;
    }

  // hardcoded at one thread to force serialization across all requesters in the jvm
  // this likely prevents the deadlocks the futures are safeguards against
  private static ExecutorService futuresPool = Executors.newSingleThreadExecutor( new ThreadFactory()
  {
  @Override
  public Thread newThread( Runnable runnable )
    {
    Thread thread = new Thread( runnable, "stats-futures" );

    thread.setDaemon( true );

    return thread;
    }
  } );

  private Future<Counters> runFuture( final RunningJob runningJob )
    {
    Callable<Counters> task = new Callable<Counters>()
    {
    @Override
    public Counters call() throws Exception
      {
      try
        {
        return runningJob.getCounters();
        }
      catch( IOException exception )
        {
        throw new FlowException( "unable to get remote counter values" );
        }
      }
    };

    return futuresPool.submit( task );
    }

  /**
   * Returns the underlying Map tasks progress percentage.
   * <p/>
   * This method is experimental.
   *
   * @return float
   */
  public float getMapProgress()
    {
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return 0;

    try
      {
      return runningJob.mapProgress();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get progress" );
      }
    }

  /**
   * Returns the underlying Reduce tasks progress percentage.
   * <p/>
   * This method is experimental.
   *
   * @return float
   */
  public float getReduceProgress()
    {
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return 0;

    try
      {
      return runningJob.reduceProgress();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get progress" );
      }
    }

  public String getStatusURL()
    {
    RunningJob runningJob = getRunningJob();

    if( runningJob == null )
      return null;

    return runningJob.getTrackingURL();
    }

  /**
   * Method getChildren returns the children of this HadoopStepStats object.
   *
   * @return the children (type Collection) of this HadoopStepStats object.
   */
  @Override
  public Collection getChildren()
    {
    return getTaskStats().values();
    }

  public Set<String> getChildIDs()
    {
    return getTaskStats().keySet();
    }

  /** Synchronized to prevent state changes mid record, #stop may be called out of band */
  @Override
  public synchronized void recordChildStats()
    {
    try
      {
      cachedCounters( true );
      }
    catch( Exception exception )
      {
      // do nothing
      }

    // if null instance don't bother capturing detail
    if( !clientState.isEnabled() )
      return;

    captureDetail();

    try
      {
      for( String id : taskStats.keySet() )
        clientState.record( id, taskStats.get( id ) );
      }
    catch( Exception exception )
      {
      LOG.error( "unable to record slice stats", exception );
      }
    }

  /** Method captureDetail captures statistics task details and completion events. */
  @Override
  public synchronized void captureDetail()
    {
    captureDetail( true );
    }

  public void captureDetail( boolean captureAttempts )
    {
    HashMap<String, HadoopSliceStats> newStats = new HashMap<String, HadoopSliceStats>();

    JobClient jobClient = getJobClient();
    RunningJob runningJob = getRunningJob();

    if( jobClient == null || runningJob == null )
      return;

    numMapTasks = 0;
    numReduceTasks = 0;

    try
      {
      // cleanup/setup tasks have no useful info so far.
//      addTaskStats( newStats, HadoopTaskStats.Kind.SETUP, jobClient.getSetupTaskReports( runningJob.getID() ), false );
//      addTaskStats( newStats, HadoopTaskStats.Kind.CLEANUP, jobClient.getCleanupTaskReports( runningJob.getID() ), false );
      addTaskStats( newStats, HadoopSliceStats.Kind.MAPPER, jobClient.getMapTaskReports( runningJob.getID() ), false );
      addTaskStats( newStats, HadoopSliceStats.Kind.REDUCER, jobClient.getReduceTaskReports( runningJob.getID() ), false );

      int count = 0;

      while( captureAttempts )
        {
        TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( count );

        if( events.length == 0 )
          break;

        addAttemptsToTaskStats( newStats, events );
        count += events.length;
        }

      setTaskStats( newStats );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to get task stats", exception );
      }
    }

  private void addTaskStats( Map<String, HadoopSliceStats> taskStats, HadoopSliceStats.Kind kind, TaskReport[] taskReports, boolean skipLast )
    {
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

  private boolean stepHasReducers()
    {
    return !getFlowStep().getGroups().isEmpty();
    }

  private void incrementKind( HadoopSliceStats.Kind kind )
    {
    switch( kind )
      {
      case SETUP:
        break;
      case MAPPER:
        numMapTasks++;
        break;
      case REDUCER:
        numReduceTasks++;
        break;
      case CLEANUP:
        break;
      }
    }

  private void addAttemptsToTaskStats( Map<String, HadoopSliceStats> taskStats, TaskCompletionEvent[] events )
    {
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
    }

  private String getIDFor( TaskID taskID )
    {
    // using taskID instance as #toString is quite painful
    String id = idCache.get( taskID );

    if( id == null )
      {
      id = Util.createID( taskID.toString() );
      idCache.put( taskID, id );
      }

    return id;
    }
  }
