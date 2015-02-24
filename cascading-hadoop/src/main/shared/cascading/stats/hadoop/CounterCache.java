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

package cascading.stats.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
import cascading.stats.CascadingStats;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.formatDurationFromMillis;
import static java.lang.System.currentTimeMillis;

/**
 *
 */
public abstract class CounterCache<JobStatus, Counters>
  {
  public static final String COUNTER_TIMEOUT_PROPERTY = "cascading.counter.timeout.seconds";
  public static final String COUNTER_FETCH_RETRIES_PROPERTY = "cascading.counter.fetch.retries";
  public static final String COUNTER_MAX_AGE_PROPERTY = "cascading.counter.age.max.seconds";
  public static final int DEFAULT_TIMEOUT_TIMEOUT_SEC = 0; // zero means making the call synchronously
  public static final int DEFAULT_FETCH_RETRIES = 3;
  public static final int DEFAULT_CACHED_AGE_MAX = 0; // rely on client interface caching

  private static final Logger LOG = LoggerFactory.getLogger( CounterCache.class );

  // hardcoded at one thread to force serialization across all requesters in the jvm
  // this likely prevents the deadlocks the futures are safeguards against
  private static ExecutorService futuresPool = Executors.newSingleThreadExecutor( new ThreadFactory()
  {
  @Override
  public Thread newThread( Runnable runnable )
    {
    Thread thread = new Thread( runnable, "stats-counter-future" );

    thread.setDaemon( true );

    return thread;
    }
  } );

  private CascadingStats stats;
  private boolean hasCapturedFinalCounters;
  private boolean hasAvailableCounters = true;
  private Counters cachedCounters = null;
  private long lastFetch = 0;
  private boolean warnedStale = false;

  protected int maxFetchAttempts;
  protected int fetchAttempts;
  protected int timeout;
  protected int maxAge;

  protected CounterCache( CascadingStats stats, Configuration configuration )
    {
    this.stats = stats;
    this.timeout = configuration.getInt( COUNTER_TIMEOUT_PROPERTY, DEFAULT_TIMEOUT_TIMEOUT_SEC );
    this.maxFetchAttempts = configuration.getInt( COUNTER_FETCH_RETRIES_PROPERTY, DEFAULT_FETCH_RETRIES );
    this.maxAge = configuration.getInt( COUNTER_MAX_AGE_PROPERTY, DEFAULT_CACHED_AGE_MAX );
    }

  protected abstract JobStatus getJobStatusClient();

  protected abstract boolean areCountersAvailable( JobStatus runningJob );

  protected abstract Counters getCounters( JobStatus runningJob ) throws IOException;

  protected abstract Collection<String> getGroupNames( Counters counters );

  protected abstract Set<String> getCountersFor( Counters counters, String group );

  protected abstract long getCounterValue( Counters counters, Enum counter );

  protected abstract long getCounterValue( Counters counters, String group, String counter );

  public Collection<String> getCounterGroups()
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return Collections.emptySet();

    return Collections.unmodifiableCollection( getGroupNames( counters ) );
    }

  public Collection<String> getCounterGroupsMatching( String regex )
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return Collections.emptySet();

    Set<String> results = new HashSet<String>();

    for( String counter : getGroupNames( counters ) )
      {
      if( counter.matches( regex ) )
        results.add( counter );
      }

    return Collections.unmodifiableCollection( results );
    }

  public Collection<String> getCountersFor( String group )
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return Collections.emptySet();

    Set<String> results = getCountersFor( counters, group );

    return Collections.unmodifiableCollection( results );
    }

  public long getCounterValue( Enum counter )
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return 0;

    return getCounterValue( counters, counter );
    }

  public long getCounterValue( String group, String counter )
    {
    Counters counters = cachedCounters();

    if( counters == null )
      return 0;

    return getCounterValue( counters, group, counter );
    }

  protected Counters cachedCounters()
    {
    return cachedCounters( false );
    }

  protected synchronized Counters cachedCounters( boolean force )
    {
    if( !hasAvailableCounters )
      return cachedCounters;

    // no point in trying again
    if( fetchAttempts >= maxFetchAttempts )
      {
      if( !hasCapturedFinalCounters && !warnedStale )
        {
        if( cachedCounters == null )
          LOG.warn( "no counters fetched, max num consecutive retries reached: {}, type: {}, status: {}", maxFetchAttempts, stats.getType(), stats.getStatus() );
        else
          LOG.warn( "stale counters being returned, max num consecutive retries reached, age: {}, type: {}, status: {}", formatDurationFromMillis( currentTimeMillis() - lastFetch ), stats.getType(), stats.getStatus() );

        warnedStale = true;
        }

      return cachedCounters;
      }

    boolean isProcessFinished = stats.isFinished();

    // ignore force, no reason to refresh completed stats
    if( isProcessFinished && hasCapturedFinalCounters )
      return cachedCounters;

    // have not capturedFinalCounters - force it
    if( !force && isProcessFinished )
      force = true;

    int currentAge = (int) ( ( lastFetch - currentTimeMillis() ) / 1000 );

    boolean isStale = currentAge >= maxAge;

    // if we have counters, aren't being forced to update, and values aren't considered stale, return them
    if( cachedCounters != null && !force && !isStale )
      return cachedCounters;

    JobStatus runningJob = getJobStatusClient();

    if( runningJob == null )
      return cachedCounters;

    if( !areCountersAvailable( runningJob ) )
      {
      hasAvailableCounters = false;
      return cachedCounters;
      }

    boolean success = false;

    try
      {
      Counters fetched = fetchCounters( runningJob );

      success = fetched != null;

      if( success )
        {
        cachedCounters = fetched;
        lastFetch = currentTimeMillis();
        fetchAttempts = 0; // reset attempt counter, mitigates for transient non-consecutive failures
        }
      }
    catch( InterruptedException exception )
      {
      LOG.warn( "fetching counters was interrupted" );
      }
    catch( ExecutionException exception )
      {
      fetchAttempts++;

      if( fetchAttempts >= maxFetchAttempts )
        LOG.error( "fetching counters failed, was final consecutive attempt: {}, type: {}, status: {}", fetchAttempts, stats.getType(), stats.getStatus(), exception.getCause() );
      else
        LOG.warn( "fetching counters failed, consecutive attempts: {}, type: {}, status: {}, message: {}", fetchAttempts, stats.getType(), stats.getStatus(), exception.getCause().getMessage() );

      if( cachedCounters != null )
        {
        LOG.error( "returning cached values" );

        return cachedCounters;
        }

      LOG.error( "unable to get remote counters, no cached values, rethrowing exception", exception.getCause() );

      if( exception.getCause() instanceof FlowException )
        throw (FlowException) exception.getCause();

      throw new FlowException( exception.getCause() );
      }
    catch( TimeoutException exception )
      {
      fetchAttempts++;

      if( fetchAttempts >= maxFetchAttempts )
        LOG.warn( "fetching counters timed out after: {} seconds, was final consecutive attempt: {}, type: {}, status: {}", timeout, fetchAttempts, stats.getType(), stats.getStatus() );
      else
        LOG.warn( "fetching counters timed out after: {} seconds, consecutive attempts: {}, type: {}, status: {}", timeout, fetchAttempts, stats.getType(), stats.getStatus() );
      }

    hasCapturedFinalCounters = isProcessFinished && success;

    return cachedCounters;
    }

  private Counters fetchCounters( JobStatus runningJob ) throws InterruptedException, ExecutionException, TimeoutException
    {
    // if timeout greater than zero, perform async call
    if( timeout > 0 )
      return runFuture( runningJob ).get( timeout, TimeUnit.SECONDS );

    try
      {
      return getCounters( runningJob );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get remote counter values", exception );
      }
    }

  private Future<Counters> runFuture( final JobStatus jobStatus )
    {
    Callable<Counters> task = new Callable<Counters>()
    {
    @Override
    public Counters call() throws Exception
      {
      try
        {
        return getCounters( jobStatus );
        }
      catch( IOException exception )
        {
        throw new FlowException( "unable to get remote counter values", exception );
        }
      }
    };

    return futuresPool.submit( task );
    }
  }
