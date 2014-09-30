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

/**
 *
 */
public abstract class CounterCache<JobStatus, Counters>
  {
  public static final String COUNTER_TIMEOUT_PROPERTY = "cascading.step.counter.timeout";
  public static final int TIMEOUT_MAX = 3;

  private static final Logger LOG = LoggerFactory.getLogger( CounterCache.class );

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

  private CascadingStats stats;
  private Counters cachedCounters = null;
  private int numTimeouts;
  private int timeout;

  protected CounterCache( CascadingStats stats, Configuration configuration )
    {
    this.stats = stats;
    this.timeout = configuration.getInt( COUNTER_TIMEOUT_PROPERTY, 5 );
    }

  protected abstract JobStatus getJobStatusClient();

  protected abstract Collection<String> getGroupNames( Counters counters );

  protected abstract Set<String> getCountersFor( Counters counters, String group );

  protected abstract long getCounterValue( Counters counters, Enum counter );

  protected abstract long getCounterValue( Counters counters, String group, String counter );

  protected abstract Counters getCounters( JobStatus runningJob ) throws IOException;

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
    if( !force && ( stats.isFinished() || numTimeouts >= TIMEOUT_MAX ) )
      return cachedCounters;

    JobStatus runningJob = getJobStatusClient();

    if( runningJob == null )
      return cachedCounters;

    Future<Counters> future = runFuture( runningJob );

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
      numTimeouts++;

      if( numTimeouts >= TIMEOUT_MAX )
        LOG.warn( "fetching counters timed out after: {} seconds, final attempt: {}", timeout, numTimeouts );
      else
        LOG.warn( "fetching counters timed out after: {} seconds, attempts: {}", timeout, numTimeouts );
      }

    return cachedCounters;
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
        throw new FlowException( "unable to get remote counter values" );
        }
      }
    };

    return futuresPool.submit( task );
    }
  }
