/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.stream.duct.DuctException;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * FlowProcess implementations provide a call-back interface into the current computing system. Each
 * {@link cascading.operation.Operation} is given a reference to a particular implementation, allowing it
 * to get configuration properties, send a "keep alive" ping, or to set a counter value.
 * <p/>
 * Depending on the underlying system, FlowProcess instances are not continuous across all operations in a {@link Flow}.
 * Thus, a call to {@link #increment(Enum, long)} may start incrementing from zero if the operation making the call
 * belongs to a subsequent 'job' or 'step' from any previous operations calling increment.
 * <p/>
 * A FlowProcess is roughly a child of {@link FlowSession}. FlowSession is roughly one to one with a particular {@link Flow}.
 * And every FlowSession will have one or more FlowProcesses.
 *
 * @see FlowSession
 */
public abstract class FlowProcess<Config>
  {
  /** Field NULL is a noop implementation of FlowSession. */
  public static FlowProcess NULL = new NullFlowProcess();

  public static class NullFlowProcess extends FlowProcess<Object>
    {
    protected NullFlowProcess()
      {
      }

    @Override
    public FlowProcess copyWith( Object object )
      {
      return new NullFlowProcess();
      }

    public Object getProperty( String key )
      {
      return null;
      }

    @Override
    public Collection<String> getPropertyKeys()
      {
      return Collections.EMPTY_SET;
      }

    @Override
    public Object newInstance( String className )
      {
      return null;
      }

    public void keepAlive()
      {
      }

    public void increment( Enum counter, long amount )
      {
      }

    public void increment( String group, String counter, long amount )
      {
      }

    @Override
    public long getCounterValue( Enum counter )
      {
      return 0;
      }

    @Override
    public long getCounterValue( String group, String counter )
      {
      return 0;
      }

    public void setStatus( String status )
      {
      }

    @Override
    public boolean isCounterStatusInitialized()
      {
      return true;
      }

    @Override
    public int getNumProcessSlices()
      {
      return 1;
      }

    @Override
    public int getCurrentSliceNum()
      {
      return 0;
      }

    public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
      {
      return tap.openForRead( this );
      }

    public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
      {
      return tap.openForWrite( this );
      }

    @Override
    public TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException
      {
      return trap.openForWrite( this );
      }

    @Override
    public TupleEntryCollector openSystemIntermediateForWrite() throws IOException
      {
      return null;
      }

    @Override
    public Object getConfig()
      {
      return null;
      }

    @Override
    public Object getConfigCopy()
      {
      return null;
      }

    @Override
    public Object copyConfig( Object config )
      {
      return config;
      }

    @Override
    public Map<String, String> diffConfigIntoMap( Object defaultConfig, Object updatedConfig )
      {
      return null;
      }

    @Override
    public Object mergeMapIntoConfig( Object defaultConfig, Map<String, String> map )
      {
      return null;
      }
    }

  private FlowSession currentSession = FlowSession.NULL;
  private Map<Tap, TupleEntryCollector> trapCollectors;

  protected FlowProcess()
    {
    }

  protected FlowProcess( FlowSession currentSession )
    {
    setCurrentSession( currentSession );
    }

  /**
   * Copy constructor.
   * <p/>
   * Shares the underlying trap collector collection across copies to avoid a static collection.
   *
   * @param flowProcess
   */
  protected FlowProcess( FlowProcess<Config> flowProcess )
    {
    setCurrentSession( flowProcess.getCurrentSession() );

    // lazy initialize trap collectors collection and share across copies
    this.trapCollectors = flowProcess.getTrapCollectors();
    }

  public abstract FlowProcess<Config> copyWith( Config config );

  /**
   * Method getID() returns the current
   *
   * @return of type String
   */
  public String getID()
    {
    return getStringProperty( FlowStep.CASCADING_FLOW_STEP_ID );
    }

  /**
   * Method getCurrentSession returns the currentSession of this FlowProcess object.
   *
   * @return the currentSession (type FlowSession) of this FlowProcess object.
   */
  public FlowSession getCurrentSession()
    {
    return currentSession;
    }

  /**
   * Method setCurrentSession sets the currentSession of this FlowProcess object.
   *
   * @param currentSession the currentSession of this FlowProcess object.
   */
  public void setCurrentSession( FlowSession currentSession )
    {
    this.currentSession = currentSession;

    currentSession.setCurrentProcess( this );
    }

  /**
   * Method getNumProcessSlices returns the number of parallel slices or tasks allocated
   * for this process execution.
   * <p/>
   * For MapReduce platforms, this is the same as the number of tasks for a given MapReduce job.
   *
   * @return an int
   */
  public abstract int getNumProcessSlices();

  /**
   * Method getCurrentSliceNum returns an integer representing which slice instance currently running.
   * <p/>
   * {@code 0} (zero) is the first slice instance.
   *
   * @return an int
   */
  public abstract int getCurrentSliceNum();

  /**
   * Method getProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current Configuration will be queried.
   *
   * @param key of type String
   * @return an Object
   */
  public abstract Object getProperty( String key );

  /**
   * Method getStringProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current Configuration will be queried.
   *
   * @param key of type String,
   * @return null if property is not set
   */
  public String getStringProperty( String key )
    {
    Object value = getProperty( key );

    if( value == null )
      return null;

    return value.toString();
    }

  /**
   * Method getStringProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current Configuration will be queried.
   *
   * @param key          of type String,
   * @param defaultValue of type String,
   * @return {@code defaultValue} if property is not set
   */
  public String getStringProperty( String key, String defaultValue )
    {
    Object value = getProperty( key );

    if( value == null )
      return defaultValue;

    return value.toString();
    }

  /**
   * Method getIntegerProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current Configuration will be queried.
   *
   * @param key of type String,
   * @return null if property is not set
   */
  public Integer getIntegerProperty( String key )
    {
    String value = getStringProperty( key );

    if( value == null || value.isEmpty() )
      return null;

    return Integer.valueOf( value );
    }

  /**
   * Method getIntegerProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current Configuration will be queried.
   *
   * @param key          of type String,
   * @param defaultValue of type int,
   * @return {@code defaultValue} if property is not set
   */
  public int getIntegerProperty( String key, int defaultValue )
    {
    String value = getStringProperty( key );

    if( value == null || value.isEmpty() )
      return defaultValue;

    return Integer.valueOf( value );
    }

  /**
   * Method getBooleanProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current Configuration will be queried.
   *
   * @param key of type Boolean, null if property is not set
   * @return an Object
   */
  public Boolean getBooleanProperty( String key )
    {
    String value = getStringProperty( key );

    if( value == null || value.isEmpty() )
      return null;

    return Boolean.valueOf( value );
    }

  /**
   * Method getBooleanProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current Configuration will be queried.
   *
   * @param key          of type String
   * @param defaultValue of type boolean
   * @return {@code defaultValue} if property is not set
   */
  public boolean getBooleanProperty( String key, boolean defaultValue )
    {
    String value = getStringProperty( key );

    if( value == null || value.isEmpty() )
      return defaultValue;

    return Boolean.valueOf( value );
    }

  /**
   * Method getPropertyKeys returns an immutable collection of all available property key values.
   *
   * @return a Collection<String>
   */
  public abstract Collection<String> getPropertyKeys();

  /**
   * Method newInstance creates a new object instance from the given className argument delegating to any
   * platform specific instantiation and configuration routines.
   *
   * @param className
   * @return an instance of className
   */
  public abstract Object newInstance( String className );

  /**
   * Method keepAlive notifies the system that the current process is still alive. Use this method if a particular
   * {@link cascading.operation.Operation} takes some moments to complete. Each system is different, so calling
   * ping every few seconds to every minute or so would be best.
   * <p/>
   * This method will fail silently if the underlying mechanism to notify keepAlive status are not initialized.
   */
  public abstract void keepAlive();

  /**
   * Method increment is used to increment a custom counter. Counters must be of type Enum. The amount
   * to increment must be a integer value.
   * <p/>
   * This method will fail if the underlying counter infrastructure is unavailable. See {@link #isCounterStatusInitialized()}.
   *
   * @param counter of type Enum
   * @param amount  of type int
   */
  public abstract void increment( Enum counter, long amount );

  /**
   * Method increment is used to increment a custom counter. The amount to increment must be a integer value.
   * <p/>
   * This method will fail if the underlying counter infrastructure is unavailable. See {@link #isCounterStatusInitialized()}.
   *
   * @param group   of type String
   * @param counter of type String
   * @param amount  of type int
   */
  public abstract void increment( String group, String counter, long amount );

  /**
   * Method getCounterValue is used to retrieve a counter value.
   * <p/>
   * This method will fail if the underlying counter infrastructure is unavailable. See {@link #isCounterStatusInitialized()}.
   *
   * @param counter of type Enum
   */
  public abstract long getCounterValue( Enum counter );

  /**
   * Method getCounterValue is used to retrieve a counter value.
   * <p/>
   * This method will fail if the underlying counter infrastructure is unavailable. See {@link #isCounterStatusInitialized()}.
   *
   * @param group   of type String
   * @param counter of type String
   */
  public abstract long getCounterValue( String group, String counter );

  /**
   * Method setStatus is used to set the status of the current operation.
   * <p/>
   * This method will fail if the underlying counter infrastructure is unavailable. See {@link #isCounterStatusInitialized()}.
   *
   * @param status of type String
   */
  public abstract void setStatus( String status );

  /**
   * Method isCounterStatusInitialized returns true if it is safe to increment a counter or set a status.
   *
   * @return boolean
   */
  public abstract boolean isCounterStatusInitialized();

  /**
   * Method openTapForRead return a {@link cascading.tuple.TupleEntryIterator} for the given Tap instance.
   * <p/>
   * Note the returned iterator will return the same instance of {@link cascading.tuple.TupleEntry} on every call,
   * thus a copy must be made of either the TupleEntry or the underlying {@code Tuple} instance if they are to be
   * stored in a Collection.
   *
   * @param tap of type Tap
   * @return TupleIterator
   * @throws java.io.IOException when there is a failure opening the resource
   */
  public abstract TupleEntryIterator openTapForRead( Tap tap ) throws IOException;

  /**
   * Method openTapForWrite returns a (@link TupleCollector} for the given Tap instance.
   *
   * @param tap of type Tap
   * @return TupleCollector
   * @throws java.io.IOException when there is a failure opening the resource
   */
  public abstract TupleEntryCollector openTapForWrite( Tap tap ) throws IOException;

  /**
   * Method openTrapForWrite returns a (@link TupleCollector} for the given Tap instance.
   *
   * @param trap of type Tap
   * @return TupleCollector
   * @throws java.io.IOException when there is a failure opening the resource
   */
  public abstract TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException;

  public abstract TupleEntryCollector openSystemIntermediateForWrite() throws IOException;

  /**
   * Method getConfig returns the actual instance of the underlying configuration instance.
   * <p/>
   * This instance should not be modified or cached, see {@link #getConfigCopy()} for a modifiable instance.
   *
   * @return an instance of the configuration
   */
  public abstract Config getConfig();

  public abstract Config getConfigCopy();

  public abstract <C> C copyConfig( C config );

  public abstract <C> Map<String, String> diffConfigIntoMap( C defaultConfig, C updatedConfig );

  public abstract Config mergeMapIntoConfig( Config defaultConfig, Map<String, String> map );

  /**
   * Method getTrapCollectorFor will return a new {@link TupleEntryCollector} if one hasn't previously
   * been created for the given trap Tap.
   *
   * @param trap
   * @return TupleEntryCollector
   */
  public TupleEntryCollector getTrapCollectorFor( Tap trap )
    {
    Map<Tap, TupleEntryCollector> trapCollectors = getTrapCollectors();

    TupleEntryCollector trapCollector = trapCollectors.get( trap );

    if( trapCollector == null )
      {
      try
        {
        trapCollector = openTrapForWrite( trap );
        trapCollectors.put( trap, trapCollector );
        }
      catch( IOException exception )
        {
        throw new DuctException( exception );
        }
      }

    return trapCollector;
    }

  protected synchronized Map<Tap, TupleEntryCollector> getTrapCollectors()
    {
    if( trapCollectors == null )
      trapCollectors = Collections.synchronizedMap( new HashMap<Tap, TupleEntryCollector>() );

    return trapCollectors;
    }

  public synchronized void closeTrapCollectors()
    {
    if( trapCollectors == null )
      return;

    for( TupleEntryCollector trapCollector : trapCollectors.values() )
      {
      try
        {
        trapCollector.close();
        }
      catch( Exception exception )
        {
        // do nothing
        }
      }

    trapCollectors.clear();
    }
  }