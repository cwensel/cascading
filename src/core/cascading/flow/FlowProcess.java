/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
import java.util.Map;

import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * FlowProcess implementations provide a call-back interface into the current computing system. Each
 * {@link cascading.operation.Operation} is given a reference to a particular implementation, allowing it
 * to get configuration properties, send a "keep alive" ping, or to set a counter value.
 * <p/>
 * Depending on the underlying system, FlowProcess instances are not continuous across all operations in a {@link cascading.flow.Flow}.
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

  private String id;

  public static class NullFlowProcess extends FlowProcess<Object>
    {
    public NullFlowProcess()
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

    public void keepAlive()
      {
      }

    public void increment( Enum counter, long amount )
      {
      }

    public void increment( String group, String counter, long amount )
      {
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
    public int getNumConcurrentTasks()
      {
      return 0;
      }

    @Override
    public int getCurrentTaskNum()
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
    public Object getConfigCopy()
      {
      return null;
      }

    @Override
    public Object copyConfig( Object jobConf )
      {
      return jobConf;
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

  /** Field currentSession */
  private FlowSession currentSession = FlowSession.NULL;

  protected FlowProcess()
    {
    }

  protected FlowProcess( FlowSession currentSession )
    {
    setCurrentSession( currentSession );
    }

  public abstract FlowProcess copyWith( Config config );

  public String getID()
    {
    return id;
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

  public abstract int getNumConcurrentTasks();

  public abstract int getCurrentTaskNum();

  /**
   * Method getProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current JobConf will be queried.
   *
   * @param key of type String
   * @return an Object
   */
  public abstract Object getProperty( String key );

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

  public abstract TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException;

  public abstract TupleEntryCollector openSystemIntermediateForWrite() throws IOException;

  public abstract Config getConfigCopy();

  public abstract Config copyConfig( Config jobConf );

  public abstract Map<String, String> diffConfigIntoMap( Config defaultConfig, Config updatedConfig );

  public abstract Config mergeMapIntoConfig( Config defaultConfig, Map<String, String> map );
  }