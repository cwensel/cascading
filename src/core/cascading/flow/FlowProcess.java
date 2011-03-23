/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.IOException;

import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * FlowProcess implementations provide a call-back interface into the current computing system. Each
 * {@link cascading.operation.Operation} is given a reference to a particluar implemenation, allowing it
 * to get configuration properties, send a "keep alive" ping, or to set a counter value.
 * <p/>
 * Depending on the underlying system, FlowProcess instances are not continuous across all operations in a {@link cascading.flow.Flow}.
 * Thus, a call to {@link #increment(Enum, int)} may start incrementing from zero if the operation making the call
 * belongs to a subsquent 'job' or 'step' from any previous operations calling increment.
 * <p/>
 * A FlowProcess is roughly a child of {@link FlowSession}. FlowSession is roughly one to one with a particular {@link Flow}.
 * And every FlowSession will have one or more FlowProcesses.
 *
 * @see FlowSession
 */
public abstract class FlowProcess
  {
  /** Field NULL is a noop implementation of FlowSession. */
  public static FlowProcess NULL = new FlowProcess( FlowSession.NULL )
  {
  public Object getProperty( String key )
    {
    return null;
    }

  public void keepAlive()
    {
    }

  public void increment( Enum counter, int amount )
    {
    }

  public void increment( String group, String counter, int amount )
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

  public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
    {
    return null;
    }

  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return null;
    }
  };

  /** Field currentSession */
  private FlowSession currentSession;

  protected FlowProcess()
    {
    }

  protected FlowProcess( FlowSession currentSession )
    {
    setCurrentSession( currentSession );
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
  public abstract void increment( Enum counter, int amount );

  /**
   * Method increment is used to increment a custom counter. The amount to increment must be a integer value.
   * <p/>
   * This method will fail if the underlying counter infrastructure is unavailable. See {@link #isCounterStatusInitialized()}.
   *
   * @param group   of type String
   * @param counter of type String
   * @param amount  of type int
   */
  public abstract void increment( String group, String counter, int amount );

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
   * Method openTapForRead return a {@link cascading.tuple.TupleIterator} for the given Tap instance.
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
  }