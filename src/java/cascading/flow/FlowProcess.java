/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

/**
 * FlowSession implementations provide a call-back interface into the current computing system. Each
 * {@link cascading.operation.Operation} is given a reference to a particluar implemenation, allowing them
 * to get configuration properties, send a "keep alive" ping, or to set a counter value.
 * <p/>
 * Depending on the underlying system, FlowSession instances are not continuous across all operations in a {@link cascading.flow.Flow}.
 * Thus, a call to {@link #increment(Enum, int)} may start incrementing from zero if the operation making the call
 * belongs to a subsquent 'job' or 'step' from any previous operations calling increment.
 */
public interface FlowProcess
  {
  /** Field NULL is a noop implemenation of FlowSession. */
  FlowProcess NULL = new FlowProcess()
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
  };

  /**
   * Method getProperty should be used to return configuration parameters from the underlying system.
   * <p/>
   * In the case of Hadoop, the current JobConf will be queried.
   *
   * @param key of type String
   * @return an Object
   */
  Object getProperty( String key );

  /**
   * Method keepAlive notifies the system that the current process is still alive. Use this method if a particular
   * {@link cascading.operation.Operation} takes some moments to complete. Each system is different, so calling
   * ping every few seconds to every minute or so would be best.
   */
  void keepAlive();

  /**
   * Method increement is used to increment a custom counter. Counters must be of type Enum. The amount
   * to increment must be a positive integer value.
   *
   * @param counter of type Enum
   * @param amount  of type int
   */
  void increment( Enum counter, int amount );

  }