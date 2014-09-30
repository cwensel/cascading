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

package cascading.stats.tez;

import java.util.HashMap;
import java.util.Map;

import cascading.stats.CascadingStats;
import cascading.stats.FlowSliceStats;

public class TezSliceStats implements FlowSliceStats
  {
  private final CascadingStats.Status parentStatus;

  public static class TezAttempt
    {
    }

  private String id;
  private Map<String, Map<String, Long>> counters;

  private Map<Integer, TezAttempt> attempts = new HashMap<Integer, TezAttempt>();

  TezSliceStats( String id, CascadingStats.Status parentStatus )
    {
    this.parentStatus = parentStatus;
    this.id = id;
    }

  public String getID()
    {
    return id;
    }

  public CascadingStats.Status getParentStatus()
    {
    return parentStatus;
    }

  public String[] getDiagnostics()
    {
    return null;
    }

  public Map<String, Map<String, Long>> getCounters()
    {
    if( counters == null )
      setCounters( null );

    return counters;
    }

  public Map<Integer, TezAttempt> getAttempts()
    {
    return attempts;
    }

  private void setCounters( Object taskReport )
    {
    }

  /**
   * Method getCounterValue returns the raw Hadoop counter value.
   *
   * @param counter of Enum
   * @return long
   */
  public long getCounterValue( Enum counter )
    {
    return getCounterValue( counter.getDeclaringClass().getName(), counter.name() );
    }

  /**
   * Method getCounterValue returns the raw Hadoop counter value.
   *
   * @param group of String
   * @param name  of String
   * @return long
   */
  public long getCounterValue( String group, String name )
    {
    if( getCounters() == null || getCounters().get( group ) == null )
      return 0;

    Long value = getCounters().get( group ).get( name );

    if( value == null )
      return 0;

    return value;
    }

  public void addAttempt( Object event )
    {
//    attempts.put( event.getEventId(), new TezAttempt( event ) );
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "TezTaskStats" );
    sb.append( "{id='" ).append( id ).append( '\'' );
    sb.append( '}' );
    return sb.toString();
    }
  }
