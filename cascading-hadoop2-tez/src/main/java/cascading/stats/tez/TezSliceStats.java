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

package cascading.stats.tez;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import cascading.stats.CascadingStats;
import cascading.stats.FlowSliceStats;
import cascading.stats.ProvidesCounters;

public class TezSliceStats extends FlowSliceStats<TezNodeStats.Kind> implements ProvidesCounters
  {
  public static class TezAttempt extends FlowSliceAttempt
    {
    @Override
    public String getProcessAttemptID()
      {
      return null;
      }

    @Override
    public int getEventId()
      {
      return 0;
      }

    @Override
    public int getProcessDuration()
      {
      return 0;
      }

    @Override
    public String getProcessStatus()
      {
      return null;
      }

    @Override
    public String getStatusURL()
      {
      return null;
      }

    @Override
    public CascadingStats.Status getStatus()
      {
      return null;
      }
    }

  private String id;
  private TezNodeStats.Kind kind;
  private final CascadingStats.Status parentStatus;
  private CascadingStats.Status status;
  private String taskID;
  private Map<String, Map<String, Long>> counters = Collections.emptyMap();

  private Map<Integer, FlowSliceAttempt> attempts = new HashMap<>();

  TezSliceStats( String id, TezNodeStats.Kind kind, CascadingStats.Status parentStatus, String taskID )
    {
    this.id = id;
    this.kind = kind;
    this.parentStatus = parentStatus;
    this.taskID = taskID;
    }

  @Override
  public String getID()
    {
    return id;
    }

  @Override
  public long getProcessStartTime()
    {
    return 0;
    }

  @Override
  public long getProcessFinishTime()
    {
    return 0;
    }

  public CascadingStats.Status getParentStatus()
    {
    return parentStatus;
    }

  protected void setStatus( @Nullable CascadingStats.Status status )
    {
    if( status != null )
      this.status = status;
    }

  @Override
  public CascadingStats.Status getStatus()
    {
    return status;
    }

  @Override
  public TezNodeStats.Kind getKind()
    {
    return kind;
    }

  public String[] getDiagnostics()
    {
    return new String[ 0 ];
    }

  @Override
  public Map<String, Map<String, Long>> getCounters()
    {
    if( counters == null )
      setCounters( null );

    return counters;
    }

  public String getProcessSliceID()
    {
    return taskID;
    }

  @Override
  public String getProcessStepID()
    {
    return null;
    }

  @Override
  public String getProcessStatus()
    {
    return null;
    }

  @Override
  public float getProcessProgress()
    {
    return 0;
    }

  public Map<Integer, FlowSliceAttempt> getAttempts()
    {
    return attempts;
    }

  public void setCounters( @Nullable Map<String, Map<String, Long>> counters )
    {
    if( counters != null )
      this.counters = counters;
    }

  @Override
  public Collection<String> getCounterGroups()
    {
    return getCounters().keySet();
    }

  @Override
  public Collection<String> getCountersFor( String group )
    {
    return getCounters().get( group ).keySet();
    }

  @Override
  public Collection<String> getCountersFor( Class<? extends Enum> group )
    {
    return getCountersFor( group.getDeclaringClass().getName() );
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    return getCounterValue( counter.getDeclaringClass().getName(), counter.name() );
    }

  @Override
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
    sb.append( "TezSliceStats" );
    sb.append( "{id='" ).append( id ).append( '\'' );
    sb.append( '}' );
    return sb.toString();
    }
  }
