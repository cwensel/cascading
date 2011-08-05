/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.management;

import cascading.flow.Flow;
import cascading.stats.CascadingStats;

/**
 *
 */
public abstract class ClientState extends BaseState
  {
  public static final String STATE_SERVICE_CLASS_PROPERTY = "cascading.management.state.service.classname";

  public static ClientState NULL = new ClientState()
  {
  @Override
  public void start( long time )
    {
    }

  @Override
  public void stop( long time )
    {
    }

  @Override
  String[] getContext( String group, String metric )
    {
    return new String[ 0 ];
    }

  @Override
  public void recordStats( CascadingStats stats )
    {
    }

  @Override
  public void recordFlow( Flow flow )
    {
    }
  };

  public ClientState()
    {
    }

  @Override
  String[] getContext( String group, String metric )
    {
    return asArray( group, metric );
    }

  public void setStatus( Enum status, long time )
    {
    setMetric( status, time );
    }

  public abstract void recordStats( CascadingStats stats );

  public abstract void recordFlow( Flow flow );

  public void record( String id, Object object )
    {
    store( id, object );
    }

  public void submit( long time )
    {
    setMetric( "state", "submit", time );
    }

  public void start( long time )
    {
    setMetric( "state", "start", time );
    }

  public void stop( long time )
    {
    setMetric( "state", "stop", time );
    }
  }
