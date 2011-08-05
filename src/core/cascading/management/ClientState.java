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

/**
 *
 */
public class ClientState extends BaseState
  {
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
  };

  protected ClientState()
    {
    }

  public ClientState( CascadingServices cascadingServices, ClientType clientType, String id )
    {
    super( cascadingServices, clientType, id );
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

  public void record( Object object )
    {
    store( getClientType().toString(), object.getClass().getSimpleName(), object );
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
