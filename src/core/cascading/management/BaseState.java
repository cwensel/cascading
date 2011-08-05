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
public abstract class BaseState
  {
  private ClientType clientType;
  private String id;

  MetricsService metricsService = new NullMetricsService();
  DocumentService objectService = new NullDocumentService();

  protected BaseState()
    {
    }

  public BaseState( CascadingServices cascadingServices, ClientType clientType, String id )
    {
    this.clientType = clientType;
    this.id = id;

    if( cascadingServices == null )
      return;

    metricsService = cascadingServices.getMetricsService();
    objectService = cascadingServices.getObjectService();
    }

  public void startService()
    {
    metricsService.startService();
    objectService.startService();
    }

  public void stopService()
    {
    metricsService.stopService();
    objectService.stopService();
    }

  public ClientType getClientType()
    {
    return clientType;
    }

  String[] getContext( Enum context )
    {
    return getContext( getGroup( context ), context.toString() );
    }

  String getGroup( Enum metric )
    {
    return metric.getClass().getSimpleName();
    }

  abstract String[] getContext( String group, String metric );

  public String getID()
    {
    return id;
    }

  protected void store( Enum metric, Object value )
    {
    objectService.put( getID(), value );
    }

  protected void store( String group, String metric, Object value )
    {
    objectService.put( getID(), value );
    }

  protected void setMetric( Enum metric, long value )
    {
    metricsService.set( getContext( metric ), value );
    }

  protected void setMetric( String group, String metric, long value )
    {
    metricsService.set( getContext( group, metric ), value );
    }

  protected void setMetric( String group, String metric, String value )
    {
    metricsService.set( getContext( group, metric ), value );
    }

  private void incrementMetric( Enum metric, int value )
    {
    metricsService.increment( getContext( metric ), value );
    }

  private void incrementMetric( String group, String metric, int value )
    {
    metricsService.increment( getContext( group, metric ), value );
    }

  String[] asArray( String... strings )
    {
    return strings;
    }
  }
